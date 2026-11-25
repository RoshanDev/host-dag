"""Linux workstation lock matrix: bounded blocking flock, phase leases and observation.

Only the main thread acquires leases (SIGALRM bounds the blocking syscall). There is
no LOCK_NB/retry loop: the process stays in the kernel flock queue until admitted.
"""
from __future__ import annotations

from hostdag_json import atomic_json as shared_atomic_json
from hostdag_json import public_path

import argparse
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
import fcntl
import hashlib
import json
import math
import os
import re
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from hostdag_json import read_json as read_json_retry
from hostdag_build_lanes import LANES, LaneLease, descriptors, phase_lane
from hostdag_ticket_state import CORE_CHAINS, all_tickets

LOCK_ORDER = ("local-heavy-admission", "workstation-build")
# Preserve the workstation pair for existing callers that unpack it. Remote
# stages take their own writer lock, after any phase-scoped workstation build.
RESOURCE_LOCK_ORDER = ("local-heavy-admission", "local-devcluster-write",
                       "java-build", "fe-build", "go-test", "image-pack", "workstation-build")
RW_ALIASES: dict[str, str] = {}


def canonical_lock(name):
    name = name.removesuffix(':shared')
    name = re.sub(r'\.slot-\d+$|\.capacity$', '', name)
    return next((new for new, old in RW_ALIASES.items() if name == old), name)


def related_lock_names(name):
    canonical = canonical_lock(name)
    return list(dict.fromkeys([canonical, RW_ALIASES.get(canonical, canonical)]))


HELD = ContextVar("resource_locks", default={})
ADVISORS: list[subprocess.Popen] = []


# First matching rule wins; test/build tokens cannot be hidden by a heavy label.
PHASE_RULES = (
    (('targeted',), 'none'),
    (('go-test', 'vet', 'jest', 'tsc', 'build', 'npm'), 'build'),
    (('package', 'deploy', 'cluster', 'reimage', 'install'), 'heavy'),
)


def validate_phase(phase, names, *, ticket=''):
    if 'workstation-build' in names and 'targeted' not in phase.lower():
        # Admission and lint use the same rule. Unknown phases must not silently
        # acquire a build lease, nor may a build token disguise verification.
        tokens = set(re.findall(r'[a-z0-9]+', phase.lower()))
        if (not tokens.intersection({'build', 'npm', 'jest', 'tsc', 'vet', 'test', 'pytest', 'unittest'})
                and not {'full', 'suite'} <= tokens):
            raise ValueError('non-build phase cannot hold workstation-build')
        if tokens.intersection({'verify', 'verification', 'screenshot', 'shot', 'browser',
                                'deploy', 'install', 'wait', 'waiting', 'package', 'promotion', 'gate'}):
            raise ValueError('non-build phase cannot hold workstation-build')
    lanes = [canonical_lock(n) for n in names if canonical_lock(n) in LANES]
    if len(lanes) > 1 or (lanes and 'workstation-build' in names):
        raise ValueError('one build lane per phase; release it before the next phase')
    expected_lane = phase_lane(phase)
    if lanes and expected_lane and lanes[0] != expected_lane:
        raise ValueError('phase requires %s, not %s' % (expected_lane, lanes[0]))
    remote = re.search(r'\bremote\b', phase + ' ' + ticket, re.I)
    if remote and 'local-heavy-admission' in names and any(
            word in phase.lower() for word in ('install', 'deploy', 'reimage', 'accept', 'verify', '验收', '安装', '部署', '重装')):
        raise ValueError('remote install/acceptance requires its host writer lock, not local-heavy-admission')
    for words, kind in PHASE_RULES:
        if any(word in phase.lower() for word in words):
            if kind == 'none':
                if lanes:
                    return  # targeted fallback may use its declared resource lane
                raise ValueError('targeted tests take no locks; run directly with MemoryMax=8G')
            if kind == 'build' and 'local-heavy-admission' in names:
                if lanes == ['image-pack']:
                    return
                raise ValueError('build/test phases must use only workstation-build')
            return


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    shared_atomic_json(path, value)

def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        value = read_json_retry(path)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def pid_start(pid: int) -> str | None:
    try:
        fields = Path(f"/proc/{int(pid)}/stat").read_text().rsplit(")", 1)[1].split()
        return fields[19] if fields[0] not in ("Z", "X") else None
    except (OSError, ValueError, TypeError, IndexError):
        return None


def flock_rows(path: Path, text: str | None = None) -> list[dict]:
    """Include blocked -> FLOCK rows; match device AND inode, never lock-table IDs."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return []
    if text is None:
        text = Path('/proc/locks').read_text()
    result = []
    for line in text.splitlines():
        parts = line.split()[1:]
        blocked = bool(parts and parts[0] == '->')
        if blocked:
            parts = parts[1:]
        if len(parts) < 7 or parts[0] != 'FLOCK':
            continue
        try:
            major, minor, inode = parts[4].split(':')
            matches = (int(major, 16), int(minor, 16), int(inode)) == (
                os.major(stat.st_dev), os.minor(stat.st_dev), stat.st_ino)
            if matches:
                result.append({'pid': int(parts[3]), 'blocked': blocked, 'mode': parts[2],
                               'device': stat.st_dev, 'inode': stat.st_ino})
        except ValueError:
            continue
    return result


def process_holds_flock(pid: int, path: Path) -> bool:
    """A declared holder may legitimately own an FD inherited from the kernel's original PID."""
    stat = path.stat()
    try:
        entries = list(Path(f'/proc/{pid}/fdinfo').iterdir())
    except OSError:
        return False
    for info in entries:
        try:
            opened = Path(f'/proc/{pid}/fd/{info.name}').stat()
            if (opened.st_dev, opened.st_ino) == (stat.st_dev, stat.st_ino) and any(
                    line.startswith('lock:') and 'FLOCK' in line and ('WRITE' in line or 'READ' in line)
                    for line in info.read_text().splitlines()):
                return True
        except OSError:
            continue
    return False


def held_flock_rows(path: Path, text: str | None = None) -> list[dict] | None:
    """Find owners, including inherited FDs absent from /proc/locks; never acquire a lock.

    ``None`` means the probe could not decide: fuser is missing, timed out or
    exited abnormally. A failed probe is not an empty owner set, so callers that
    act destructively (the holder reaper) must treat it as "leave it alone".
    """
    try:
        stat = path.stat()
    except FileNotFoundError:
        return []  # no inode, so no process can hold this lock
    except OSError:
        return None
    rows = [r for r in flock_rows(path, text) if not r['blocked']]
    if rows:
        return rows
    # psmisc fuser has no ``--`` separator (it answers with usage and exit 1);
    # 0 = access found, 1 = none found, anything else = probe failure.
    try:
        result = subprocess.run(['fuser', str(path)], capture_output=True, text=True, timeout=2)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode not in (0, 1):
        return None
    try:
        return [{'pid': int(pid), 'blocked': False, 'mode': 'WRITE',
                 'device': stat.st_dev, 'inode': stat.st_ino}
                for pid in result.stdout.split() if pid.isdigit() and process_holds_flock(int(pid), path)]
    except OSError:
        return None


class LockTimeout(TimeoutError):
    pass


class Terminated(BaseException):
    def __init__(self, signum):
        self.signum = signum


def current_refs() -> dict:
    """Carry the fixed order across exec boundaries, without reacquiring parent FDs."""
    inherited = json.loads(os.environ.get('HOST_DAG_LOCK_FDS', '{}'))
    if not isinstance(inherited, dict) or not set(inherited) <= set(RESOURCE_LOCK_ORDER):
        raise ValueError('invalid inherited lock names')
    for name, primary in inherited.items():
        for ref in [primary] + primary.get('guards', []):
            try:
                fd = ref['fd']
                if type(fd) is not int or fd < 3:
                    raise ValueError('invalid descriptor')
                opened, expected = os.fstat(fd), Path(ref['path']).stat()
                if (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino):
                    raise ValueError('inherited inode mismatch')
                info = Path(f'/proc/self/fdinfo/{fd}').read_text()
                if not any(line.startswith('lock:') and 'FLOCK' in line and ('READ' if ref.get('shared') else 'WRITE') in line
                           for line in info.splitlines()):
                    raise ValueError('inherited descriptor has no exclusive flock')
                if name == 'remote-platform' and ref is primary:
                    cas_fd = ref['cas_fd']
                    expected_cas = Path(ref['path']).parent.parent / 'remote-experiment-slot.json.cas.lock'
                    cas, expected = os.fstat(cas_fd), expected_cas.stat()
                    if (cas.st_dev, cas.st_ino) != (expected.st_dev, expected.st_ino):
                        raise ValueError('inherited CAS mutex inode mismatch')
                    if not any(line.startswith('lock:') and 'FLOCK' in line and 'READ' in line
                               for line in Path(f'/proc/self/fdinfo/{cas_fd}').read_text().splitlines()):
                        raise ValueError('inherited CAS mutex is not held')
            except (KeyError, TypeError, OSError, ValueError) as exc:
                raise ValueError(f'invalid inherited {name} lease: {exc}') from exc
    return {**inherited, **HELD.get()}


@contextmanager
def termination_cleanup():
    """Turn TERM/INT/HUP into stack unwinding, including child drain and holder cleanup."""
    def terminate(signum, frame):
        raise Terminated(signum)
    saved = {sig: signal.signal(sig, terminate) for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
    try:
        yield
    finally:
        for sig, handler in saved.items():
            signal.signal(sig, handler)


def blocking_flock(fd: int, seconds: float, shared=False) -> None:
    if seconds <= 0:
        raise LockTimeout('lock queue deadline expired')
    def expired(signum, frame):
        raise LockTimeout('lock queue deadline expired')
    # Do not silently disable another caller's deadline.
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError('active ITIMER_REAL: acquire the phase lock before setting another alarm')
    old = signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)


def mem_available_gib() -> float:
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) / (1024 ** 2)


def positive(value, name, *, zero=False):
    if not math.isfinite(float(value)) or (value < 0 if zero else value <= 0):
        raise ValueError(f'{name} must be finite and {"nonnegative" if zero else "positive"}')
    return value


def ticket_is_core(run_dir, ticket):
    row = all_tickets(read_json(Path(run_dir) / 'run.json')).get(ticket) or {}
    return row.get('chain_id', row.get('chain')) in CORE_CHAINS


class ResourceLocks:
    """A phase lease. Nest heavy -> build, and drain children before leaving the scope.

    Normal children should use run_command() below, which passes all current FDs.
    SIGKILL cannot run cleanup; watcher detects the resulting stale holder.
    """
    def __init__(self, run_dir, names, *, ticket, phase, expected_min=10, max_min=None,
                 wait_seconds=7200, memory_gib=0, memory_wait=600, core=False, unit=None, shared=False,
                 jest_shard=False):
        names = list(names)
        if shared and not set(names) <= set(RW_ALIASES):
            raise ValueError('shared mode requires reader/writer lock names; legacy aliases are exclusive')
        self.shared = shared
        self.jest_shard = jest_shard
        if jest_shard and (names != ['fe-build'] or 'jest' not in phase.lower() or 'shard' not in phase.lower()):
            raise ValueError('two FE slots require one explicit jest-shard phase')
        if len({canonical_lock(n) for n in names}) != len(names):
            raise ValueError('platform alias cannot be acquired twice')
        if not names or not set(names) <= set(RESOURCE_LOCK_ORDER):
            raise ValueError('select a known resource lock')
        if names != [name for name in RESOURCE_LOCK_ORDER if name in names]:
            raise ValueError('lock arguments must follow local-heavy-admission -> workstation-build; '
                             'reversed or repeated arguments are not allowed')
        if 'local-heavy-admission' in names and max_min is None:
            raise ValueError('heavy lock requires its contract max_min')
        if not ticket or not phase:
            raise ValueError('ticket and phase are required')
        validate_phase(phase, [n + ':shared' if shared else n for n in names], ticket=ticket)
        self.names = names
        if 'image-pack' in names and len(names) > 1 and 'local-heavy-admission' not in names:
            raise ValueError('image-pack requires its implicit heavy guard before other resources')
        self.directory = Path(run_dir) / 'control/locks'
        self.paths = {name: self.directory / (RW_ALIASES.get(canonical_lock(name), name) + '.lock') for name in self.names}
        self.ticket, self.phase, self.unit = ticket, phase, unit or process_context(os.getpid())['unit']
        self.expected_min = positive(expected_min, 'expected_min')
        self.max_min = positive(max_min if max_min is not None else 30, 'max_min')
        if set(names).intersection({'workstation-build', *LANES}) and (self.expected_min > 30 or self.max_min > 30):
            raise ValueError('workstation-build lease limit is 30 minutes; split the phase')
        self.wait_seconds = positive(wait_seconds, 'wait_seconds')
        self.memory_gib = positive(memory_gib, 'memory_gib', zero=True)
        self.memory_gib = max([self.memory_gib] + [LANES[n][1] for n in names if n in LANES])
        if 'local-devcluster-write' in names:
            self.memory_gib = max(8, self.memory_gib)
        self.memory_wait = positive(memory_wait, 'memory_wait', zero=True)
        self.core = ticket_is_core(run_dir, ticket)
        self.refs = {}
        self.claim = uuid.uuid4().hex
        self.token = None
        self.signals = None
        self.cas_fd = None
        self.cas_waiter = None

    def holder_path(self, name):
        if name in LANES:
            return self.paths[name].with_suffix('.holder.json')
        canonical = canonical_lock(name)
        if canonical in RW_ALIASES:
            return self.directory / f'{canonical}.holders/{os.getpid()}.json'
        return self.directory / f'{name}.holder.json'

    def yield_to_core(self, name):
        if self.core:
            return
        paths = [self.paths[name]]
        if name in LANES:
            paths.extend(self.directory / guard for guard in lane_guard_names(name))
        for lock_path in paths:
            waiting = {r['pid'] for r in flock_rows(lock_path) if r['blocked']}
            related = set(related_lock_names(canonical_lock(lock_path.stem)))
            related.update(lane for lane in LANES if lock_path.name in lane_guard_names(lane))
            for path in (p for resource in sorted(related)
                         for p in (self.directory / f'{resource}.waiters').glob('*.json')):
                row = read_json(path)
                queued = scalar_number(row.get('queued_at'))
                start = pid_start(row.get('pid'))
                if (row.get('core') and queued is not None and time.time() - queued > 600
                        and row.get('pid') in waiting and start
                        and start == row.get('pid_start')):
                    raise LockTimeout('yield to queued core task: ' + str(row.get('ticket')))

    def __enter__(self):
        held = current_refs()
        if 'image-pack' in self.names and held and 'local-heavy-admission' not in held:
            raise ValueError('image-pack must acquire heavy before any outer resource')
        if set(held).intersection(LANES) and set(self.names).intersection(LANES):
            raise ValueError('release current lane before entering another build phase')
        if {canonical_lock(n) for n in held}.intersection(canonical_lock(n) for n in self.names):
            raise ValueError('nested lock order: no reacquisition through resource alias')
        if held and min(RESOURCE_LOCK_ORDER.index(n) for n in self.names) <= max(RESOURCE_LOCK_ORDER.index(n) for n in held):
            raise ValueError('nested lock order must be heavy -> build; no reacquisition')
        self.signals = termination_cleanup()
        self.signals.__enter__()
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            deadline = time.monotonic() + self.wait_seconds
            # Publish observable wait receipts, but never take a flock while
            # waiting for memory. A nested caller must release its outer lease.
            memory_deadline = time.monotonic() + self.memory_wait
            memory_rows = []
            try:
                while self.memory_gib and mem_available_gib() < self.memory_gib:
                    if held:
                        raise LockTimeout('release outer locks before memory-wait')
                    if not memory_rows:
                        for name in self.names:
                            path = self.directory / f'{name}.waiters/{os.getpid()}-{pid_start(os.getpid())}.json'
                            atomic_json(path, {'ticket': self.ticket, 'phase': 'memory-wait',
                                              'requested_phase': self.phase, 'pid': os.getpid(),
                                              'pid_start': pid_start(os.getpid()), 'core': self.core,
                                              'queued_at': time.time()})
                            memory_rows.append(path)
                    if time.monotonic() >= memory_deadline:
                        raise LockTimeout('MemAvailable admission deadline expired (no locks held)')
                    time.sleep(min(5, max(0, memory_deadline - time.monotonic())))
            finally:
                for path in memory_rows:
                    path.unlink(missing_ok=True)
            if 'remote-platform' in self.names:
                # Legacy CAS writers hold this exact metadata mutex while checking
                # and replacing the slot. Keep a shared mutex for the entire platform
                # lease: old writers cannot publish OWNED behind an active reader.
                # Check the durable owner only AFTER taking the mutex; old owners
                # do not keep their CAS flock for their whole experiment.
                self.cas_waiter = self.directory / f'remote-platform.waiters/{os.getpid()}-{pid_start(os.getpid())}.json'
                atomic_json(self.cas_waiter, {'ticket': self.ticket, 'phase': self.phase,
                    'pid': os.getpid(), 'pid_start': pid_start(os.getpid()), 'core': self.core,
                    'shared': self.shared, 'queued_at': time.time(), 'waiting_for': 'legacy-cas-mutex'})
                mutex = self.directory.parent / 'remote-experiment-slot.json.cas.lock'
                self.cas_fd = os.open(mutex, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
                blocking_flock(self.cas_fd, deadline - time.monotonic(), shared=True)
                slot = self.directory.parent / 'remote-experiment-slot.json'
                if slot.exists():
                    value = read_json_retry(slot)
                    if not isinstance(value, dict) or value.get('state') != 'RELEASED':
                        raise LockTimeout('remote-platform waits for existing CAS slot owner to release')
            for name in self.names:
                self.yield_to_core(name)
                row = {'lock': canonical_lock(name), 'alias': RW_ALIASES.get(canonical_lock(name)),
                       'requested_lock': name, 'ticket': self.ticket, 'phase': self.phase, 'pid': os.getpid(),
                       'pid_start': pid_start(os.getpid()), 'unit': self.unit,
                       'expected_min': self.expected_min, 'max_min': self.max_min,
                       'jest_shard': self.jest_shard,
                       'shared': self.shared, 'core': self.core, 'claim': self.claim, 'queued_at': time.time()}
                waiter = self.directory / f'{name}.waiters/{os.getpid()}-{row["pid_start"]}.json'
                atomic_json(waiter, row)
                try:
                    if name in LANES:
                        held_paths = [r['path'] for r in [*held.values(), *self.refs.values()]]
                        lease = LaneLease(self.directory, name, deadline-time.monotonic(),
                                          held_paths=held_paths, blocking=blocking_flock,
                                          jest_shard=self.jest_shard, timeout_error=LockTimeout, metadata=row).__enter__()
                        ref = lease.ref
                        self.paths[name] = Path(ref['path'])
                        row.update(slot=ref['slot'], path=ref['path'], guards=ref['guards'])
                    else:
                        fd = os.open(self.paths[name], os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o600)
                        ref = {'shared': self.shared, 'fd': fd, 'path': str(public_path(self.paths[name]))}
                        if name == 'remote-platform':
                            ref['cas_fd'] = self.cas_fd
                        self.refs[name] = ref
                        blocking_flock(fd, deadline - time.monotonic(), shared=True) if self.shared else blocking_flock(fd, deadline - time.monotonic())
                    self.refs[name] = ref
                    fd = ref['fd']
                    self.yield_to_core(name)
                    stat = os.fstat(fd)
                    row.update(acquired_at=time.time(), device=stat.st_dev, inode=stat.st_ino)
                    # Heavy/remote budgets are advisory: only build leases kill
                    # their foreground command when the registered budget expires.
                    if name == 'workstation-build' or name in LANES:
                        ref['deadline'] = time.monotonic() + min(self.max_min, 30) * 60
                    # The kernel lock serializes replacement of stale metadata.
                    holder_path = self.holder_path(name)
                    atomic_json(holder_path, row)
                    ref.update(holder_path=str(public_path(holder_path)), claim=self.claim)
                finally:
                    waiter.unlink(missing_ok=True)
            self.token = HELD.set({**held, **self.refs})
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *exc):
        try:
            for name, ref in reversed(list(self.refs.items())):
                try:
                    holder = self.holder_path(name)
                    if read_json(holder).get('claim') == self.claim:
                        holder.unlink(missing_ok=True)
                finally:
                    # Close instead of LOCK_UN so a surviving inherited FD still excludes writers.
                    for fd in reversed(descriptors({name: ref})):
                        os.close(fd)
            self.refs.clear()
            if self.cas_waiter is not None:
                self.cas_waiter.unlink(missing_ok=True)
                self.cas_waiter = None
            if self.cas_fd is not None:
                os.close(self.cas_fd)
                self.cas_fd = None
        finally:
            if self.token is not None:
                HELD.reset(self.token)
                self.token = None
            if self.signals is not None:
                self.signals.__exit__(*exc)
                self.signals = None


def run_command(argv, *, env=None, job=None, io=False, **kwargs) -> int:
    """Apply job scratch and priority only after the caller acquired its leases."""
    from hostdag_job_io import job_environment, jest_command, io_command
    refs = current_refs()
    job = job or next((r.get('ticket') for r in refs.values() if r.get('ticket')), 'worker')
    with job_environment(job, {**os.environ, **(env or {})}) as child_env:
        child_env['HOST_DAG_IO_CLASS'] = 'best-effort' if 'local-heavy-admission' in refs else 'idle'
        argv = jest_command(argv, child_env)
        # Heavy owner is best-effort, never idle; bypass bulk I/O opts into idle.
        if 'local-heavy-admission' in refs or io:
            argv = io_command(argv, heavy='local-heavy-admission' in refs)
        return _run_command(argv, env=child_env, **kwargs)


def command_holders(refs, pid):
    """Record the command separately from the process owning the flock.

    Children such as ssh/sudo may close inherited FDs. Keep pid/pid_start tied
    to the acquiring process so observer and write guard inspect a real owner.
    Inherited/nested commands restore their caller's diagnostics on exit.
    """
    previous = []
    for ref in refs.values():
        if not ref.get('holder_path') or not ref.get('claim'):
            continue  # older inherited refs do not identify a writable receipt
        path = Path(ref['holder_path'])
        row = read_json(path)
        if row.get('claim') != ref['claim']:
            continue
        previous.append((path, row))
        atomic_json(path, {**row, 'child_pid': pid, 'child_pid_start': pid_start(pid)})
    return previous


def _run_command(argv, *, env=None, **kwargs) -> int:
    """Supervise a foreground process group; every exec child inherits the lease FDs."""
    refs = current_refs()
    child = None
    previous_holders = []
    pending = []
    # Do not unwind Popen after fork but before it returns the child's handle:
    # that would strand a process with inherited lease FDs. Defer cancellation
    # only over this short launch interval, then drain in the normal finally.
    signals = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)
    saved = {sig: signal.signal(sig, lambda signum, frame: pending.append(signum)) for sig in signals}
    try:
        try:
            child = subprocess.Popen(argv, pass_fds=tuple(descriptors(refs) + [r['cas_fd'] for r in refs.values() if r.get('cas_fd') is not None]),
                                     start_new_session=True,
                                     env={**os.environ, **(env or {}), 'HOST_DAG_LOCK_FDS': json.dumps(refs)}, **kwargs)
            previous_holders = command_holders(refs, child.pid)
        finally:
            for sig, handler in saved.items():
                signal.signal(sig, handler)
        if pending:
            raise Terminated(pending[0])
        deadlines = [r['deadline'] for r in refs.values() if 'deadline' in r]
        try:
            return child.wait(timeout=max(0, min(deadlines)-time.monotonic()) if deadlines else None)
        except subprocess.TimeoutExpired as exc:
            raise LockTimeout('lock lease expired; split phases within 30 minutes') from exc
    finally:
        if child is not None:
            # A command must not leave detached workers holding the phase lock. The
            # commands under this API must not daemonize into another process group.
            with_signals = {sig: signal.signal(sig, signal.SIG_IGN) for sig in
                            (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)}
            try:
                try:
                    os.killpg(child.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    pass
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
                for path, row in previous_holders:
                    current = read_json(path)
                    if current.get('claim') == row.get('claim') and current.get('child_pid') == child.pid:
                        atomic_json(path, row)
            finally:
                for sig, handler in with_signals.items():
                    signal.signal(sig, handler)


def observed_paths(run_dir: Path, locks_dir=None) -> list[Path]:
    """Observe canonical files plus historical independent heavy locks; never rename live locks."""
    control = run_dir / 'control'
    directory = Path(locks_dir) if locks_dir else control / 'locks'
    return sorted(set([*directory.glob('*.lock'),
                       *control.glob('local-heavy-*.lock'),
                       *directory.glob('workstation-build.lock'),
                       *control.glob('workstation-build.lock'),
                       *(p.with_suffix('').with_suffix('.lock')
                         for p in directory.glob('*.holder.json')
                         if p.name.removesuffix('.holder.json') in RESOURCE_LOCK_ORDER)]))


def lane_guard_names(lane: str) -> tuple[str, ...]:
    """Compatibility inodes taken by LaneLease before publishing its slot."""
    return (('local-heavy-admission.lock',) if lane == 'image-pack' else ()) + (
        'workstation-build.lock',) + (('fe-build.capacity.lock',) if lane == 'fe-build' else ())


def acquiring_lane_waiters(path: Path) -> list[dict]:
    """A live lane receipt registers only guards that its own PID really holds."""
    matches = []
    for lane in LANES:
        if path.name not in lane_guard_names(lane):
            continue
        for receipt in sorted((path.parent / (lane + '.waiters')).glob('*.json')):
            row = read_json(receipt)
            start = pid_start(row.get('pid'))
            if (start and row.get('pid_start') == start and row.get('phase') != 'memory-wait'
                    and process_holds_flock(row['pid'], path)):
                matches.append({'waiter_path': str(receipt), 'lane': lane, **row})
    return matches


def waiter_pid_stale(pid: int, expected_start: str) -> bool:
    """Positive evidence only: an absent /proc PID or a readable different start.

    Unlike pid_start(), this destructive predicate must distinguish an absent
    process from an unreadable or malformed stat. Even ENOENT on stat alone is
    inconclusive while the process directory still exists.
    """
    try:
        number = int(pid)
        if number <= 0:
            return False
        proc = Path(f'/proc/{number}')
        try:
            fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        except FileNotFoundError:
            try:
                proc.stat()
            except FileNotFoundError:
                return True
            return False
        start = fields[19]
        return int(start) > 0 and start != expected_start
    except (OSError, ValueError, TypeError, IndexError):
        return False  # Unknown is never evidence for deletion.


def reap_waiters(directory: Path, names=None) -> list[dict]:
    """Remove confirmed dead/reused PID receipts, preserving live or changed rows.

    PID/start filenames are private to one process incarnation. Re-read both the
    receipt and /proc immediately before unlinking; never infer death from age.
    Malformed/unidentified receipts are left for diagnosis.
    """
    reaped = []
    folders = ([directory / (name + '.waiters') for name in names] if names is not None
               else sorted(directory.glob('*.waiters')))
    for folder in folders:
        for path in sorted(folder.glob('*.json')):
            row = read_json(path)
            if not row.get('pid') or not row.get('pid_start'):
                continue
            if not waiter_pid_stale(row['pid'], row['pid_start']):
                continue
            if read_json(path) != row or not waiter_pid_stale(row['pid'], row['pid_start']):
                continue
            try:
                path.unlink()
            except OSError:
                continue
            reaped.append({'path': str(path), 'waiter': row,
                           'reason': 'waiter pid absent/reused'})
    return reaped


def lane_guard_registered(path: Path) -> bool:
    """Compatibility guards have their metadata in the owning slot's receipt."""
    for holder in path.parent.glob('*.slot-*.holder.json'):
        row = read_json(holder)
        if row.get('pid_start') != pid_start(row.get('pid')):
            continue
        if any(public_path(g.get('path', '')) == public_path(path) for g in row.get('guards', [])):
            if process_holds_flock(row['pid'], path) and process_holds_flock(row['pid'], holder.with_suffix('').with_suffix('.lock')):
                return True
    return bool(acquiring_lane_waiters(path))


def process_started_at(pid: int) -> float | None:
    start = pid_start(pid)
    try:
        btime = next(float(l.split()[1]) for l in Path('/proc/stat').read_text().splitlines() if l.startswith('btime '))
        return btime + int(start) / os.sysconf('SC_CLK_TCK') if start else None
    except (OSError, ValueError, StopIteration):
        return None


def scalar_number(value, *, allow_iso: bool = True) -> float | None:
    """Budgets and stamps arrive as epoch numbers, numeric strings or ISO-8601.

    Hand-written and post-merge units register ``acquired_at`` as a timezone
    aware string (2026-10-29/30: 84 false stale-holder advisor calls). Naive ISO
    stamps are local wall time, the same reading as ``datetime.timestamp()``.
    Budgets (``max_min``/``expected_min``) pass ``allow_iso=False``: a date there
    is a broken budget, not a very large one.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            number = float(value)
        else:
            text = str(value).strip()
            if not text:
                return None
            try:
                number = float(text)
            except ValueError:
                if not allow_iso:
                    return None
                try:
                    number = datetime.fromisoformat(text.replace('Z', '+00:00')).timestamp()
                except (ValueError, TypeError, OverflowError, OSError):
                    return None
    except (OverflowError, ValueError):
        return None
    return number if math.isfinite(number) else None


def append_lock_event(root: Path, event: dict) -> None:
    """Append-only observation receipt; a watcher must never fail on it."""
    try:
        path = root / 'control/locks/events.jsonl'
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a') as stream:
            stream.write(json.dumps(event, ensure_ascii=False) + '\n')
    except OSError:
        pass


def reap_holder(root: Path, holder_path: Path, path: Path, holder: dict, reason: str, now: float) -> str:
    """Move dead holder metadata aside, serialized against a new owner.

    The move must not steal a registration written between our read and the
    rename: take a non-blocking exclusive flock (the acquisition path writes the
    holder while it owns that flock and only then releases it), re-read the
    holder under the lock and move it only when it is still the one we judged.
    Returns ``reaped``, ``changed`` (a new registration appeared), ``busy`` (a
    live kernel owner appeared), ``live`` (holder PID alive on recheck) or
    ``failed`` (filesystem/probe error).
    """
    directory = root / 'control/locks/reaped'
    stamp = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(now))
    lock_fd = None
    try:
        lock_fd = os.open(path, os.O_RDONLY | os.O_CREAT | os.O_CLOEXEC, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 'busy'
        except OSError:
            return 'failed'
        if read_json(holder_path) != holder:
            return 'changed'
        # Recheck liveness under the inode lock: the first observation may be
        # stale by the time we acquire it. Age alone never authorizes reaping.
        start = pid_start(holder.get('pid'))
        if start and (not holder.get('pid_start') or holder['pid_start'] == start):
            return 'live'
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / f'{stamp}-{path.name}.json'
        serial = 1
        while target.exists():
            target = directory / f'{stamp}-{path.name}-{serial}.json'
            serial += 1
        holder_path.replace(target)  # same filesystem: atomic rename, never copy+unlink
    except OSError:
        return 'failed'
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
    receipt = {'at': now, 'path': str(path), 'holder_path': str(holder_path), 'reaped': str(target),
               'holder': holder, 'reason': reason}
    append_lock_event(root, {'trigger': 'reaped-holder', **receipt})
    try:
        with (root / 'control/locks/reaped.jsonl').open('a') as stream:
            stream.write(json.dumps(receipt, ensure_ascii=False) + '\n')
    except OSError:
        pass
    return 'reaped'


def process_context(pid: int) -> dict:
    """Best-effort read-only provenance; a disappearing/denied proc is evidence too."""
    proc = Path('/proc') / str(pid)
    result = {'pid': pid, 'cmdline': None, 'cwd': None, 'cgroup': None, 'unit': None}
    errors = {}
    for field in ('cmdline', 'cwd', 'cgroup'):
        try:
            if field == 'cwd':
                result[field] = os.readlink(proc / field)
            elif field == 'cmdline':
                result[field] = (proc / field).read_bytes().rstrip(b'\0').decode(errors='replace').replace('\0', ' ')
            else:
                result[field] = (proc / field).read_text()
                parts = [part for line in result[field].splitlines()
                         for part in line.split(':', 2)[-1].split('/')]
                result['unit'] = next((p for p in reversed(parts) if p.endswith(('.service', '.scope'))), None)
        except OSError as exc:
            errors[field] = str(exc)
    if errors:
        result['errors'] = errors
    return result


def lock_process_contexts(path: Path, owners: list[dict]) -> list[dict]:
    """Keep kernel PIDs and trace live inherited FDs after `flock 9` exits."""
    contexts = {pid: process_context(pid) for pid in sorted({r['pid'] for r in owners})}
    if any(row['cmdline'] is None for row in contexts.values()):
        try:
            result = subprocess.run(['fuser', str(path)], capture_output=True, text=True, timeout=2)
            if result.returncode in (0, 1):
                for value in result.stdout.split():
                    if value.isdigit() and int(value) not in contexts and process_holds_flock(int(value), path):
                        contexts[int(value)] = {**process_context(int(value)), 'inherited_flock': True}
        except (OSError, subprocess.TimeoutExpired):
            pass  # Original proc read errors remain visible; detection still proceeds.
    return list(contexts.values())


def backfill_holder(holder_path: Path, path: Path, pid: int) -> bool:
    """Register an unregistered live flock owner with the default build budget."""
    started, start = process_started_at(pid), pid_start(pid)
    if started is None or not start:
        return False
    try:
        unit = next((l.rsplit('/', 1)[-1] for l in Path(f'/proc/{pid}/cgroup').read_text().splitlines()), '')
        stat = path.stat()
    except OSError:
        return False
    atomic_json(holder_path, {'ticket': None, 'phase': 'unregistered-flock', 'pid': pid, 'pid_start': start,
                              'unit': unit, 'expected_min': 30, 'max_min': 30, 'core': False,
                              'claim': f'backfill:{unit}', 'acquired_at': started,
                              'device': stat.st_dev, 'inode': stat.st_ino, 'backfilled': True})
    return True


def lock_events(state: dict, now: float, *, read_only=False, locks_dir=None) -> list[dict]:
    """Shared lock verdicts; read_only never reaps receipts or appends events.

    Observation clocks live in state; read-only callers may keep them in memory
    or use clocks already persisted by the owning watcher.
    """
    anchor = state.get('state_path') or state.get('run_receipt')
    if not anchor:
        return []
    root = Path(anchor).parent
    text = Path('/proc/locks').read_text()
    waiting = state.get('resource_lock_wait_since', {})
    incomplete = state.get('resource_lock_incomplete_since', {})
    live_waits, live_incomplete = set(), set()
    events = []
    directory = Path(locks_dir) if locks_dir else root / 'control/locks'
    if not read_only:
        for receipt in reap_waiters(directory):
            append_lock_event(root, {'at': now, 'trigger': 'reaped-waiter', **receipt})
    observations = [(p, None) for p in observed_paths(root, locks_dir)]
    observations += [(directory / (old + '.lock'), p)
                     for name, old in RW_ALIASES.items()
                     for p in (directory / (name + '.holders')).glob('*.json')]
    for path, explicit_holder in observations:
        rows = flock_rows(path, text)
        owners = held_flock_rows(path, text)
        # Legacy paths must not borrow canonical holders from a DIFFERENT inode.
        holder_path = explicit_holder or path.with_suffix('.holder.json')
        if not holder_path.exists() and path.with_name(path.name + '.holder.json').exists():
            holder_path = path.with_name(path.name + '.holder.json')  # hand-written <lock>.lock.holder.json
        holder = read_json(holder_path)
        start = pid_start(holder.get('pid'))
        identity = [str(path), holder.get('claim'), holder.get('pid'), holder.get('pid_start'),
                    holder.get('acquired_at'), [(r['pid'], pid_start(r['pid'])) for r in owners or []]]
        def add(trigger, **details):
            digest = hashlib.sha256(json.dumps([trigger, identity], sort_keys=True).encode()).hexdigest()[:24]
            events.append({'key': f'lock-matrix-{trigger}-{digest}', 'trigger': trigger,
                           'path': str(path), 'holder': holder, 'owners': owners, **details})
        reason = None
        if holder_path.exists():
            dead = not start or bool(holder.get('pid_start') and holder['pid_start'] != start)
            if owners is None and read_only:
                pass
            elif owners is None:
                # "Probe failed" is not "confirmed free": never reap (and never page)
                # on it, only leave a receipt instead of the decision we would take.
                append_lock_event(root, {
                    'at': now, 'trigger': 'probe-failed', 'path': str(path), 'holder': holder,
                    'reason': ('kernel flock probe failed; dead holder left in place' if dead else
                               'kernel flock probe failed; live holder left unreported')})
            elif dead and owners:
                add('stale-holder', reason='holder pid absent/reused or metadata invalid')
            elif dead and read_only:
                pass  # The watcher owns mechanical reaping.
            elif dead:
                # Dead metadata is mechanical when the kernel says the lock is free.
                outcome = reap_holder(root, holder_path, path, holder,
                                      'holder pid absent/reused; no kernel flock owner', now)
                if outcome == 'failed':
                    add('stale-holder', reason='holder pid absent/reused or metadata invalid')
                elif outcome != 'reaped':
                    append_lock_event(root, {
                        'at': now, 'trigger': f'reap-skipped-{outcome}', 'path': str(path),
                        'holder': holder,
                        'reason': ('holder changed before reap; new registration kept' if outcome == 'changed'
                                   else 'holder pid alive on recheck; not reaped' if outcome == 'live'
                                   else 'live kernel flock owner appeared before reap')})
            elif not owners:
                reason = 'holder exists without a kernel flock'
            else:
                stat = path.stat()
                if ((holder.get('device', stat.st_dev), holder.get('inode', stat.st_ino)) !=
                        (stat.st_dev, stat.st_ino)):
                    add('stale-holder', reason='holder inode differs from kernel lock')
                elif (holder['pid'] not in [r['pid'] for r in owners]
                      and not process_holds_flock(holder['pid'], path)):
                    add('stale-holder', reason='holder pid does not own or inherit the kernel flock')
                else:
                    acquired = scalar_number(holder.get('acquired_at'))
                    # A budget is a number or numeric string; an ISO stamp there is
                    # a broken budget (it would read as an enormous, never-hit limit).
                    budget = scalar_number(holder.get('max_min'), allow_iso=False)
                    expected = scalar_number(holder.get('expected_min'), allow_iso=False)
                    if acquired is None or budget is None or budget <= 0:
                        add('stale-holder', reason='missing/invalid acquired_at or max_min')
                    elif now - acquired > budget * 60:
                        add('max-min', held_seconds=now-acquired)
                    elif now - acquired > (expected if expected and expected > 0 else budget) * 120:
                        add('expected-min-2x', held_seconds=now-acquired)
        elif owners and (lane_guard_registered(path) or list((path.parent / (canonical_lock(path.stem) + '.holders')).glob('*.json'))):
            acquiring = acquiring_lane_waiters(path)
            if acquiring:
                add('lane-acquiring', waiters=acquiring, informational=True)
        elif owners:
            reason = 'kernel flock held but holder.json missing'
        if reason:
            key = json.dumps(identity, sort_keys=True)
            live_incomplete.add(key)
            first = incomplete.setdefault(key, now)
            # Acquisition publishes metadata immediately after flock; release removes it
            # immediately before close. Do not turn that tiny transition into an advisor call.
            if now - first >= 30:
                missing = not holder_path.exists()
                details = {'processes': lock_process_contexts(path, owners or [])} if missing else {}
                add('missing-holder' if missing else 'stale-holder', reason=reason, **details)
        queue_reported = False
        for row in (r for r in rows if r['blocked']):
            start = pid_start(row['pid'])
            key = f'{row["device"]}:{row["inode"]}:{row["pid"]}:{start}'
            live_waits.add(key)
            first = waiting.setdefault(key, now)
            metadata = read_json(path.with_suffix('.waiters') / f'{row["pid"]}-{start}.json')
            if not metadata:
                metadata = read_json(path.parent / f'{canonical_lock(path.stem)}.waiters/{row["pid"]}-{start}.json')
            if not metadata:
                # Slot helpers and legacy/heavy compatibility waits retain the
                # requested lane's identity, even while queued on another inode.
                for receipt in (p for lane in LANES for p in (path.parent / (lane + '.waiters')).glob('*.json')):
                    candidate = read_json(receipt)
                    if candidate.get('pid') == row['pid'] and candidate.get('pid_start') == start:
                        metadata = candidate
                        break
            queued = scalar_number(metadata.get('queued_at'))
            if metadata.get('pid_start') == start and queued is not None:
                first = min(first, queued)
            if now - first > 1800 and not queue_reported:
                add('queue-30min', waiter=row, waiter_metadata=metadata, waiting_seconds=now-first)
                queue_reported = True  # still retain clocks for every other blocked waiter
    for name, values, live in [('resource_lock_wait_since', waiting, live_waits),
                               ('resource_lock_incomplete_since', incomplete, live_incomplete)]:
        if live or name in state:
            state[name] = {k: v for k, v in values.items() if k in live}
    return events


def launch_advisor(state_path: Path, event: dict, directory: Path) -> None:
    question = directory / 'question.md'
    question.write_text('本机锁矩阵运维触发，请按 advisor-lock-matrix-0320 裁定延长一次、要求释放/拆阶段或核心链插队。'
                        '只作运维裁定，不发 TG、不升用户；本观察器不杀进程。若涉及中途停止核心链，只交 L2 复核。\n\n'
                        + json.dumps(event, ensure_ascii=False, indent=2) + '\n')
    with (directory / 'advisor.log').open('a') as output:
        child = subprocess.Popen(['python3', str(Path(__file__).with_name('host-dag-advisor.py')),
                                  '--state', str(state_path), '--class', 'ops', '--resource-lock',
                                  '--key', event['key'], '--title', '本机锁运维：' + event['trigger'],
                                  '--question-file', str(question)], stdout=output, stderr=subprocess.STDOUT,
                                 start_new_session=True)
    ADVISORS.append(child)


ADVISOR_COOLDOWN_SECONDS = 60 * 60


def watch_locks(state: dict, now: float) -> list[dict]:
    if not (state.get('state_path') or state.get('run_receipt')):
        return []
    for child in ADVISORS[:]:
        if child.poll() is not None:
            ADVISORS.remove(child)
    events = lock_events(state, now)
    root = Path(state.get('state_path') or state['run_receipt']).parent
    sent = []
    # One advisor per lock path per hour, persisted in watch.json so an observer
    # restart cannot reopen the flood. Everything else lands in events.jsonl.
    saved_throttle = state.get('resource_lock_advisor_at')
    throttle = dict(saved_throttle) if isinstance(saved_throttle, dict) else {}
    saved_suppressed = state.get('resource_lock_suppressed')
    suppressed = dict(saved_suppressed) if isinstance(saved_suppressed, dict) else {}
    live_paths, live_keys = set(), set()
    for event in events:
        if event.get('informational'):
            continue
        path_key = event.get('path')
        if path_key:
            live_paths.add(path_key)
        live_keys.add(event['key'])
        directory = root / 'control/locks/advisor' / event['key']
        claim = directory / 'requested.json'
        if claim.exists():
            continue  # durable at-most-once: this exact event already paged the advisor
        last = throttle.get(path_key) if path_key else None
        if isinstance(last, (int, float)) and now - last < ADVISOR_COOLDOWN_SECONDS:
            if not isinstance(suppressed.get(event['key']), (int, float)):
                append_lock_event(root, {'at': now, 'suppressed': True,
                                         'reason': 'advisor cooldown: one call per lock path per hour',
                                         **event})
                suppressed[event['key']] = now
            continue
        directory.mkdir(parents=True, exist_ok=True)
        try:
            # Durable at-most-once, even across observer restarts; never re-ask the
            # same key automatically (advisor interprets a repeat as escalation).
            with claim.with_suffix('.lock').open('a') as handle:
                fcntl.flock(handle, fcntl.LOCK_EX)
                if claim.exists():
                    raise FileExistsError(claim)
                atomic_json(claim, {'at': now, **event})
        except FileExistsError:
            continue
        try:
            launch_advisor(Path(state.get('state_path') or root / 'watch.json'), event, directory)
        except OSError as exc:
            atomic_json(directory / 'launch-error.json', {'at': now, 'error': str(exc)})
            claim.unlink(missing_ok=True)  # no child launched; retry is safe
            continue
        if path_key:
            throttle[path_key] = now
        sent.append(event)
    if events or isinstance(saved_throttle, dict):
        state['resource_lock_advisor_at'] = {
            path: at for path, at in throttle.items()
            if path in live_paths or (isinstance(at, (int, float)) and now - at < 7 * 86400)}
    if events or isinstance(saved_suppressed, dict):
        state['resource_lock_suppressed'] = {key: at for key, at in suppressed.items() if key in live_keys}
    return sent


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--lock', action='append', choices=RESOURCE_LOCK_ORDER, required=True)
    parser.add_argument('--ticket', required=True)
    parser.add_argument('--phase', required=True)
    parser.add_argument('--unit')
    parser.add_argument('--expected-min', type=float, default=10)
    parser.add_argument('--max-min', type=float)
    parser.add_argument('-w', '--wait', type=float, default=7200, help='total flock queue deadline, seconds')
    parser.add_argument('--memory', choices=('build', 'cluster'), help='build=12GiB; cluster=8GiB')
    parser.add_argument('--memory-gib', type=float, default=0)
    parser.add_argument('--memory-wait', type=float, default=600)
    parser.add_argument('--shared', action='store_true')
    parser.add_argument('--jest-shard', action='store_true', help='explicit Jest shard: allow two FE slots')
    parser.add_argument('--core', action='store_true', help='deprecated: core is derived from the run ticket')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('a foreground command after -- is required')
    if args.jest_shard and not re.search(r'--shard(?:=|\s|$)', ' '.join(command)):
        parser.error('--jest-shard requires an explicit Jest --shard command')
    memory = {'build': 12, 'cluster': 8}.get(args.memory, args.memory_gib)
    try:
        with ResourceLocks(args.run_dir, args.lock, ticket=args.ticket, phase=args.phase,
                           expected_min=args.expected_min, max_min=args.max_min, unit=args.unit,
                           wait_seconds=args.wait, memory_gib=memory, memory_wait=args.memory_wait, core=args.core, shared=args.shared,
                           jest_shard=args.jest_shard):
            code = run_command(command, job=args.ticket)
            return code if code >= 0 else 128 - code
    except LockTimeout as exc:
        print(str(exc), file=sys.stderr)
        return 75
    except Terminated as exc:
        return 128 + exc.signum
    except (ValueError, OSError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        # TOOL-GUARD-FAIL-CLOSED (Fable L2177 §4(c)): a crashed admission guard is a refusal, never a run.
        print(f'INFRA_ERROR: lock guard crashed ({type(exc).__name__}: {exc}); command not run '
              '(BLOCKED, not a product FAIL)', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())


def capacity_admission(snapshot, *, cpu=0, memory=0, storage=None):
    """Read-only adapter boundary. CPU cores, memory/storage GiB; caller supplies
    cluster resource free values. Missing data fails closed.
    """
    requests = {'cpu': cpu, 'memory': memory}
    available = {}
    for key in requests:
        available[key] = snapshot['allocatable'][key] - snapshot['used'][key]
    requests.update({'storage:' + k: v for k, v in (storage or {}).items()})
    available.update({'storage:' + k: v for k, v in snapshot['storage_free'].items()})
    for value in [*requests.values(), *available.values()]:
        positive(value, 'capacity', zero=True)
    shortages = {k: v - available.get(k, 0) for k, v in requests.items() if v > available.get(k, 0)}
    return {'admitted': not shortages, 'shortages': shortages, 'available': available}

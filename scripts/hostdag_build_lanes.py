"""Build profiles and flock slot leases, shared by local and remote runner workers."""
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import socket
import select
import struct
import subprocess
import sys
import time
import uuid

LANES = {'java-build': (1, 6), 'fe-build': (1, 12), 'go-test': (2, 4), 'image-pack': (1, 0)}


def remote_profile(runner, stage):
    """remote budgets, not host thresholds."""
    lane, shard = stage['lane'], stage.get('jest_shard', False)
    if runner == '154':
        cap, minimum, slots = {'go-test':(4,5,2), 'java-build':(6,7,1),
                               'image-pack':(8,0,1), 'fe-build':(4,5,2) if shard else (8,9,1)}[lane]
        return dict(cap=cap, minimum=minimum, slots=slots, total=10)
    if runner == '155':
        if lane != 'go-test' and not (lane == 'fe-build' and shard):
            raise ValueError('runner155 accepts only Go or explicit Jest shards')
        return dict(cap=4, minimum=4.5, slots=1, total=4)
    return None  # ARM retains its existing CAS contract and default admission.


def phase_lane(phase):
    value = phase.lower().replace('_', '-').replace(' ', '-')
    for lane, patterns in (
        ('image-pack', ('image-pack', 'docker', 'oci', 'skopeo', 'platform-pack', 'offline-pack', 'image-build')),
        ('java-build', ('java-build', 'maven', 'mvn')),
        ('fe-build', ('fe-build', 'umi', 'max-build', 'jest', 'tsc', 'npm', 'frontend')),
        ('go-test', ('go-test', 'go-vet', 'go-build', 'golang', 'vet')),
    ):
        if any(p in value for p in patterns):
            return lane
    return None


def bounded_flock(fd, seconds, shared=False):
    if seconds <= 0:
        raise TimeoutError('build lane queue deadline expired')
    def expired(signum, frame):
        raise TimeoutError('build lane queue deadline expired')
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError('active ITIMER_REAL while acquiring build lane')
    old = signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        fcntl.flock(fd, fcntl.LOCK_SH if shared else fcntl.LOCK_EX)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old)


class LaneLease:
    """One slot plus compatibility guards. Never replace or unlink lock files.

    Probe each slot once, then join one kernel flock queue (no polling retry).
    EX legacy callers, including processes started before cutover, conflict with
    the SH guard on their original inode. Image packing takes heavy before build.
    """
    def __init__(self, directory, lane, wait_seconds=1800, held_paths=(),
                 blocking=bounded_flock, jest_shard=False, timeout_error=TimeoutError, metadata=None, slots=None):
        if lane not in LANES:
            raise ValueError('unknown build lane')
        self.directory = Path(directory)
        self.lane = lane
        self.wait_seconds = wait_seconds
        self.held_paths = set(str(Path(os.path.abspath(Path(p).expanduser()))) for p in held_paths)
        self.blocking = blocking
        self.jest_shard = jest_shard
        self.timeout_error = timeout_error
        self.metadata = metadata or {}
        self.slots = slots
        self.refs = []
        self.ref = None

    def open_ref(self, path, shared=False):
        ref = dict(fd=os.open(str(path), os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600),
                   path=str(Path(os.path.abspath(path))), shared=shared)
        self.refs.append(ref)
        return ref

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.wait_seconds
        guards = []
        if self.lane == 'image-pack':
            guards.append(('local-heavy-admission.lock', False))
        guards.append(('workstation-build.lock', True))
        if self.lane == 'fe-build':
            # Full builds exclude both shard slots; only explicit shard jobs share.
            guards.append(('fe-build.capacity.lock', self.jest_shard))
        try:
            for name, shared in guards:
                path = self.directory / name
                if str(Path(os.path.abspath(path))) in self.held_paths:
                    continue
                ref = self.open_ref(path, shared)
                self.blocking(ref['fd'], deadline-time.monotonic(), shared=shared)
            count = self.slots or (2 if self.lane == 'fe-build' and self.jest_shard else LANES[self.lane][0])
            slots = []
            for slot in range(count):
                ref = self.open_ref(self.directory / ('%s.slot-%d.lock' % (self.lane, slot)))
                slots.append(ref)
                try:
                    fcntl.flock(ref['fd'], fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self.ref = ref
                    break
                except BlockingIOError:
                    pass
            if self.ref is None:
                self.ref = self.wait_any(slots, deadline)
            for ref in slots:
                if ref is not self.ref:
                    os.close(ref['fd'])
                    self.refs.remove(ref)
            self.ref['guards'] = [r for r in self.refs if r is not self.ref]
            self.ref['slot'] = int(re.search(r'\.slot-(\d+)\.lock$', self.ref['path']).group(1))
            return self
        except BaseException:
            self.__exit__()
            raise

    def wait_any(self, slots, deadline):
        """Wait in each kernel queue, keep the first granted OFD via SCM_RIGHTS.

        Two short-lived ordinary processes let either free slot wake the caller;
        there is no repeated LOCK_NB probe or polling window. Helpers are drained
        before returning and have their own timeout if their parent is killed.
        """
        helper = '''import fcntl,json,os,pathlib,signal,socket,struct,sys,time
path, sockfd, seconds, receipt, metadata = sys.argv[1:]
signal.signal(signal.SIGALRM, lambda *a: sys.exit(75))
signal.setitimer(signal.ITIMER_REAL, max(.01,float(seconds)))
sock=socket.socket(fileno=int(sockfd)); fd=os.open(path,os.O_RDWR)
row=json.loads(metadata); row.update(pid=os.getpid(),pid_start=pathlib.Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19])
p=pathlib.Path(receipt); p.parent.mkdir(parents=True,exist_ok=True); p.write_text(json.dumps(row))
try:
 fcntl.flock(fd,fcntl.LOCK_EX)
 sock.sendmsg([b'L'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,struct.pack('i',fd))])
 sock.recv(1)
finally:
 if p.exists(): p.unlink()
'''
        children, sockets, receipts = [], [], []
        try:
            for ref in slots:
                parent, child = socket.socketpair()
                sockets.append(parent)
                receipt = self.directory / (self.lane + '.waiters') / ('slotwait-' + uuid.uuid4().hex + '.json')
                receipts.append(receipt)
                try:
                    children.append(subprocess.Popen([sys.executable, '-c', helper, ref['path'], str(child.fileno()),
                                                       str(deadline-time.monotonic()), str(receipt), json.dumps(self.metadata)],
                                                      pass_fds=(child.fileno(),), stdin=subprocess.DEVNULL))
                finally:
                    child.close()
            ready = select.select(sockets, [], [], max(0, deadline-time.monotonic()))[0]
            if not ready:
                raise self.timeout_error('build lane queue deadline expired')
            winner = ready[0]
            data, ancillary, _, _ = winner.recvmsg(1, socket.CMSG_SPACE(struct.calcsize('i')))
            if data != b'L' or not ancillary:
                raise self.timeout_error('build lane queue helper expired')
            fd = struct.unpack('i', ancillary[0][2][:struct.calcsize('i')])[0]
            ref = slots[sockets.index(winner)]
            os.close(ref['fd'])
            ref['fd'] = fd
            return ref
        finally:
            for child in children:
                child.terminate()
            for child in children:
                child.wait()
            for sock in sockets:
                sock.close()
            for receipt in receipts:
                if receipt.exists():
                    receipt.unlink()

    def __exit__(self, *exc):
        for ref in reversed(self.refs):
            os.close(ref['fd'])
        self.refs.clear()


def descriptors(refs):
    return [r['fd'] for ref in refs.values() for r in [ref] + ref.get('guards', [])]


def mem_available_gib():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) / (1024 ** 2)


class Reservation:
    """Atomic per-runner memory reservation; budget waits wake on active flock release."""
    def __init__(self, directory, profile, row, deadline):
        self.directory, self.profile, self.row, self.deadline = Path(directory), profile, row, deadline
        self.path = self.directory / 'reservations.json'
        self.active = self.directory / ('budget-' + row['claim'] + '.lock')
        self.fd = None

    def ledger(self):
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def save(self, rows):
        tmp = self.path.with_name(self.path.name + '.' + self.row['claim'])
        tmp.write_text(json.dumps(rows, indent=2) + '\n')
        os.replace(str(tmp), str(self.path))

    def __enter__(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        while True:
            if time.monotonic() >= self.deadline:
                raise TimeoutError('runner admission expired; MemAvailable=%.2fGiB' % mem_available_gib())
            with (self.directory/'admission.lock').open('a') as mutex:
                bounded_flock(mutex.fileno(), self.deadline-time.monotonic())
                rows = self.ledger()
                # Reap only kernel-unlocked reservations, including crashed workers.
                for claim, row in list(rows.items()):
                    with Path(row['active']).open('a') as active:
                        try:
                            fcntl.flock(active, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        except BlockingIOError:
                            continue
                    del rows[claim]
                available = mem_available_gib()
                enough = sum(r['cap'] for r in rows.values()) + self.profile['cap'] <= self.profile['total']
                if enough and available >= self.profile['minimum']:
                    self.fd = os.open(str(self.active), os.O_CREAT | os.O_RDWR | os.O_CLOEXEC, 0o600)
                    fcntl.flock(self.fd, fcntl.LOCK_EX)
                    rows[self.row['claim']] = dict(self.row, active=str(self.active), cap=self.profile['cap'],
                                                  mem_available_gib=available)
                    try:
                        self.save(rows)
                    except BaseException:
                        os.close(self.fd)
                        self.fd = None
                        raise
                    return self
                self.save(rows)
            if rows and not enough:
                waiter = LaneLease(self.directory, 'go-test', metadata=self.row)
                refs = [dict(fd=os.open(r['active'], os.O_RDWR), path=r['active']) for r in rows.values()]
                try:
                    waiter.wait_any(refs, self.deadline)
                finally:
                    for ref in refs:
                        os.close(ref['fd'])
            else:
                # Memory is an external gauge, unlike slots; no resource lease is held.
                time.sleep(min(5, max(0,self.deadline-time.monotonic())))

    def __exit__(self, *exc):
        if self.fd is not None:
            # Closing is enough; the next atomic admission reaps this row only
            # when the last inherited FD closes. Never trust PID liveness alone.
            os.close(self.fd)
            self.fd = None
            with (self.directory/'admission.lock').open('a') as mutex:
                bounded_flock(mutex.fileno(), 5)
                with self.active.open('a') as active:
                    try:
                        fcntl.flock(active, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        return  # an inherited child still owns the reservation
                    rows = self.ledger()
                    rows.pop(self.row['claim'], None)
                    self.save(rows)


class RunnerPhase:
    """Runner-local counterpart of ResourceLocks, using the same slot protocol."""
    def __init__(self, directory, stage, ticket, expected_min=10, max_min=30, unit=None, runner=None,
                 progress_path=None):
        self.directory, self.stage = Path(directory), stage
        self.lane = stage['lane']
        self.claim = uuid.uuid4().hex
        self.lease = None
        self.holder = None
        self.reservation = None
        self.profile = remote_profile(runner, stage)
        self.progress_path = progress_path
        self.waiter = self.directory / (self.lane + '.waiters') / (str(os.getpid()) + '.json')
        start = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19]
        self.row = dict(ticket=ticket, phase=stage['phase'], lock=self.lane, requested_lock=self.lane,
                        pid=os.getpid(), pid_start=start, unit=unit, expected_min=expected_min,
                        max_min=max_min, claim=self.claim, queued_at=time.time(),
                        jest_shard=stage.get('jest_shard', False))

    def write(self, path, row):
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + '.' + self.claim + '.tmp')
        tmp.write_text(json.dumps(row, indent=2) + '\n')
        os.replace(str(tmp), str(path))

    def __enter__(self):
        deadline = time.monotonic() + self.row['max_min'] * 60
        try:
            self.write(self.waiter, dict(self.row, phase='memory-wait', requested_phase=self.row['phase']))
            if self.progress_path:
                self.write(self.progress_path, dict(self.row, waiting_for='memory/admission', profile=self.profile))
            if self.profile:
                self.reservation = Reservation(self.directory, self.profile, self.row, deadline).__enter__()
            else:
                minimum = LANES[self.lane][1]
                while mem_available_gib() < minimum:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('runner memory admission deadline expired')
                    time.sleep(min(5, max(0, deadline-time.monotonic())))
            self.write(self.waiter, self.row)
            if self.progress_path:
                self.write(self.progress_path, dict(self.row, waiting_for='lane-slot'))
            self.lease = LaneLease(self.directory, self.lane, deadline-time.monotonic(),
                                   jest_shard=self.stage.get('jest_shard', False), metadata=self.row,
                                   slots=self.profile['slots'] if self.profile else None).__enter__()
            ref = self.lease.ref
            stat = os.fstat(ref['fd'])
            self.row.update(acquired_at=time.time(), path=ref['path'], slot=ref['slot'],
                            guards=ref['guards'], device=stat.st_dev, inode=stat.st_ino)
            self.holder = Path(ref['path']).with_suffix('.holder.json')
            self.write(self.holder, self.row)
            if self.progress_path:
                self.write(self.progress_path, dict(self.row, waiting_for=None, status='running'))
            return self
        except BaseException:
            self.__exit__()
            raise
        finally:
            if self.waiter.exists():
                self.waiter.unlink()

    def __exit__(self, *exc):
        try:
            if self.holder and self.holder.exists():
                if json.loads(self.holder.read_text()).get('claim') == self.claim:
                    self.holder.unlink()
        finally:
            if self.lease:
                self.lease.__exit__()
                self.lease = None
            if self.reservation:
                self.reservation.__exit__()
                self.reservation = None

    def pass_fds(self):
        return tuple(descriptors({self.lane: self.lease.ref}) +
                     ([self.reservation.fd] if self.reservation else []))

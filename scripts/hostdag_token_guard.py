"""Shared executor guard. No daemon, board writes, model calls, or automatic retries.

Codex is sampled by the existing watcher's host reads; CLI executors are sampled
by their existing supervisor. Only explicitly bound executors can be enforced.
"""
from __future__ import annotations

from hostdag_json import public_path

import contextlib
import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

CAPS = {'T0': 30_000_000, 'T1': 80_000_000, 'T2': 200_000_000, 'T3': 300_000_000}
FIELDS = ('target_files', 'failed_command', 'last_output', 'ruled_out', 'next_step', 'created_resources')
PROC_ROOT = Path('/proc')
MAX_FOREGROUND_WAIT_SECONDS = 45 * 60
# Coordinator policy is advisory and separate from executor stop/retry guards.
COORDINATOR_SOFT_CONTEXT_FRACTION = .30
COORDINATOR_CONTEXT_FRACTION = .35
COORDINATOR_FORCE_CONTEXT_FRACTION = .60
COORDINATOR_MAX_AGE_SECONDS = 24 * 60 * 60
COORDINATOR_MAX_RESUMES = 30  # fallback only when current context is unknown


def coordinator_rotation_reasons(*, age_seconds=0, context_fraction=None, resumes=0,
                                 resume_limit=COORDINATOR_MAX_RESUMES, compressed=False):
    """Rotation signals; callers still enforce idle and handoff guardrails."""
    reasons = []
    if age_seconds >= COORDINATOR_MAX_AGE_SECONDS:
        reasons.append('age_24h')
    if context_fraction is not None:
        if context_fraction >= COORDINATOR_FORCE_CONTEXT_FRACTION:
            reasons.append('context_60_percent')
        elif context_fraction >= COORDINATOR_CONTEXT_FRACTION:
            reasons.append('context_35_percent')
        elif context_fraction >= COORDINATOR_SOFT_CONTEXT_FRACTION:
            reasons.append('context_30_percent')
    elif resumes >= resume_limit:
        reasons.append('resume_limit')
    if compressed:
        reasons.append('auto_compaction')
    return reasons


HANDOFF_TEMPLATE = '''# Executor handoff
target_files: <file:line; include every target>
failed_command: <exact failed command, or none>
last_output: <last output or evidence path, or none>
ruled_out: <excluded approaches, or none>
next_step: <one sentence>
created_resources: <objects with cleanup_owner/due_at, or none>
'''


def policy(contract, ticket=None):
    ticket = ticket or {}
    match = re.search(r'(?im)^\s*(?:tier|级别|审查级别)\s*[:：=]\s*(T[0-3])\b', contract)
    if not match:
        match = re.search(r'^.{0,200}?[（(,，\s](T[0-3])\b', contract.split('\n\n', 1)[0])
    tier = str(ticket.get('tier') or (match[1] if match else 'T2')).upper()
    tier = tier if tier in CAPS else 'T2'
    limits = [CAPS[tier]]
    for key in ('token_budget', 'token_limit', 'effective_input_token_limit'):
        value = ticket.get(key)
        if value is not None:
            limits.append(_tokens(str(value)))
    for line in contract.splitlines():
        if re.search(r'(?i)(?:token[_ -]?(?:budget|limit)|预算|budget)\s*[:：=≤<]', line):
            for value, suffix in re.findall(r'(?i)(\d+(?:\.\d+)?)\s*([MK])\b', line):
                limits.append(_tokens(value + suffix))
            match = re.search(r'(?i)(?:token[_ -]?(?:budget|limit)|有效输入\s*token\s*上限)\s*[:：=≤]\s*(\d+)\s*$', line)
            if match:
                limits.append(int(match[1]))
    return tier, min(n for n in limits if n > 0)


def _tokens(value):
    match = re.fullmatch(r'\s*(\d+(?:\.\d+)?)\s*([MK]?)\s*', value, re.I)
    if not match:
        raise ValueError('Invalid effective input token limit')
    return int(float(match[1]) * {'': 1, 'K': 1000, 'M': 1_000_000}[match[2].upper()])


def validate_handoff(path):
    text = Path(path).read_text(encoding='utf-8')
    values = {}
    for field in FIELDS:
        match = re.search(rf'(?m)^{field}:[ \t]*(\S[^\n]*)$', text)
        if not match or match[1].startswith('<'):
            raise ValueError(f'Handoff rejected: missing/empty {field}; 补写交接字段')
        values[field] = match[1].strip()
    if not re.search(r'\S+:\d+\b', values['target_files']):
        raise ValueError('Handoff rejected: target_files requires file:line')
    return text


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f'.{os.getpid()}.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def index_path(identity):
    root = Path(os.environ.get('HOST_DAG_TOKEN_GUARD_STATE',
                               Path.home() / '.local/state/host-dag-token-guard'))
    return root / (hashlib.sha256(identity.encode()).hexdigest() + '.json')


def binding(identity):
    path = index_path(identity)
    return Path(json.loads(path.read_text())['state']) if path.exists() else None


@contextlib.contextmanager
def locked(identity):
    path = binding(identity)
    if path is None:
        yield None
        return
    with path.with_suffix('.lock').open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        g = Guard(path, json.loads(path.read_text()))
        try:
            yield g
        finally:
            write_json(path, g.state)


def read_usage(path, old, provider):
    """Incremental complete JSONL records. Cached Codex input is already in input_tokens."""
    state = dict(old)
    state.setdefault('tokens', 0)
    state.setdefault('rounds', 0)
    if not path or not Path(path).is_file():
        return state
    path = Path(path)
    stat = path.stat()
    file_id = f'{stat.st_dev}:{stat.st_ino}'
    offset = state.get('offset', 0)
    if state.get('file_id') != file_id or stat.st_size < offset:
        offset = 0
    with path.open('rb') as handle:
        handle.seek(offset)
        while raw := handle.readline():
            if not raw.endswith(b'\n'):
                break
            offset = handle.tell()
            try:
                row = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                continue
            if not isinstance(row, dict):
                continue
            track_work(state, row, provider)
            if provider == 'codex':
                payload = row.get('payload') or {}
                info = payload.get('info') or {}
                if row.get('type') != 'event_msg' or payload.get('type') != 'token_count' or not info:
                    continue
                total = info.get('total_token_usage') or {}
                signature = [total.get('input_tokens', 0), total.get('output_tokens', 0)]
                if signature != state.get('last_total'):
                    state['rounds'] += 1
                    state['last_total'] = signature
                state['tokens'] = max(state['tokens'], int(total.get('input_tokens') or 0))
                window = info.get('model_context_window')
                if window:
                    state['context_fraction'] = (info.get('last_token_usage') or {}).get('input_tokens', 0) / window
            else:
                if row.get('type') == 'result':
                    state['result'] = row
                message = row.get('message') or {}
                if not isinstance(message, dict):
                    # Some Claude stream rows (e.g. system notices) carry a plain-string message.
                    continue
                usage = message.get('usage') or {}
                if not isinstance(usage, dict):
                    continue
                mid = message.get('id')
                if row.get('type') != 'assistant' or not mid or not usage:
                    continue
                # Claude emits multiple content blocks with the same message id.
                seen = state.setdefault('messages', {})
                count = sum(int(usage.get(k) or 0) for k in
                            ('input_tokens', 'cache_read_input_tokens', 'cache_creation_input_tokens'))
                prior = seen.get(mid)
                state['tokens'] += max(0, count - (prior or 0))
                state['rounds'] += int(prior is None)
                seen[mid] = max(count, prior or 0)
                # No hard-coded model context window: supplied by runtime/control when known.
                if usage.get('context_window'):
                    state['context_fraction'] = count / usage['context_window']
    state.update(offset=offset, file_id=file_id)
    return state


def session_ids(text):
    return set(re.findall(r'(?:session_id|cell_id)["\s]*:\s*["\']?([\w-]+)', text)) - {'null', 'None'}


def track_work(state, row, provider):
    """Track outstanding calls, including exec calls which yield a live session.

    Token updates are independent: unchanged usage doesn't mean a tool finished.
    Only assistant declarations are accepted as explicit waits, never tool text.
    """
    pending = state.setdefault('pending_tools', {})
    sessions = set(state.get('tool_sessions', []))
    texts = []
    if provider == 'codex':
        p = row.get('payload') or {}
        kind = p.get('type')
        if row.get('type') == 'event_msg' and kind in {'task_started', 'task_complete', 'turn_aborted'}:
            pending.clear()
            sessions.clear()
        if row.get('type') == 'response_item':
            call = p.get('call_id') or p.get('id')
            if kind in {'function_call', 'custom_tool_call', 'local_shell_call'} and call:
                pending[call] = list(session_ids(str(p.get('arguments') or p.get('input') or '')))
            elif kind in {'function_call_output', 'custom_tool_call_output', 'local_shell_call_output'}:
                requested = set(pending.pop(call, []))
                output = str(p.get('output') or '')
                yielded = set(re.findall(r'(?:Process running with session ID|Script running with cell ID)\s+([\w-]+)', output))
                yielded.update(session_ids(output))
                sessions.update(yielded)
                if not yielded and re.search(r'Process exited with code|Script completed|["\']?exit_code["\']?\s*:\s*-?\d+', output):
                    sessions.difference_update(requested)
            elif kind == 'message' and p.get('role') == 'assistant':
                texts = [b.get('text', '') for b in p.get('content', []) if isinstance(b, dict)]
    else:
        message = row.get('message') or {}
        # Some stream rows (e.g. interrupts and system notices) carry a plain string message.
        blocks = message.get('content') or [] if isinstance(message, dict) else []
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                if row.get('type') == 'assistant' and block.get('type') == 'tool_use':
                    pending[block['id']] = []
                elif row.get('type') == 'user' and block.get('type') == 'tool_result':
                    pending.pop(block.get('tool_use_id'), None)
                elif row.get('type') == 'assistant' and block.get('type') == 'text':
                    texts.append(block.get('text', ''))
        if row.get('type') == 'result':
            pending.clear()
            sessions.clear()
    for text in texts:
        for line in text.splitlines():
            if line.strip() == 'TOKEN-GUARD-RESUME':
                state.pop('declared_wait', None)
            match = re.fullmatch(r'TOKEN-GUARD-WAIT:\s*(\d+(?:\.\d+)?)\s+(.+)', line.strip())
            if match:
                state['declared_wait'] = {'until': float(match[1]), 'reason': match[2]}
    state['tool_sessions'] = sorted(sessions)


def process_activity(cwd, root_pid=None):
    """Diagnostic only: process presence never grants a no-progress exemption.

    Claude owns a child tree; Codex commands use the ticket's isolated worktree.
    Exclude this observer and its ancestors so polling cannot exempt itself.
    """
    rows = {}
    for p in PROC_ROOT.iterdir():
        if not p.name.isdigit():
            continue
        try:
            stat = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
            if stat[0] == 'Z':
                continue
            rows[int(p.name)] = (int(stat[1]), p)
        except (OSError, ValueError, IndexError):
            continue
    excluded = set()
    pid = os.getpid()
    while pid and pid not in excluded:
        excluded.add(pid)
        pid = rows.get(pid, (0,))[0]
    descendants = {root_pid} if root_pid else set()
    if root_pid:
        while True:
            more = {pid for pid, (parent, _) in rows.items() if parent in descendants}
            if more <= descendants:
                break
            descendants.update(more)
    cwd = Path(cwd).resolve()
    for pid, (_, p) in rows.items():
        if pid in excluded:
            continue
        try:
            if root_pid:
                scoped = pid in descendants
            else:
                scoped = Path(os.readlink(p / 'cwd')).is_relative_to(cwd)
            if scoped:
                comm = (p / 'comm').read_text().strip()
                wchan = (p / 'wchan').read_text().strip()
                if comm == 'flock' or 'lock' in wchan:
                    return 'flock'
                if pid != root_pid:
                    return 'process'
        except OSError:
            continue
    return None


def wait_reason(state, thread=None, *, provider='codex'):
    """Identity of the current unreturned foreground call, not a process/session.

    Codex requires a live app-server item. Claude's equivalent is a stream
    tool_use without its tool_result. Declarations, board WAIT, /proc activity,
    and an exec result which already yielded a session aren't blocking calls.
    """
    for turn in (thread or {}).get('turns', [])[-1:]:
        if turn.get('status') != 'inProgress':
            continue
        for item in turn.get('items', []):
            if (item.get('type') in {'commandExecution', 'mcpToolCall', 'dynamicToolCall', 'webSearch', 'fileChange'}
                    and item.get('status') in {'inProgress', 'running'} and item.get('id')):
                return f"native:{turn['id']}:{item['id']}"
    if provider == 'claude':
        pending = state.get('usage', {}).get('pending_tools', {})
        if pending:
            return 'claude:' + sorted(pending)[0]
    return None


def _snapshot(cwd, evidence, excluded):
    cwd, evidence = Path(cwd), Path(evidence)
    excluded = {str(public_path(p)) for p in excluded}
    def allowed(p):
        return str(public_path(p)) not in excluded and not p.name.startswith('handoff-')
    files = {}
    result = subprocess.run(['git', '-C', str(cwd), 'ls-files', '-z', '--cached', '--others',
                             '--exclude-standard'], capture_output=True)
    paths = [cwd / os.fsdecode(p) for p in result.stdout.split(b'\0') if p] if result.returncode == 0 else cwd.rglob('*')
    for p in paths:
        if not allowed(p) or p.is_symlink() or not p.is_file() or '.git' in p.parts:
            continue
        try:
            st = p.stat()
            files[str(p)] = [st.st_size, st.st_mtime_ns]
        except FileNotFoundError:
            pass
    new_evidence = sorted(str(p) for p in evidence.rglob('*') if p.is_file() and allowed(p)) if evidence.exists() else []
    return files, new_evidence


class Guard:
    def __init__(self, path, state):
        self.path, self.state = path, state
        self.evidence = Path(state['evidence'])
        self.events = Path(state['run']) / 'control/token-guard/events.jsonl'
        self.handoff = self.evidence / f"handoff-{state.get('handoff_number', 1)}.md"

    @classmethod
    def create(cls, run, ticket, identity, cwd, contract, *, mode='dry-run', evidence=None,
               ticket_record=None, exclude=(), now=None, publish=False, phase=None,
               handoff_number=1, retry_count=0, predecessor=None):
        if mode not in {'dry-run', 'enforce'}:
            raise ValueError('guard mode must be dry-run or enforce')
        if not re.fullmatch(r'[A-Za-z0-9_.-]+', ticket) or ticket in {'.', '..'}:
            raise ValueError('guard requires a ticket id')
        tier, cap = policy(contract, ticket_record)
        run = public_path(run)
        evidence = public_path(evidence or run / 'evidence' / ticket)
        path = run / 'control/token-guard' / (hashlib.sha256(identity.encode()).hexdigest() + '.json')
        if path.exists():
            raise ValueError('Guard identity already exists; retries require a new session')
        state = dict(run=str(run), ticket=ticket, identity=identity, cwd=str(Path(cwd).resolve()),
                     evidence=str(evidence), mode=mode, tier=tier, cap=cap, exclude=list(map(str, exclude)),
                     last_progress=time.time() if now is None else now, base_round=0, strikes=0,
                     retry_count=retry_count, warned=False, stopped=False, phase=phase,
                     handoff_number=handoff_number, predecessor=predecessor)
        state.update(last_observed=state['last_progress'], active=True)
        g = cls(path, state)
        state['files'], state['evidence_files'] = _snapshot(cwd, evidence, exclude)
        with locked(predecessor) if predecessor else contextlib.nullcontext(None) as old:
            if predecessor and (old is None or old.state.get('successor')):
                raise ValueError('Predecessor already has a successor or is unavailable')
            write_json(path, state)
            if publish:
                write_json(index_path(identity), {'state': str(path)})
            if old:
                old.state['successor'] = identity
        return g

    @property
    def blocked(self):
        return self.state['mode'] == 'enforce' and self.state['stopped']

    def failed(self):
        if self.state['mode'] == 'enforce' and not self.blocked:
            self.state.update(stopped=True, stop_confirmed=True, retry_count=self.state['retry_count'] + 1)
            self.log(dict(identity=self.state['identity'], ticket=self.state['ticket'], mode='enforce',
                          action='stop', reason='failed_retry', handoff=str(self.handoff)))

    def observe(self, *, now=None, rounds=0, tokens=0, phase=None, context_fraction=0, active=True, waiting=None):
        s = self.state
        now = time.time() if now is None else now
        files, evidence = _snapshot(s['cwd'], self.evidence, s['exclude'])
        progress = files != s['files'] or bool(set(evidence) - set(s['evidence_files']))
        previous_rounds = s.get('rounds', rounds)
        previous_wait = s.get('waiting')
        last_observed = s.get('last_observed', now)
        paused = 0
        if not active or not s.get('active', True):
            paused = max(0, now - last_observed)
        elif (previous_wait and waiting in {previous_wait, None}
              and rounds == previous_rounds == s.get('wait_rounds')):
            # Charge all time after the original deadline, even across a sparse
            # poll, a warning, or a brief model continuation on the same call.
            deadline = s['wait_started'] + MAX_FOREGROUND_WAIT_SECONDS
            paused = max(0, min(now, deadline) - last_observed)
        if waiting != previous_wait:
            s.update(wait_started=now if waiting else None,
                     wait_rounds=rounds if waiting else None)
        s.update(files=files, evidence_files=evidence, tokens=tokens, rounds=rounds)
        # Only the time clock pauses. Round progress and strikes never do.
        s['last_progress'] += paused
        s.update(last_observed=now, active=active, waiting=waiting)
        if progress:
            s.update(last_progress=now, base_round=rounds, strikes=0)
        previous_phase = s.get('phase')
        if phase:
            s['phase'] = phase
        if s['stopped']:
            return []
        events = []
        def event(action, reason):
            if action == 'stop':
                s.update(stopped=True, retry_count=s['retry_count'] + 1)
            events.append(dict(at=now, identity=s['identity'], ticket=s['ticket'], mode=s['mode'],
                               action=action, reason=reason, tokens=tokens, cap=s['cap'], rounds=rounds,
                               handoff=str(self.handoff), retry_count=s['retry_count']))
        if tokens >= s['cap']:
            event('stop', 'token_100')
        elif previous_phase == 'deploy' and phase == 'acceptance' and context_fraction > .4 and s['tier'] not in {'T0', 'T1'}:
            event('stop', 'phase_boundary')
        else:
            if tokens >= s['cap'] * .8 and not s['warned']:
                s['warned'] = True
                event('handoff', 'token_80')
            if active and (rounds - s['base_round'] >= 15 or now - s['last_progress'] >= 600):
                s.update(strikes=s['strikes'] + 1, base_round=rounds, last_progress=now)
                event('checkpoint' if s['strikes'] == 1 else 'stop', 'no_progress')
        for item in events:
            self.log(item)
        write_json(self.path, s)
        return events

    def log(self, event):
        self.events.parent.mkdir(parents=True, exist_ok=True)
        with self.events.open('a') as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            handle.write(json.dumps(event, ensure_ascii=False) + '\n')

    def instruction(self):
        return ('TOKEN-GUARD: 写检查点，并用一句话声明交付物。写交接到 ' + str(self.handoff)
                + '；逐字段替换占位符，没有则写 none。缺字段宿主拒收；重试必须新会话。\n' + HANDOFF_TEMPLATE)

    def check(self, *, inject, stop, **sample):
        events = self.observe(**sample)
        if self.state['mode'] != 'enforce':
            return events
        for event in events:
            # Persist intent before external mutations. A lost response must not
            # cause duplicate injections on the next watcher read.
            if event['action'] == 'stop':
                try:
                    validate_handoff(self.handoff)
                    self.state['handoff_valid'] = True
                except (OSError, ValueError) as exc:
                    self.state.update(handoff_valid=False, handoff_error=str(exc))
                    self.log({**event, 'action': 'handoff_rejected', 'message': str(exc)})
                stop()
                self.state['stop_confirmed'] = True
            else:
                inject(self.instruction())
        write_json(self.path, self.state)
        return events

    def sample_log(self, log, provider, **kw):
        usage = read_usage(log, self.state.get('usage', {}), provider)
        self.state['usage'] = usage
        return dict(rounds=usage['rounds'], tokens=usage['tokens'],
                    context_fraction=usage.get('context_fraction', 0), **kw)


def add_arguments(parser):
    parser.add_argument('--guard-run', type=Path, help='DAG run root; defaults to $HOST_DAG_RUN')
    parser.add_argument('--guard-ticket', help='executor ticket id (never bind the coordinator)')
    parser.add_argument('--token-guard', choices=['dry-run', 'enforce'], default='dry-run')
    parser.add_argument('--guard-evidence', type=Path, help='ticket evidence directory')
    parser.add_argument('--guard-handoff', type=Path, help='validated handoff for a fresh retry session')
    parser.add_argument('--guard-retry-of', help='stopped predecessor identity; never resumes it')
    parser.add_argument('--guard-phase', choices=['implement', 'deploy', 'acceptance'])


def prepare(args, contract, *, run=None, ticket=None):
    run = getattr(args, 'guard_run', None) or run or os.environ.get('HOST_DAG_RUN')
    ticket = getattr(args, 'guard_ticket', None) or ticket
    # Standard executor dispatches carry an absolute run evidence/control path
    # and a ticket heading. Discover only those, never scan global board state.
    if not ticket:
        match = re.search(r'(?im)^\s*(?:#+\s*|ticket\s*[:：]\s*)([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)+)\b', contract)
        ticket = match[1] if match else None
    if not run and ticket:
        for candidate in re.findall(r'(/[^\s`"<>（）()]+)/(?:control|evidence)/', contract):
            if (Path(candidate) / 'run.json').is_file():
                run = candidate
                break
    mode = getattr(args, 'token_guard', 'dry-run')
    handoff = getattr(args, 'guard_handoff', None)
    predecessor = getattr(args, 'guard_retry_of', None)
    continuation = {}
    if predecessor:
        with locked(predecessor) as old:
            if old is None or not old.blocked or not old.state.get('stop_confirmed'):
                raise ValueError('Fresh retry requires a confirmed stopped predecessor')
            if not handoff or public_path(handoff) != public_path(old.handoff):
                raise ValueError('Fresh retry requires the predecessor handoff')
            if old.state.get('successor'):
                raise ValueError('Predecessor already has a successor')
            if not run or public_path(run) != public_path(old.state['run']) or ticket != old.state['ticket']:
                raise ValueError('Fresh retry must preserve the predecessor run and ticket')
            continuation = dict(handoff_number=old.state.get('handoff_number', 1) + 1,
                                retry_count=old.state['retry_count'], predecessor=predecessor)
    if handoff:
        contract += '\n\n' + validate_handoff(handoff)
    if not run or not ticket:
        if mode == 'enforce' or handoff or predecessor:
            raise ValueError('Token guard requires --guard-run and --guard-ticket')
        return None, contract
    rec = {}
    receipt = Path(run) / 'run.json'
    if receipt.exists():
        matches = [b.get('tickets', {}).get(ticket) for b in json.loads(receipt.read_text()).get('batches', {}).values()]
        matches = [t for t in matches if t]
        rec = matches[-1] if matches else {}
    return dict(run=str(run), ticket=ticket, mode=mode,
                evidence=str(getattr(args, 'guard_evidence', None) or Path(run) / 'evidence' / ticket),
                ticket_record=rec, phase=getattr(args, 'guard_phase', None), **continuation), contract


def refuse_resume(identity, *, retry=False):
    with locked(identity) as g:
        if g and g.state['mode'] == 'enforce' and (g.blocked or retry):
            if retry:
                g.failed()
            raise ValueError('TOKEN_GUARD_NEW_SESSION_REQUIRED: use a fresh start with --guard-retry-of '
                             + identity + ' --guard-handoff ' + str(g.handoff))


def pending_instruction(identity, message):
    with locked(identity) as g:
        if g and g.state['mode'] == 'enforce':
            pending = g.state.pop('pending_instruction', '')
            if pending:
                return message + '\n\n' + pending
    return message


def before_send(identity, log, provider, status, message):
    """Close the read/send sampling gap; do not wake an exhausted session."""
    refuse_resume(identity, retry=status in {'failed', 'cancelled'})
    with locked(identity) as g:
        if g:
            def defer(text):
                g.state['pending_instruction'] = text
            sample = g.sample_log(log, provider, active=True)
            sample['waiting'] = wait_reason(g.state)
            g.check(inject=defer, stop=lambda: None, **sample)
    refuse_resume(identity)
    return pending_instruction(identity, message)


def set_phase(identity, phase, context_fraction, *, terminal=False):
    if phase not in {'implement', 'deploy', 'acceptance'} or not 0 <= context_fraction <= 10:
        raise ValueError('Invalid guard phase or context fraction')
    with locked(identity) as g:
        if g is None:
            raise ValueError('Executor is not bound to a token guard')
        write_json(g.path.with_suffix('.phase.json'), dict(phase=phase, context_fraction=context_fraction))
        if terminal:
            g.check(inject=lambda text: g.state.update(pending_instruction=text), stop=lambda: None,
                    active=False, phase=phase, context_fraction=context_fraction,
                    rounds=g.state.get('rounds', 0), tokens=g.state.get('tokens', 0))
        elif phase != 'acceptance':
            # Preserve a declared deploy edge even if acceptance is declared
            # before the supervisor's next five-second sample.
            g.state['phase'] = phase
        return {'newSessionRequired': g.blocked, 'handoff': str(g.handoff)}


def supervise(argv, cwd, prompt, log, stderr, identity, *, stream_input=False):
    """Own one CLI child, inject via Claude's documented stream-json input.

    Called only for bound executors. Keeping stdin open permits steering; close
    it after a result so a normal completed print session can exit. No CLI resume
    is performed here; the coordinator owns all fresh-session retries.
    """
    log, stderr = Path(log), Path(stderr)
    with log.open('ab') as out, stderr.open('ab') as err:
        child = subprocess.Popen(argv, cwd=cwd, stdin=subprocess.PIPE, stdout=out, stderr=err)
        def send(message):
            payload = {'type': 'user', 'message': {'role': 'user', 'content': message}}
            child.stdin.write((json.dumps(payload) + '\n').encode())
            child.stdin.flush()
        def stop():
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
        def interrupted(_signum, _frame):
            stop()
            raise InterruptedError('executor supervisor cancelled')
        prior = {s: signal.signal(s, interrupted) for s in (signal.SIGINT, signal.SIGTERM)}
        try:
            if stream_input:
                send(prompt)
            else:
                child.stdin.write(prompt.encode())
                child.stdin.close()
            while True:
                try:
                    code = child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    code = None
                with locked(identity) as g:
                    sample = g.sample_log(log, 'claude', active=code is None)
                    sample['waiting'] = wait_reason(g.state, provider='claude') if code is None else None
                    control = g.path.with_suffix('.phase.json')
                    if control.exists():
                        phase = json.loads(control.read_text())
                        sample.update(phase=phase.get('phase'),
                                      context_fraction=phase.get('context_fraction', sample['context_fraction']))
                    g.check(inject=send, stop=stop, **sample)
                    if g.blocked:
                        if not g.state.get('stop_confirmed'):
                            stop()
                            g.state['stop_confirmed'] = True
                        return child.returncode
                    if stream_input and g.state.get('usage', {}).get('result') and not child.stdin.closed:
                        child.stdin.close()
                if code is not None:
                    return code
        finally:
            for sig, handler in prior.items():
                signal.signal(sig, handler)
            if child.poll() is None:
                stop()
            if child.stdin and not child.stdin.closed:
                child.stdin.close()


def worker(spec_path):
    """Claude-host detached turn wrapper; preserves its existing result JSON ABI."""
    spec = json.loads(Path(spec_path).read_text())
    code = supervise(spec['argv'], spec['cwd'], Path(spec['prompt']).read_text(), spec['log'],
                     spec['stderr'], spec['identity'], stream_input=spec['stream_input'])
    result = {}
    with locked(spec['identity']) as g:
        result = g.state.get('usage', {}).get('result') or {}
        if g.blocked:
            result = {'type': 'result', 'is_error': True, 'result': 'TOKEN_GUARD_NEW_SESSION_REQUIRED: ' + g.instruction()}
    if not result:
        result = {'type': 'result', 'is_error': True, 'result': f'CLI ended without result (exit {code})'}
    print(json.dumps(result, ensure_ascii=False))
    return code


if __name__ == '__main__':
    raise SystemExit(worker(sys.argv[1]))

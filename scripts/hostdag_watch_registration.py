"""Durable observer registrations. Only the watch state lock holder applies them."""
from __future__ import annotations

from hostdag_json import atomic_json
import json
import os
import re
import time
import uuid
from pathlib import Path
from hostdag_json import read_json
from hostdag_cli_watch import CLI_PROVIDERS


def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_json(path, value, durable=True)


def inbox(path):
    return path.with_name(path.name + '.registrations')


def enqueue(path, request):
    key = f'{time.time_ns():020d}-{uuid.uuid4().hex}'
    target = inbox(path) / (key + '.json')
    atomic(target, {**request, 'id': key})
    return target


def receipt(path):
    ack = path.with_suffix('.ack')
    return json.loads(ack.read_text()) if ack.exists() else {'status': 'queued'}


def identity(rec):
    if rec.get('provider') == 'grok':
        return ['grok', rec.get('session'), rec.get('log'), rec.get('wrapper_pid')]
    if rec.get('provider') in CLI_PROVIDERS:
        return [rec['provider'], rec.get('run_file')]
    return ['codex', rec.get('thread')]


def register(state, name, rec, now):
    tracked = state.setdefault('tracked', {})
    old = tracked.get(name) or {}
    if identity(old) == identity(rec):
        old.update(rec)  # Preserve parser, fingerprint and terminal state on duplicate registration.
    else:
        old = {**rec, 'terminal': False, 'direction_anchor': now}
    tracked[name] = old


def drain(state, path, save, allowed_tickets=None):
    """Commit state before ack; replay after a crash cannot re-arm completed work."""
    pause_path = path.parent / 'control' / 'credit-pause.json'
    if allowed_tickets is None and pause_path.exists():
        pause = json.loads(pause_path.read_text())
        if pause.get('active'):
            allowed_tickets = set(pause.get('priority_tickets') or [])
    done = state.setdefault('registration_applied', {})
    for file in sorted(inbox(path).glob('*.json')):
        try:
            req = json.loads(file.read_text())
            if allowed_tickets is not None and req.get('ticket') not in allowed_tickets:
                continue  # preserve paused tickets and their pending registrations
            if req.get('op') not in ('track', 'track-grok', 'track-cli', 'untrack'):
                raise ValueError('unknown registration operation')
            if not req.get('id') or not req.get('ticket'):
                raise ValueError('registration lacks id/ticket')
            if req['op'] != 'untrack' and not isinstance(req.get('record'), dict):
                raise ValueError('registration lacks record')
        except (ValueError, AttributeError) as exc:
            atomic(file.with_suffix('.ack'), {'status': 'rejected', 'error': str(exc)})
            file.rename(file.with_suffix('.rejected'))
            continue
        name, key = req['ticket'], req['id']
        if key > done.get(name, ''):
            if req['op'] == 'untrack':
                state.setdefault('tracked', {}).pop(name, None)
                state.setdefault('registration_suppressed', {})[name] = True
            else:
                register(state, name, req['record'], req['at'])
                state.setdefault('registration_suppressed', {}).pop(name, None)
            done[name] = key
            save(path, state)
        atomic(file.with_suffix('.ack'), {'status': 'applied', 'id': key, 'ticket': name})
        file.unlink()


def read_object(path):
    if not Path(path).exists():
        return {}
    try:
        value = read_json(path)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


UUID = re.compile(r'^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$')


def claude_thread(value):
    return isinstance(value, str) and value.startswith('claude-') and bool(UUID.fullmatch(value[7:]))


def resolve(executor, evidence):
    """Only follow the ticket's exact receipt, never scan historical evidence trees."""
    if isinstance(executor, str) and (UUID.fullmatch(executor) or claude_thread(executor)):
        return {'thread': executor, 'evidence': evidence}
    if evidence and Path(evidence).is_dir() and (Path(evidence) / 'cli-run.json').is_file():
        # 2026-10-23 V20-F2: the coordinator registered the round directory instead of its cli-run.json.
        evidence = str(Path(evidence) / 'cli-run.json')
    d = read_object(evidence) if evidence else {}
    if d.get('log') and d.get('provider') == 'grok':
        session, pid = d.get('sessionId'), d.get('wrapper_pid')
        if not session and d.get('stdout'):
            try:
                with Path(d['stdout']).open('rb') as f:
                    f.seek(0, 2)
                    f.seek(max(0, f.tell() - 65536))
                    tail = f.read().decode('utf-8', errors='replace')
                for line in tail.splitlines():
                    if line.startswith('SESSION '):
                        session = line.split()[1]
                    if line.startswith('WRAPPER_PID '):
                        pid = int(line.split()[1])
            except (OSError, ValueError):
                pass
        if session:
            return {'provider': 'grok', 'session': session, 'log': d['log'],
                    'wrapper_pid': pid, 'evidence': evidence}
    run_file = d.get('run_file') or d.get('runFile')
    if not run_file and d.get('unit') and d.get('log'):
        # The evidence is host-dag-cli-agent.py's own cli-run.json (what dispatch auto-registration writes).
        run_file = evidence
    if d.get('provider') in CLI_PROVIDERS and run_file:
        return {'provider': d['provider'], 'run_file': run_file, 'evidence': evidence}
    return None


def reconcile(state, data, now):
    """Recover forgotten registration from authoritative batch identities, without an LLM."""
    seen = state.setdefault('registration_sources', {})
    issues = {}
    for batch, body in (data.get('batches') or {}).items():
        for name, ticket in (body.get('tickets') or {}).items():
            if ticket.get('state') in ('ACCEPTED', 'INTEGRATED', 'CANCELED') or ticket.get('status') in ('ACCEPTED', 'SUPERSEDED'):
                for key in list(state.get('tracked', {})):
                    if key == name or key.startswith(name + '-review-'):
                        state['tracked'].pop(key, None)
                continue
            refs = {name: (ticket.get('executor'), ticket.get('evidence') or '')}
            # 2026-10-22: V18-BATCH-ACCEPTANCE was claimed with a new executor but no state, so its
            # finished run went unobserved for 30 minutes. Claimed-with-executor and no state is running.
            active = ticket.get('state') in ('RUNNING', 'REVIEWING') or (
                not ticket.get('state') and ticket.get('status') in ('CLAIMED', 'RUNNING'))
            if not active:
                continue  # A deliberate wait must not replay a historical executor's result.
            for axis, review in (ticket.get('reviews') or {}).items():
                if not review.get('verdict'):
                    refs[name + '-review-' + axis] = (review.get('thread'), review.get('evidence') or '')
            evidence = ticket.get('evidence') or ''
            if ticket.get('state') == 'REVIEWING' and 'review' in Path(evidence).name:
                dispatch = read_object(evidence)
                thread = dispatch.get('threadId')
                if thread and thread != ticket.get('executor'):
                    refs[name + '-review-incremental'] = (thread, evidence)
            for key, (executor, evidence) in refs.items():
                if not executor:
                    continue
                source = [executor, evidence]
                rec = resolve(executor, evidence)
                previous = seen.get(key)
                if previous is not None and previous != source:
                    state.setdefault('registration_suppressed', {}).pop(key, None)
                if state.get('registration_suppressed', {}).get(key):
                    continue
                if rec is None:
                    # An explicit track-cli/track-grok registration may already cover it.
                    if key not in state.get('tracked', {}):
                        issues[key] = {'executor': executor, 'evidence': evidence,
                                       'reason': 'receipt lacks observable session/log or run_file'}
                    continue
                old = state.get('tracked', {}).get(key)
                stale_terminal = old and old.get('terminal') and identity(old) != identity(rec)
                if old is None or stale_terminal or (previous is not None and previous != source):
                    register(state, key, rec, now)
                elif identity(old) != identity(rec):
                    issues[key] = {'executor': executor, 'evidence': evidence,
                                   'reason': 'active observation differs from batch identity'}
                seen[key] = source
    state['registration_gaps'] = issues

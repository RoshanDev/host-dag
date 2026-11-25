#!/usr/bin/env python3
"""Deterministic DAG patrol; inspect_run is read-only.

CLI executes only the two L1265 mechanical freeze actions via the board CLI.

--run accepts a directory or run.json. Only current ticket evidence is walked
(metadata only, no symlinks); JSON reads are capped. Times accept ISO or epoch.
Repeated tool errors require explicit chain_id/error_class/method and distinct
attempt ids; prose and aggregate error counts cannot prove an unchanged retry.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime
import json
import math
import os
import re
import subprocess
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hostdag_ticket_state as ts
import hostdag_dag_view as view
from hostdag_resource_locks import canonical_lock, validate_phase, lock_events, pid_start
from hostdag_json import read_json as read_json_retry

LARGE = 100 * 1024 ** 2
_LOCK_OBSERVATIONS = {}  # read-only patrol clocks; watcher persists its own clocks
_MISSING_DELIVERY_WARNED: set[str] = set()


def _warn_missing_delivery(module: str) -> None:
    if module not in _MISSING_DELIVERY_WARNED:
        print(f"WARNING: {module} unavailable; skipping release lint checks", file=sys.stderr)
        _MISSING_DELIVERY_WARNED.add(module)


def read_json(path):
    try:
        if path.stat().st_size >= LARGE:
            return {}
        value = read_json_retry(path)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def stamp(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        try:
            return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
        except (ValueError, TypeError):
            return None


def resolve(root, value):
    p = Path(str(value))
    return p if p.is_absolute() else root / p


def evidence_dir(root, ticket):
    if not ticket.get('evidence'):
        return None
    p = resolve(root, ticket['evidence'])
    if p.name == 'cli-run.json' and p.parent.name == 'cli':
        p = p.parent.parent
    elif p.suffix in ('.json', '.jsonl', '.log', '.md', '.txt', '.out'):
        p = p.parent
    # A ticket can retain a previous run pointer after a retry. Only consider
    # direct run-* siblings within that ticket, never unrelated evidence.
    parent = p.parent if p.name.startswith('run-') else p
    attempts = [x for x in parent.glob('run-*') if x.is_dir() and not x.is_symlink()]
    if attempts:
        p = max(attempts, key=lambda x: tuple((1, int(v)) if v.isdigit() else (0, v)
                                             for v in re.split(r'(\d+)', x.name)))
    return p


def mtime(p):
    try:
        return p.stat().st_mtime
    except OSError:
        return None


def failure_verdict(value):
    if isinstance(value, dict):
        return any(failure_verdict(v) for k, v in value.items()
                   if k in ('verdict', 'status', 'outcome', 'gates', 'spec', 'standards'))
    return str(value).strip().upper().replace('_', ' ').replace('-', ' ') in ('NOT PASS', 'FAIL', 'FAILED', 'CHANGES')


def phase_mismatch(phase, name, ticket=''):
    # Ticket identity matters: real r21 holder's phase omitted the remote host.
    try:
        validate_phase(phase, [name], ticket=ticket)
    except ValueError as exc:
        return str(exc)
    phase = phase.lower()
    if name == 'workstation-build' and not any(x in phase for x in ('build','npm','go-test','vet','jest','tsc','full-suite','full-test')):
        if any(x in phase for x in ('package','promotion','closure','install','accept','verify','gate','shot','browser','wait','read-only')):
            return 'non-build phase holds workstation-build'
    return None


def same_candidate(left, right):
    if left == right:
        return True
    # Boards often abbreviate Git SHAs. Non-SHA labels still require equality.
    return (isinstance(left, str) and isinstance(right, str) and min(len(left), len(right)) >= 7
            and all(c in '0123456789abcdef' for c in left + right)
            and (left.startswith(right) or right.startswith(left)))


def queued_units(blocker):
    if not re.search(r'排队|等锁|queued|queuing|queueing', blocker, re.I):
        return []
    units = re.findall(r'(?<![\w./-])([\w@:.\-]+\.service)\b', blocker)
    units += re.findall(r'(?:--unit[=\s]+|\bunit[=:\s]+|单元[=：:\s]*)([a-zA-Z0-9_@][\w@:.\-]*)', blocker)
    return sorted({u if u.endswith('.service') else u + '.service' for u in units})


def unit_details(unit):
    """Unknown/unavailable units are not proof of a failed queue."""
    try:
        result = subprocess.run(['systemctl', '--user', 'show', unit,
                                 '--property=LoadState', '--property=ActiveState',
                                 '--property=ExecMainStatus', '--property=ExecMainCode',
                                 '--property=InvocationID', '--property=ExecMainExitTimestampMonotonic'],
                                capture_output=True, text=True, timeout=3)
        props = dict(line.split('=', 1) for line in result.stdout.splitlines() if '=' in line)
        if result.returncode == 0 and props.get('LoadState') == 'loaded':
            exited = (props.get('ExecMainCode') not in (None, '', '0')
                      and props.get('ExecMainExitTimestampMonotonic') not in (None, '', '0'))
            return {'active_state': props.get('ActiveState'),
                    'exit_code': int(props['ExecMainStatus']) if exited and props.get('ExecMainStatus', '').isdigit() else None,
                    'main_code': props.get('ExecMainCode'), 'invocation': props.get('InvocationID'),
                    'exited_at': props.get('ExecMainExitTimestampMonotonic')}
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def unit_state(unit):
    details = unit_details(unit)
    return details.get('active_state') if details else None


def running_heartbeat(result, now, unit_states):
    """None: ordinary progress rules; bool: a live bounded background phase."""
    if str(result.get('outcome', '')).lower() != 'running':
        return None
    runtime = result.get('runtime_unit')
    runtime = runtime if isinstance(runtime, dict) else {}
    unit = result.get('unit') or runtime.get('name')
    pid = result.get('pid') or runtime.get('pid')
    live = False
    units = unit if isinstance(unit, list) else [unit]
    for name in units:
        if not isinstance(name, str) or not name:
            continue
        if name not in unit_states:
            unit_states[name] = unit_state(name)
        if unit_states[name] in ('active', 'activating', 'reloading'):
            live = True
            break
    if isinstance(pid, int) and not isinstance(pid, bool) and pid > 0:
        start = pid_start(pid)
        recorded_start = result.get('pid_start') or runtime.get('pid_start')
        live = live or bool(start and (not recorded_start or recorded_start == start))
    if not live:
        return None
    deadlines = {**runtime, **result}
    deadline = next((stamp(deadlines[k]) for k in ('hard_deadline', 'deadline', 'deadline_at', 'segment_deadline')
                     if stamp(deadlines.get(k)) is not None), None)
    # A live unit/pid is progress. If a deadline is recorded, it is the only
    # stale-running threshold, including before the ordinary 30-minute timer.
    return deadline is None or now < deadline


def json_rows(path):
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return []
    rows = []
    for line in lines:
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                rows.append(row)
        except ValueError:
            continue
    return rows


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def incident_key(finding, tickets):
    # Progress timestamps and mutable blocker prose do not resolve an incident.
    e = finding.get('evidence') or {}
    subject = {k: e[k] for k in ('path', 'line', 'axis', 'wait_on', 'dependency', 'unit',
                               'key', 'id', 'chain_id', 'error_class', 'method') if k in e}
    if finding['rule'] == 'advisor-parent-review':
        subject = e  # each final/overriding advisor receipt must reach the parent
    if finding['rule'] == 'chain-result-latency':
        subject['result_mtime'] = e.get('result_mtime')
    if finding['rule'] == 'decision-not-applied':
        subject['decided_at'] = e.get('decided_at')
    t = tickets.get(finding['ticket']) or {}
    identity = {k: t.get(k) for k in ('attempt', 'round', 'candidate', 'executor')}
    return 'dag-lint-' + digest([finding['ticket'], finding['rule'], subject, identity])[:24]


def core_tickets(run):
    tickets = ts.all_tickets(run)
    core = {tid for tid, t in tickets.items() if t.get('chain_id', t.get('chain')) in
            ts.CORE_CHAINS}
    todo = [tid for tid in tickets if re.search(os.environ.get('HOST_DAG_RELEASE_TICKET_REGEX') or r'(?!x)x', tid)]
    seen = set()
    while todo:
        tid = todo.pop()
        if tid in seen:
            continue
        seen.add(tid)
        todo.extend(d for d in tickets.get(tid, {}).get('deps', []) if isinstance(d, str))
    return core | seen


def release_window(tickets, root=None):
    """Find the release owner through the frozen receipt, ledger or alias registry."""
    try:
        import release_version as rv
    except ImportError:
        _warn_missing_delivery("release_version")
        return None
    cfg = read_json(root / 'control/release-heads.json') if root is not None else {}
    explicit = cfg.get('release_ticket')
    if explicit in tickets:
        return explicit
    rows = rv.read_ledger(rv.ledger_path(root)) if root is not None else []
    void = {row['build'] for row in rows if row['status'] == 'void'}
    for row in reversed(rows):
        if (row['status'] == 'allocated' and row['build'] not in void
                and (not cfg.get('release') or rv.family(row['release_id']) == rv.family(cfg['release']))
                and row['ticket'] in tickets):
            return row['ticket']
    aliases = rv.load_legacy()['legacy_families']
    candidates = [tid for tid in tickets if tid in aliases and 'RELEASE' in tid
                  and (not cfg.get('release') or aliases[tid] == rv.family(cfg['release']))]
    return candidates[-1] if candidates else None


def release_chain_tickets(run, root=None):
    """Release owner, its dependencies and explicitly tagged current-line tickets."""
    try:
        import release_version as rv
    except ImportError:
        _warn_missing_delivery("release_version")
        return set()
    tickets = ts.all_tickets(run)
    owner = release_window(tickets, root)
    cfg = read_json(root / 'control/release-heads.json') if root is not None else {}
    tags = {str(x) for x in (cfg.get('release'), rv.family(cfg.get('release')), cfg.get('chain_id')) if x}
    if owner:
        tags.add(owner)
        tags.update(owner.split('-RELEASE-')[1:])
    todo = ([owner] if owner else []) + [
        tid for tid, t in tickets.items() if str(t.get('chain_id') or t.get('chain') or '') in tags
        or any(re.search(r'(?:^|[-_])' + re.escape(tag) + r'(?:$|[-_])',
                         tid + ' ' + str(t.get('chain_id') or t.get('chain') or ''), re.I)
               for tag in tags)]
    seen = set()
    while todo:
        tid = todo.pop()
        if tid in seen:
            continue
        seen.add(tid)
        todo.extend(d for d in tickets.get(tid, {}).get('deps', []) if isinstance(d, str))
    return seen


# Kept for read-only metrics callers; selection itself is version-driven.
release_tickets = release_chain_tickets


def chain_result_artifacts(root, ticket):
    """Current evidence only, including separately bound review directories.

    Never scan old run siblings or build trees. Explicit attempt/round/candidate
    mismatches and in-progress result heartbeats are not completed results.
    """
    base = evidence_dir(root, ticket)
    paths = {base / 'result.json', base / 'verdict.json'} if base else set()
    if base:
        paths.update(base.glob('review*/verdict.json'))
    for review in (ticket.get('reviews') or {}).values():
        if isinstance(review, dict) and review.get('evidence'):
            path = resolve(root, review['evidence'])
            paths.add(path / 'verdict.json' if path.is_dir() else path)
    for path in sorted(paths):
        if path.name not in ('result.json', 'verdict.json') or path.is_symlink():
            continue
        at = mtime(path)
        payload = read_json(path)
        if at is None or not payload:
            continue
        if str(payload.get('outcome', payload.get('status', ''))).lower() == 'running':
            continue
        if any(payload.get(k) is not None and str(payload[k]) != str(ticket.get(k))
               for k in ('attempt', 'round')):
            continue
        if payload.get('candidate') and ticket.get('candidate') and not same_candidate(payload['candidate'], ticket['candidate']):
            continue
        yield path, at


def chain_result_latency_findings(root, run, now):
    tickets = ts.all_tickets(run)
    out = []
    for tid in sorted(release_chain_tickets(run, root)):
        ticket = tickets.get(tid, {})
        if ts.derive_state(ticket) in ts.DONE | {ts.CANCELED}:
            continue
        updated = stamp(ticket.get('updated_at'))
        if updated is None:
            continue
        for path, at in chain_result_artifacts(root, ticket):
            if updated < at < now - 300:
                out.append(dict(ticket=tid, rule='chain-result-latency', owner='parent',
                                evidence=dict(path=str(path), result_mtime=at, updated_at=updated,
                                              latency_seconds=now-at, escalate_after_seconds=900,
                                              automatic=False)))
    return out


def decided(root, key):
    # Exact key only. Provisional/L1 samples/needs_user are never release authority.
    row = view.advisor_answers(root).get(key)
    try:
        view.validate_record(row or {})
    except ValueError:
        return None
    return row if row.get('verdict') == 'decided' and view.is_final_answer(row) else None


def bound_verdict(root, ticket, tracked, tid):
    """Registration requires explicit evidence binding and current review identity.

    Do not infer identity from the ticket, a prose note, or a directory name.
    Every current attempt/round must be bound in the review or payload.
    """
    if ticket.get('round') is None and ticket.get('attempt') is None:
        return
    for axis, row in (ticket.get('reviews') or {}).items():
        if not isinstance(row, dict) or row.get('verdict') or axis not in view.REVIEW_AXES or not row.get('evidence'):
            continue
        path = resolve(root, row['evidence'])
        path = path / 'verdict.json' if path.is_dir() else path
        payload = read_json(path)
        verdict = str(payload.get('verdict', '')).upper()
        if verdict not in ('PASS', 'FAIL', 'CHANGES', 'NOT PASS'):
            continue
        candidate = payload.get('candidate') or row.get('candidate')
        if not candidate or not ticket.get('candidate') or not same_candidate(candidate, ticket['candidate']):
            continue
        if row.get('candidate') and not same_candidate(row['candidate'], candidate):
            continue
        if any(payload.get(k) and payload[k] != axis for k in ('axis', 'review_axis')):
            continue
        if any(ticket.get(k) is not None and str(payload.get(k, row.get(k))) != str(ticket[k])
               for k in ('attempt', 'round')):
            continue
        if any(payload.get(k) is not None and row.get(k) is not None and str(payload[k]) != str(row[k])
               for k in ('attempt', 'round')):
            continue
        thread = payload.get('thread') or payload.get('threadId')
        if thread and row.get('thread') != thread:
            continue
        observation = tracked.get(f'{tid}-review-{axis}') or {}
        if (row.get('thread') and observation.get('thread') == row['thread'] and
                observation.get('terminal') is False):
            continue
        # A newer verdict with a matching (or unknown) identity is ambiguous:
        # leave resolution to the parent, never register a known older result.
        base = evidence_dir(root, ticket)
        others = list(base.glob('review*/verdict.json')) + [base / 'verdict.json'] if base else []
        ambiguous = False
        for other in others:
            if other.resolve() == path.resolve() or (mtime(other) or 0) <= (mtime(path) or 0):
                continue
            other_payload = read_json(other)
            if str(other_payload.get('verdict', '')).upper() not in ('PASS', 'FAIL', 'CHANGES', 'NOT PASS'):
                continue
            if other_payload.get('candidate') and not same_candidate(other_payload['candidate'], candidate):
                continue
            if (other_payload.get('axis') or other_payload.get('review_axis')) not in (None, axis):
                continue
            if any(other_payload.get(k) is not None and str(other_payload[k]) != str(ticket.get(k))
                   for k in ('round', 'attempt')):
                continue
            ambiguous = True
        if not ambiguous:
            yield axis, ('PASS' if verdict == 'PASS' else 'FAIL'), path, payload


def deps_all_done(root, run, tid, now):
    """Prove terminal dependencies and a satisfied wait, without waiving deps.

    Require unique identities and timestamps: an old waiter is not proof that
    its last prerequisite has been terminal for ten minutes.
    """
    rows = {}
    for body in run.get('batches', {}).values():
        for name, ticket in body.get('tickets', {}).items():
            rows.setdefault(name, []).append(ticket)
    if len(rows.get(tid, [])) != 1:
        return None
    t = rows[tid][0]
    if ts.derive_state(t) not in (ts.WAITING_DEPENDENCY, ts.CREATED):
        return None
    deps = t.get('deps')
    if not isinstance(deps, list) or not deps or any(not isinstance(d, str) or d == tid for d in deps):
        return None
    proof = {'dependencies': {}}
    times = [stamp(t.get('updated_at'))]
    for dep in deps:
        if len(rows.get(dep, [])) != 1 or not isinstance(rows[dep][0], dict):
            return None
        other = rows[dep][0]
        if ts.derive_state(other) not in ts.TERMINAL:
            return None
        proof['dependencies'][dep] = other
        times.append(stamp(other.get('updated_at')))
    wait = str(t.get('wait_on') or '')
    if wait.startswith('ticket:'):
        dep = wait.split(':', 1)[1]
        if len(rows.get(dep, [])) != 1 or not isinstance(rows[dep][0], dict):
            return None
        other = rows[dep][0]
        if ts.derive_state(other) not in ts.DONE or t.get('wait_holder') not in (None, '', dep):
            return None
        proof['wait_ticket'] = other
        times.append(stamp(other.get('updated_at')))
    elif wait.startswith('decision:'):
        answer = decided(root, wait.split(':', 1)[1])
        if not answer:
            return None
        proof['answer'] = answer
        times.append(stamp(answer.get('at')))
    elif wait or t.get('wait_holder'):
        return None
    if any(at is None for at in times) or now - max(times) <= 600:
        return None
    proof['ready_since'] = max(times)
    return proof


def auto_plan(root, run, batch, tid, rule):
    """Read-only narrow action proposal; CLI recomputes this under run.json.lock."""
    matches = [(b, t) for b, body in run.get('batches', {}).items()
               for name, t in body.get('tickets', {}).items() if name == tid]
    if len(matches) != 1 or matches[0][0] != batch:
        return None
    t = matches[0][1]
    state = ts.derive_state(t, ts.all_tickets(run))
    if rule == 'freeze-gate-clear':
        proof = view.freeze_gate.freeze_install_receipt(root)
        if not proof:
            return None
        plan = dict(rule=rule, ticket=tid, batch=batch, fields={}, proof=proof, pending_action=None)
        plan['revision'] = digest([t, plan])
        return plan
    if state in ts.TERMINAL | {ts.FROZEN_GATE} or t.get('page_request'):
        return None
    if rule=='review-cap-degrade-due':
        from hostdag_lint_actions import degrade_proposal
        proof=degrade_proposal(root,run,tid,t)
        if not proof:return None
        plan=dict(rule=rule,ticket=tid,batch=batch,fields={},proof=proof,pending_action=None)
        plan['revision']=digest([t,plan]);return plan
    if state in ts.DEGRADE_STATES:return None
    fields, proof, pending = {}, {}, None
    if rule == 'freeze-gate-stale-exempt':
        proof = view.freeze_gate.stale_exempt(root, t)
        if not proof or state == ts.FROZEN_GATE:
            return None
        fields = {'state': ts.FROZEN_GATE, 'freeze_reason': 'stale_exempt',
                  'blocker': 'reason=stale_exempt; blocked_step already PASS'}
    elif rule == 'deps-all-done':
        proof = deps_all_done(root, run, tid, time.time())
        if (not proof or 'answer' in proof or
                any(ts.derive_state(d) not in ts.DONE for d in proof['dependencies'].values()) or
                (state == ts.CREATED and not t.get('wait_on') and t.get('blocker') == '前置已全部完成')):
            return None
        fields = {'state': ts.CREATED, 'wait_on': '', 'wait_holder': '',
                  'blocker': '前置已全部完成'}
    elif rule == 'verdict-not-registered':
        if state != ts.REVIEWING:
            return None
        tracked = read_json(root / 'watch.json').get('tracked') or {}
        for axis, verdict, path, payload in bound_verdict(root, t, tracked, tid):
            fields = {'reviews': {axis: {'verdict': verdict}}, 'state': ts.REVIEWING,
                      'blocker': f'待父处理：{axis} 审查 {verdict} 已登记；请父处理审查结果'}
            proof = {'axis': axis, 'path': str(path), 'payload': payload}
            pending = {'kind': 'review_returned', 'axis': axis, 'verdict': verdict}
            break
    elif rule == 'wait-on-decided':
        wait = str(t.get('wait_on') or '')
        if state not in ts.WAITING or not wait.startswith('decision:'):
            return None
        key = wait.split(':', 1)[1]
        answer = decided(root, key)
        if not answer:
            return None
        fields = {'state': ts.WAITING_DEPENDENCY if ts.dependency_status(t, ts.all_tickets(run))[0] else ts.CREATED,
                  'wait_on': '', 'wait_holder': '',
                  'blocker': '待父执行裁定：' + str(answer.get('decision') or answer.get('summary') or '')[:2000]}
        proof = {'key': key, 'answer': answer}
        pending = {'kind': 'apply_decision', 'key': key, 'decision_at': answer['at']}
    elif rule == 'queued-but-unit-failed':
        blocker = str(t.get('blocker') or '')
        if '排队单元已退出(' in blocker:
            return None
        units = queued_units(blocker if state != ts.WAITING_RESOURCE else 'queued ' + blocker)
        for unit in units:
            details = unit_details(unit)
            if details and details.get('active_state') in ('failed', 'inactive') and details.get('exit_code') is not None:
                fields = {'blocker': f'排队单元已退出({details["exit_code"]})，需重新排队；unit={unit}；原记录：{blocker}'}
                proof = {'unit': unit, **details}
                break
    if not fields:
        return None
    plan = dict(rule=rule, ticket=tid, batch=batch, fields=fields, proof=proof, pending_action=pending)
    plan['revision'] = digest([t, plan])
    return plan


def decision_findings(root, run, findings, now):
    tickets = ts.all_tickets(run)
    current = {incident_key(f, tickets): f for f in findings}
    pairs = {}
    active = {}
    for row in json_rows(root / 'control/nudge-ledger.jsonl'):
        if row.get('type') == 'lint-cleared':
            active.pop(row.get('incident'), None)
        elif row.get('type') == 'lint-escalated':
            active[row.get('incident')] = row
    for ident, row in active.items():
        if ident in current:
            pairs[(row['ticket'], row['key'])] = row
    for tid, t in tickets.items():
        if ts.derive_state(t) in ts.TERMINAL:
            continue
        action = t.get('pending_action') or {}
        key = action.get('key') if action.get('kind') == 'apply_decision' else None
        if str(t.get('wait_on', '')).startswith('decision:'):
            key = t['wait_on'].split(':', 1)[1]
        if key:
            pairs[(tid, key)] = {}
    out = []
    for (tid, key), row in pairs.items():
        answer = decided(root, key)
        if answer and now - answer['at'] >= 900:
            out.append({'ticket': tid, 'rule': 'decision-not-applied', 'owner': 'parent',
                        'evidence': {'key': key, 'decided_at': answer['at'],
                                     'decision': answer.get('decision') or answer.get('summary'),
                                     'incident': row.get('incident')}})
    return out


def review_failures(root, base, tid, ticket, tracked, updated, now):
    """Only proven candidate/axis identities may supersede another file.

    An explicit evidence binding can supply its own review candidate, after
    checking it against the ticket. The ticket's candidate and a sole declared
    axis are never used to fill missing verdict identity. Unknowns stay per-file.
    """
    reviews = {axis: row for axis, row in (ticket.get('reviews') or {}).items() if isinstance(row, dict)}
    paths = {p.resolve(): set() for p in base.glob('review*/verdict.json')} if base else {}
    if base:
        paths[(base / 'verdict.json').resolve()] = set()
    for axis, row in reviews.items():
        if row.get('evidence'):
            p = resolve(root, row['evidence'])
            paths.setdefault((p / 'verdict.json' if p.is_dir() else p).resolve(), set()).add(axis)
    running = set()
    for axis, row in reviews.items():
        if row.get('candidate') and ticket.get('candidate') and not same_candidate(row['candidate'], ticket['candidate']):
            continue
        observed = tracked.get(f'{tid}-review-{axis}') or {}
        if (row.get('thread') and observed.get('thread') == row['thread']
                and observed.get('terminal') is False
                and str(observed.get('last_status', '')).lower() in ('running', 'inprogress', 'in_progress', 'busy')):
            running.add(axis)
    latest = {}
    for p, bindings in paths.items():
        at = mtime(p)
        if at is None:
            continue
        verdict = read_json(p)
        declared_axis = next(iter(bindings)) if len(bindings) == 1 else None
        bound_review = reviews.get(declared_axis, {})
        expected = ticket.get('candidate')
        bound_candidate = bound_review.get('candidate')
        # A stale explicit binding cannot become current just because its
        # payload omits candidate, or even contradicts that binding.
        if expected and bound_candidate and not same_candidate(expected, bound_candidate):
            continue
        candidate = verdict.get('candidate') or bound_candidate
        if expected and candidate and not same_candidate(expected, candidate):
            continue
        if bound_candidate and candidate and not same_candidate(bound_candidate, candidate):
            continue
        payload_axis = verdict.get('axis') or verdict.get('review_axis')
        axis = declared_axis or payload_axis
        if len(bindings) > 1 or (declared_axis and payload_axis and declared_axis != payload_axis):
            axis = None  # ambiguous/conflicting bindings cannot prove supersession
        row = reviews.get(axis, {})
        thread = verdict.get('thread') or verdict.get('threadId')
        if thread and row.get('thread') and thread != row['thread']:
            continue
        if axis in running or (not axis and running):
            continue
        key = ('file', str(p))
        if axis and candidate:
            # Canonicalize only after candidate equality has been proven.
            key = ('review', axis, expected or candidate)
            if not expected:
                key = next((k for k in latest if k[0] == 'review' and k[1] == axis
                            and same_candidate(k[2], candidate)), key)
        if key not in latest or (at, str(p)) > (latest[key][0], str(latest[key][1])):
            latest[key] = (at, p, verdict, axis, candidate)
    for at, p, verdict, axis, candidate in latest.values():
        if updated is not None and at > updated and now-at > 300 and failure_verdict(verdict):
            yield dict(path=str(p), mtime=at, updated_at=updated, axis=axis, candidate=candidate)


def legacy_lock_findings(paths):
    """Advisory cutover hints only; legacy callers remain valid exclusive users."""
    aliases = {}
    findings = []
    for path in paths:
        try:
            lines = Path(path).read_text().splitlines()
        except (OSError, UnicodeError):
            continue
        for number, line in enumerate(lines, 1):
            for old, new in aliases.items():
                if re.search(r'(?<![A-Za-z0-9_./-])' + re.escape(old) + r'(?![A-Za-z0-9_/-]|\.(?:lock|holder)(?![A-Za-z0-9_-]))', line):
                    findings.append({'ticket': '<cutover>', 'rule': 'legacy-lock-name',
                                     'owner': 'parent', 'severity': 'info', 'blocking': False,
                                     'evidence': {'path': str(path), 'line': number,
                                                  'old': old, 'replacement': new}})
    return findings


HOT_LINK_RULE = 'hot-path-link-replaced'


def hot_link_findings(root, hot_root=None):
    """Segment-one manifest `linked` entries must still be public symlinks to their SSD copy.

    Fable L2174 §1c / TOOL-HOTPATH-SYMLINK-KEEP. lstat/readlink only: never relinks,
    never edits the manifest; `detached` entries are skipped. Fail-open per manifest.
    """
    hot_root = Path(hot_root or os.environ.get('HOST_DAG_HOT_RUNS_ROOT') or Path.home() / '.local/state/host-dag-hot-runs')
    findings = []
    try:
        root_real = Path(root).resolve()
        manifests = sorted(hot_root.glob('*/*/*-migration.json'))
    except OSError:
        return findings
    for manifest in manifests:
        try:
            data = json.loads(manifest.read_text())
            if not isinstance(data, dict) or data.get('phase') != 'active':
                continue
            if Path(str(data.get('run') or '/nonexistent')).resolve() != root_real:
                continue
            ssd = Path(str(data.get('ssd') or manifest.parent))
            for entry in data.get('entries') or []:
                if not isinstance(entry, dict) or entry.get('phase') != 'linked':
                    continue  # detached / pending / rolled-back are not expected to be links
                rel = str(entry.get('path') or '')
                public, expected = Path(root) / rel, ssd / rel
                if public.is_symlink():
                    if Path(os.path.realpath(public)) == Path(os.path.realpath(expected)):
                        continue
                    actual = 'symlink-elsewhere'
                elif public.is_file():
                    actual = 'regular-file'
                elif os.path.lexists(public):
                    actual = 'not-a-file'
                else:
                    actual = 'missing'
                findings.append({'ticket': '<hot-paths>', 'rule': HOT_LINK_RULE, 'owner': 'parent',
                                 'severity': 'warning', 'blocking': False,
                                 'evidence': {'path': rel, 'actual': actual, 'expected_target': str(expected),
                                              'manifest': str(manifest), 'automatic': False,
                                              'reason': '清单 linked 项已不是指向 SSD 副本的软链；只报不修。'
                                                        '确认公共文件为唯一权威后，用 hostdag_hot_paths_seg1.py detach 改标，勿 rollback/migrate'}})
        except (OSError, ValueError, TypeError):
            continue
    return findings


def leftover_findings(rows, tickets, now):
    """Read-only policy checks over append order; no closure inference."""
    import hostdag_leftover_ledger as ledger
    current, opened = {}, {}
    for line, row in enumerate(rows, 1):
        ident = row.get('id')
        if not ident:
            continue
        if row.get('status') == 'OPEN':
            opened.setdefault(ident, ledger.stamp(row.get('at')))
        else:
            opened.pop(ident, None)
        current[ident] = (line, row)
    findings = []

    def add(rule, row=None, **evidence):
        findings.append(dict(ticket=(row or {}).get('scheduled_ticket') or (row or {}).get('id') or '<leftovers>',
                             rule=rule, owner='parent', evidence=evidence))

    ticket_refs = re.compile(r'(?<![A-Z0-9-])(?:' + '|'.join(re.escape(tid) for tid in sorted(tickets)) + r')(?![A-Z0-9-])') if tickets else None
    for ident, (line, row) in current.items():
        at = opened.get(ident)
        if (row.get('severity') == 'P1' and row.get('status') == 'OPEN'
                and row.get('kind') != 'roadmap'
                and at is not None and now - at > 86400):
            add('leftover-p1-stale', row, id=ident, line=line, opened_at=at)
        if row.get('status') in ('OPEN', 'SCHEDULED'):
            # next may contain a prose instruction: match known ticket IDs exactly.
            refs = {row.get('scheduled_ticket')}
            next_text = str(row.get('next') or '')
            if ticket_refs:
                refs.update(ticket_refs.findall(next_text))
            stale = sorted(tid for tid in refs if tid in tickets and
                           tickets[tid].get('state', tickets[tid].get('status')) in ('ACCEPTED', 'CANCELED', 'CANCELLED', 'CLOSED'))
            if stale:
                add('leftover-pointer-stale', row, id=ident, line=line, pointers=stale)
    count = sum(row.get('status') == 'OPEN' and row.get('kind') != 'roadmap'
                and not ledger.lifecycle_master(row) for _, row in current.values())
    if count > 60:
        add('leftover-open-high', count=count, threshold=60)
    # Three completed, nonoverlapping rolling 24h windows, newest first.
    flow_rows = [dict({'chain': '其他', 'severity': 'P2', 'status': 'OPEN'}, **r)
                 for r in rows if r.get('id')]
    windows = [ledger.summarize(flow_rows, now - day * 86400, 86400) for day in range(3)]
    if all(w['new'] > w['closed'] for w in windows):
        add('leftover-net-inflow', windows=[dict(end=now-day*86400, new=w['new'], closed=w['closed'])
                                          for day, w in enumerate(windows)])
    return findings


# --- idle_nonchain_slots (release-window D1: non-chain product slots must be filled) ---
# Tune these sets here; ids are matched with re.search on the ticket id.
NONCHAIN_SLOTS = 2                          # D1 cap = number of non-chain product slots
SIDE_ID_PATTERNS = (r'^TOOL-', r'^SUPERSEDED-TRIAGE$', r'^PROD-RESOURCE-SCOPE-MATRIX$',
                    r'^POST-[^-]+-ROADMAP')   # tool/doc side tickets: never count as a slot
SIDE_KINDS = frozenset({'tool', 'doc', 'docs', 'side', 'roadmap'})
CANDIDATE_ORDER: tuple[str, ...] = ()
EXTRA_ID_PATTERN = r'(?!x)x'
# Only explicit lock names / need markers exclude a candidate; prose such as "do not deploy yet"
# or "storage component" must not. A match preceded by a negation word is ignored.
ENV_LOCK_PATTERN = re.compile(
    r'\bhost-\d{3}(?:-[\w.]+)?\b'
    r'|(?:needs?|requires?|需要|依赖)\s*[:：]?\s*(?:the\s+)?(?:an?\s+)?'
    r'(?:experiment[- ]env(?:ironment)?|host[- ]lock|arm[- ]?[34]\b|(?<!\d)9\.31(?!\d)|(?<!\d)9\.33(?!\d))'
    r'|board[-_ ]impact[^\n]{0,60}?(?:arm[- ]?[34]\b|(?<!\d)9\.3[13](?!\d)|host-\d{3})', re.I)
ENV_NEGATION = re.compile(r'(?:\bnot?\b|\bnever\b|\bwithout\b|不|无需|不需要|禁止)\s*$', re.I)
GATE_DEP_PATTERN = re.compile(r'^(?:[^-]+-KK-INSTALL-GATE-|[^-]+-ACCEPT-)')
STARTABLE_STATES = frozenset({ts.CREATED, ts.WAITING_DEPENDENCY, ts.WAITING_RESOURCE})


def _matches(patterns, tid):
    return any(re.search(p, tid) for p in patterns)


def needs_env(text):
    return any(not ENV_NEGATION.search(text[max(0, m.start() - 8):m.start()])
               for m in ENV_LOCK_PATTERN.finditer(text))


def idle_nonchain_slot_findings(tickets, root=None):
    """Read-only: report empty non-chain release-window slots and the next candidate.

    The rule-4 prefetch note (per waiting ticket) is only attached while a slot is free
    (free > 0) and only for CANDIDATE_ORDER plus configured tickets."""
    release_ticket = release_window(tickets, root)
    release = tickets.get(release_ticket)
    if not isinstance(release, dict) or ts.derive_state(release, tickets) in ts.DONE:
        return []
    states = {tid: ts.derive_state(t, tickets) for tid, t in tickets.items() if isinstance(t, dict)}

    def side(tid, t):
        return (_matches(SIDE_ID_PATTERNS, tid) or str(t.get('kind') or '').lower() in SIDE_KINDS
                or 'roadmap' in str(t.get('title') or '').lower())

    chain_ids = release_chain_tickets({'batches': {'lint': {'tickets': tickets}}}, root)

    def chain(tid):
        return tid in chain_ids

    busy = sorted(tid for tid, st in states.items() if st in (ts.RUNNING, ts.REVIEWING)
                  and not chain(tid) and not side(tid, tickets[tid]))
    free = NONCHAIN_SLOTS - len(busy)
    if free <= 0:
        return []

    def waits(t):
        wait = str(t.get('wait_on') or '')
        return set(t.get('deps') or []) | ({wait.split(':', 1)[1]} if wait.startswith('ticket:') else set())

    def startable(tid):
        t = tickets.get(tid)
        if not isinstance(t, dict) or states.get(tid) not in STARTABLE_STATES or side(tid, t) or chain(tid):
            return False
        if str(t.get('status') or '').upper() in ('REVIEWED_WAIT_INTEGRATE', 'ACCEPTED', 'CLOSED'):
            return False
        if str(t.get('blocker') or '').lstrip().upper().startswith('REVIEWED_WAIT_INTEGRATE'):
            return False  # real board shape: status CLAIMED, review passed, parked until V38 integrates
        if t.get('executor') or t.get('candidate'):
            return False  # already developed / in hand
        if needs_env(f"{t.get('scope') or ''} {t.get('blocker') or ''}"):
            return False  # needs an experiment environment or host lock
        return not any(GATE_DEP_PATTERN.search(d) for d in waits(t))

    ordered = [tid for tid in CANDIDATE_ORDER if tid in tickets]
    ordered += sorted(tid for tid in tickets if tid not in ordered and re.search(EXTRA_ID_PATTERN, tid))
    out = []
    nxt = next((tid for tid in ordered if startable(tid)), None)
    if nxt:
        out.append(dict(ticket='<release-window>', rule='idle_nonchain_slots', owner='parent',
                        evidence=dict(id=nxt, title=tickets[nxt].get('title'), idle_slots=free,
                                      slots=NONCHAIN_SLOTS, busy=busy, state=states[nxt],
                                      next_candidate=nxt)))
    for tid in ordered:
        t = tickets[tid]
        if (str(t.get('wait_on') or '') == 'ticket:' + release_ticket and startable(tid)
                and states[tid] in ts.WAITING and not t.get('executor') and not t.get('candidate')):
            out.append(dict(ticket=tid, rule='idle_nonchain_slots', owner='parent',
                            evidence=dict(id=tid, wait_on=t['wait_on'], idle_slots=free,
                                          note='可提前开发，发布票只是合入点')))
    return out


# --- accepted_not_in_release_head (accepted/integrated product commits must be in the release head) ---
# Read-only. Tune here; documented in docs/local-dev/dag-lint.md. Heads can be overridden per run by
# <run>/control/release-heads.json: {"GC": {"repo": "/path", "heads": ["ref-or-sha", ...]}, ...,
# "TOOL": {...}}; a repo that is missing, or any of whose release refs (non-HEAD) fails to resolve, is skipped whole
# (fail-open, one info diagnostic) so a deleted/renamed branch cannot flood findings.
DEV = Path(os.environ.get('HOST_DAG_DEV_ROOT', str(Path.home() / 'Developer')))
# Paths identify local object databases; product refs must come from the receipt.
RELEASE_REPOS = {
    'GC': (DEV / 'host-dag-container', ()),
    
    
    
}
TOOL_REPO = ('GC', (DEV / 'host-dag-container', ('HEAD',)))
REPO_BY_WORKTREE: dict[str, str] = {}
REPO_LABELS: dict[str, str] = {}
TOOL_TICKET_PATTERNS = (r'^(?:V\d+[A-Z]*-)?TOOL-', r'^LEFT-P1-HOOK-')
ANCESTRY_EXEMPT_PREFIX = 'REVIEWED_WAIT_INTEGRATE'
ANCESTRY_EXEMPT_TARGETS = frozenset({'r4', 'roadmap'})
GIT_TIMEOUT = 20
_SHA = re.compile(r'(?<![0-9A-Za-z_])(?:(?P<label>[A-Za-z][\w-]*)\s*[@: ]\s*)?(?P<sha>[0-9a-f]{7,40})(?![0-9A-Za-z_])')


def _git(repo, *args):
    """(returncode, stdout) or None on any failure/timeout: callers fail open."""
    try:
        p = subprocess.run(['git', '-C', str(repo), *args], capture_output=True, text=True, timeout=GIT_TIMEOUT)
    except (OSError, subprocess.SubprocessError):
        return None
    return p.returncode, p.stdout


def candidate_commits(ticket, default_repo, root):
    """[(repo_key, sha)] from ticket.candidate/commits and the evidence result.json (commits/candidate)."""
    try:
        from release_coverage import accepted_candidate, sources
    except ImportError:
        _warn_missing_delivery("release_coverage")
    else:
        if accepted_candidate(ticket.get('candidate')):
            return sources(ticket, heads=read_json(root / "control/release-heads.json") or {})  # explicit receipt
    texts = []  # legacy read-only compatibility

    def collect(value):
        if isinstance(value, str):
            texts.append(value)
        elif isinstance(value, (list, tuple)):
            for v in value:
                collect(v)
        elif isinstance(value, dict):
            for k in ('sha', 'commit', 'candidate'):
                collect(value.get(k))
            repo = value.get('repo')
            if isinstance(repo, str):  # {"repo": "FE", "sha": ...}: keep the label next to the sha
                texts.append(repo + ' ' + ' '.join(str(value.get(k)) for k in ('sha', 'commit') if isinstance(value.get(k), str)))
    collect(ticket.get('candidate'))
    collect(ticket.get('commits'))
    ev = ticket.get('evidence')
    if isinstance(ev, str) and ev:
        p = resolve(root, ev)
        result = p if p.name == 'result.json' else p / 'result.json'
        data = read_json(result) if result.is_file() else {}
        collect(data.get('commits'))
        if not ticket.get('candidate'):
            collect(data.get('candidate'))
    out, seen = [], set()
    for text in texts:
        for m in _SHA.finditer(text):
            sha, label = m.group('sha'), (m.group('label') or '').lower()
            if not re.search(r'[a-f]', sha):
                continue
            repo = REPO_LABELS.get(label) or default_repo
            if repo and (repo, sha) not in seen:
                seen.add((repo, sha))
                out.append((repo, sha))
    return out


def dispatch_ready(ticket, tickets, root, now):
    if any(dep not in tickets or ts.derive_state(tickets[dep]) not in ts.DONE
           for dep in ticket.get('deps') or []):
        return False
    wait = str(ticket.get('wait_on') or '')
    if wait.startswith('ticket:'):
        dep = tickets.get(wait.split(':', 1)[1])
        return isinstance(dep, dict) and ts.derive_state(dep) in ts.DONE
    if wait.startswith('decision:'):
        return bool(decided(root, wait.split(':', 1)[1]))
    if wait.startswith('heavy-slot:'):
        # The existing lock-wait-not-queued rule diagnoses missing queue proof.
        return False
    until = re.search(r'WAIT-UNTIL:(\S+)', str(ticket.get('blocker') or ''))
    if until and (stamp(until[1]) or 0) > now:
        return False
    return True


def current_release(root):
    """Explicit frozen line, then latest live allocation, then version family."""
    cfg = read_json(root / 'control/release-heads.json')
    if cfg.get('release'):
        return str(cfg['release'])
    try:
        import release_version as rv
    except ImportError:
        _warn_missing_delivery("release_version")
        return None
    rows = rv.read_ledger(rv.ledger_path(root))
    void = {row['build'] for row in rows if row['status'] == 'void'}
    allocated = [row for row in rows if row['status'] == 'allocated' and row['build'] not in void]
    return allocated[-1]['release_id'] if allocated else rv.load_version()['version']


def release_heads(root, tool=False):
    """Product heads come only from the delivery receipt; tools use their own head."""
    cfg = read_json(root / 'control/release-heads.json')
    if tool:
        tool_cfg = cfg.get('TOOL') if isinstance(cfg.get('TOOL'), dict) else {}
        table = {k: (Path(tool_cfg.get(k, {}).get('repo') or v[0]), [],
                     list(tool_cfg.get(k, {}).get('heads') or []) + ['HEAD'])
                 for k, v in RELEASE_REPOS.items()}
        try:
            from release_coverage import tool_head_spec
        except ImportError:
            _warn_missing_delivery("release_coverage")
            return table
        repo, refs = tool_head_spec(cfg)
        table['GC'] = (Path(repo), [], refs)
        return table
    vector = cfg.get('heads') if isinstance(cfg.get('heads'), dict) else {}
    table = {}
    for key, (default_repo, _) in RELEASE_REPOS.items():
        spec = cfg.get(key) if isinstance(cfg.get(key), dict) else {}
        head = vector.get(key) or spec.get('sha')
        refs = [str(head)] if head else list(spec.get('heads') or [])
        table[key] = (Path(spec.get('repo') or default_repo), refs, [])
    # The accepted two-line merge is the next delivery head, even before a new
    # package receipt replaces the frozen vector. RUNNING candidates never count.
    tickets = ts.all_tickets(read_json(root / 'run.json'))
    merge = tickets.get('LINE-INTEGRATE') or {}
    if ts.derive_state(merge) in ts.DONE:
        refs = [sha for key, sha in candidate_commits(merge, 'GC', root) if key == 'GC']
        if refs:
            repo, old, extra = table['GC']
            table['GC'] = (repo, old + refs, extra)
    return table


def _patch_ids(repo, args, timeout=GIT_TIMEOUT * 6):
    """{patch-id: commit} for `git log -p <args>`; None when unreadable."""
    try:
        log = subprocess.Popen(['git', '-C', str(repo), 'log', '-p', '--no-merges', '--format=commit %H', *args],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        out = subprocess.run(['git', 'patch-id', '--stable'], stdin=log.stdout, capture_output=True, text=True, timeout=timeout)
        log.stdout.close()
        log.wait(timeout=5)
    except (OSError, subprocess.SubprocessError):
        return None
    return {line.split()[0]: line.split()[1] for line in out.stdout.splitlines() if len(line.split()) == 2}


def _ancestry_eligible(t, root=None, tid=None, release=None):
    """ACCEPTED/INTEGRATED, not parked for a later integration (REVIEWED_WAIT_INTEGRATE / r4 / roadmap)."""
    if not isinstance(t, dict):
        return False
    status = {str(t.get('state') or '').upper(), str(t.get('status') or '').upper()}
    try:
        from release_coverage import LEGACY_UNTIL, disposition_valid
        import release_version as rv
    except ImportError as exc:
        _warn_missing_delivery(exc.name or "release_coverage")
        return False
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    release = release or (current_release(root) if root is not None else rv.load_version()['version'])
    family = rv.family(release)
    # Reuse the gate's signed disposition proof and numeric required_by semantics.
    # A family/legacy line has no build; b1 is only a scope adapter, never allocated.
    scope = rv.release_id(family, 1) if family and not rv.is_product_release(release) else release
    if disposition_valid(t.get('release_disposition'), scope, now, root, tid):
        return False
    # Legacy read-only bridge expires 2026-11-10; the packaging gate never trusts it.
    return bool(status & {'ACCEPTED', 'INTEGRATED'}) and not (now < LEGACY_UNTIL and (
        ANCESTRY_EXEMPT_PREFIX in status
        or str(t.get('blocker') or '').lstrip().upper().startswith(ANCESTRY_EXEMPT_PREFIX)
        or str(t.get('integration_target') or '').strip().lower() in ANCESTRY_EXEMPT_TARGETS))


def accepted_not_in_release_head_findings(tickets, root):
    """Pass 1: ancestry per commit. Pass 2: one patch-id sweep per head for the non-ancestors."""
    try:
        from release_coverage import evaluate, tool_kind_proof, tool_ticket, verify_kind_paths
        import release_version as _rv  # noqa: F401
    except ImportError as exc:
        _warn_missing_delivery(exc.name or "release_coverage")
        return []
    heads_cache, verdicts, jobs = {}, {}, []
    release = current_release(root)
    tables = {False: release_heads(root), True: release_heads(root, tool=True)}
    diagnostics = []
    # A signed supersession alone cannot suppress an obligation. Use the gate's
    # complete verdict, including target acceptance, ancestry and final coverage.
    superseded = {tid for tid, t in tickets.items()
                  if isinstance(t.get('release_disposition'), dict)
                  and t['release_disposition'].get('kind') == 'superseded'}
    if superseded:
        heads = read_json(root / 'control/release-heads.json') or {}
        report = evaluate(tickets, heads, release, run_dir=root)
        accepted = [tid for tid, t in sorted(tickets.items())
                    if str(t.get('state') or t.get('status') or '').upper() in ('ACCEPTED', 'INTEGRATED')]
        for tid, row in zip(accepted, report['rows']):
            if tid in superseded and row['classification'] == 'BLOCKED':
                diagnostics.append(dict(ticket=tid, rule='accepted_not_in_release_head', owner='parent',
                    evidence=dict(reasons=row['reasons'], classification=row['classification'])))
    invalid_kinds, verified_kinds = set(), set()
    for tid, t in sorted(tickets.items()):
        if tid in superseded:
            continue  # The shared coverage verdict includes tool/doc validation.
        if str(t.get('state') or t.get('status') or '').upper() not in ('ACCEPTED', 'INTEGRATED'):
            continue
        kind = str(t.get('kind') or '').lower()
        kind = 'doc' if kind == 'docs' else kind
        legacy = not kind and tool_ticket(tid, t)
        if legacy:
            kind = 'tool'  # legacy tool id: same live path proof as the gate and the board
        if kind not in ('tool', 'doc'):
            continue
        try:
            if kind == 'tool':
                tool_kind_proof(tid, t, root)
            else:
                verify_kind_paths(t, root, kind=kind, ticket_id=tid)
            verified_kinds.add(tid)
        except (ValueError, OSError, TypeError) as exc:
            invalid_kinds.add(tid)
            # Unclassified legacy ids are already BLOCKED by the coverage gate; surface them
            # without adding blocking work: only an explicit kind is a blocking mis-classification.
            diagnostics.append(dict(ticket=tid, rule='kind_touches_delivery', owner='parent',
                severity='info' if legacy else 'error', blocking=not legacy, evidence=dict(
                    reason='KIND_' + kind.upper() + '_TOUCHES_DELIVERY', detail=str(exc),
                    **({'legacy_tool_id': True} if legacy else {}))))

    def resolved(table, key):
        repo, refs, extra = table.get(key, (None, (), ()))
        if repo is None or not Path(repo).exists():
            return None, []

        def rev(ref):
            r = _git(repo, 'rev-parse', '-q', '--verify', ref + '^{commit}')
            return r[1].strip() if r and r[0] == 0 and r[1].strip() else None
        out = [rev(ref) for ref in refs]
        if None in out:  # a release ref is gone: whole repo fails open
            bad = [ref for ref, sha in zip(refs, out) if sha is None]
            diagnostics.append(dict(ticket='<release-heads>', rule='accepted_not_in_release_head_unresolvable',
                                    owner='parent', severity='info', blocking=False,
                                    evidence=dict(repo=key, path=str(repo), unresolved=bad or list(refs),
                                                  note='发布头不可解析，该仓本轮不判；更新 control/release-heads.json')))
            return None, []
        out += [sha for sha in map(rev, extra) if sha]
        return repo, out

    def ancestry(repo, sha, heads):
        """(full sha, True | False | None); None = commit/head unreadable (fail-open)."""
        full = _git(repo, 'rev-parse', '-q', '--verify', sha + '^{commit}')
        if not full or full[0] != 0 or not heads:
            return None, None
        full = full[1].strip()
        for head in heads:
            r = _git(repo, 'merge-base', '--is-ancestor', full, head)
            if r is None:
                return full, None
            if r[0] == 0:
                return full, True
        return full, False

    for tid, t in sorted(tickets.items()):
        if tid in superseded or tid in invalid_kinds or not _ancestry_eligible(t, root, tid, release):
            continue
        tool = _matches(TOOL_TICKET_PATTERNS, tid)  # only picks the default repo
        is_tool = tid in verified_kinds
        try:
            default_repo = TOOL_REPO[0] if tool else REPO_BY_WORKTREE.get(os.path.basename(str(t.get('worktree') or '').rstrip('/')))
            for key, sha in candidate_commits(t, default_repo, root):
                if (is_tool, key) not in heads_cache:
                    heads_cache[(is_tool, key)] = resolved(tables[is_tool], key)
                repo, heads = heads_cache[(is_tool, key)]
                if repo is None:
                    continue
                if ((is_tool, key), sha) not in verdicts:
                    full, verdict = ancestry(repo, sha, heads)
                    verdicts[((is_tool, key), sha)] = verdict
                    if verdict is False:
                        jobs.append(((is_tool, key), sha, full))
        except Exception:  # fail-open per ticket
            continue
    # pass 2: cherry-equivalence for commits that are not ancestors
    by_repo = {}
    for key, sha, full in jobs:
        by_repo.setdefault(key, []).append((sha, full))
    for key, items in by_repo.items():
        try:
            repo, heads = heads_cache[key]
            dates = []
            for _, full in items:
                r = _git(repo, 'show', '-s', '--format=%ct', full)
                if r and r[0] == 0 and r[1].strip().isdigit():
                    dates.append(int(r[1]))
            if not dates:
                continue
            since = ['--since=%d' % (min(dates) - 86400)]
            pool = {}
            for head in heads:
                ids = _patch_ids(repo, [*since, head])
                if ids is None:
                    pool = None
                    break
                pool.update(ids)
            if pool is None:
                for sha, _ in items:
                    verdicts[(key, sha)] = None  # sweep failed: cannot judge, fail open
                continue
            for sha, full in items:
                mine = _patch_ids(repo, ['-1', full])
                if mine is None or not mine:
                    verdicts[(key, sha)] = None
                elif any(pid in pool for pid in mine):
                    verdicts[(key, sha)] = True
        except Exception:
            for sha, _ in items:
                verdicts[(key, sha)] = None
    findings = []
    for tid, t in sorted(tickets.items()):
        if tid in superseded or tid in invalid_kinds or not _ancestry_eligible(t, root, tid, release):
            continue
        tool = _matches(TOOL_TICKET_PATTERNS, tid)  # only picks the default repo, never the verdict
        try:
            default_repo = TOOL_REPO[0] if tool else REPO_BY_WORKTREE.get(os.path.basename(str(t.get('worktree') or '').rstrip('/')))
            is_tool = tid in verified_kinds
            missing = [dict(repo=key, sha=sha) for key, sha in candidate_commits(t, default_repo, root)
                       if verdicts.get(((is_tool, key), sha)) is False]
        except Exception:
            continue
        if missing:
            findings.append(dict(ticket=tid, rule='accepted_not_in_release_head', owner='parent', evidence=dict(
                state=t.get('state') or t.get('status'), missing=missing,
                advice='纳入当前交付线；有意延期须提供有效 release_disposition')))
    return findings + diagnostics


POST_HOOK_ADVICE = ('不批量改状态，交协调者/顾问：交付票补 Fable candidate 行后 ticket set --candidate-decision-ref；'
                    '无代码票请 Fable 签 release_disposition；无法补证则 reopen 后经 ticket set 重新验收。'
                    '同时查是谁绕过 ticket set 写了 run.json（直接写文件、旧脚本、手工回滚）。')


def post_hook_acceptance_problem(root, tid, t, tickets=None):
    """A done row the ticket-set acceptance hook would have refused when it was accepted.

    The board write point refuses these shapes; one on the board means something wrote run.json without
    ticket set (or before the hook). Rows accepted before the hook (LEGACY_TOOL_CUTOFF) stay with the
    coverage gate and its signed migration."""
    try:
        from release_coverage import LEGACY_TOOL_CUTOFF, acceptance_problem
    except ImportError:
        _warn_missing_delivery("release_coverage")
        return None
    raw = t.get('accepted_at')
    if not raw:
        return dict(reason='missing_accepted_at', accepted_at=None, advice=POST_HOOK_ADVICE)
    try:
        at = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
        if at.tzinfo is None:
            raise ValueError(raw)
    except ValueError:
        return dict(reason='unparseable_accepted_at', accepted_at=raw, advice=POST_HOOK_ADVICE)
    if at < LEGACY_TOOL_CUTOFF:
        return None
    # A not_applicable disposition may be signed after the acceptance (the coverage migration did) or
    # expire later: the row is fine if the hook passes it at either time.
    problem = acceptance_problem(tid, t, root, now=at, tickets=tickets)
    if problem and acceptance_problem(tid, t, root, tickets=tickets):
        problem = acceptance_problem(tid, t, root, tickets=tickets)
    else:
        problem = None
    if problem:
        return dict(reason=problem[0], detail=problem[1], accepted_at=raw, cutoff=LEGACY_TOOL_CUTOFF.isoformat(),
                    advice=POST_HOOK_ADVICE)
    return None


def inspect_run(run_path: Path, *, locks_dir=None, now=None, quota_unavailable_minutes=5, disabled_rules=()):
    now = time.time() if now is None else now
    run_path = Path(run_path)
    if run_path.is_dir():
        run_path /= 'run.json'
    root = run_path.parent
    # Missing/malformed run is an invocation failure, not a clean patrol.
    run = read_json_retry(run_path)
    if not isinstance(run, dict):
        raise ValueError('run.json must be an object')
    tickets = ts.all_tickets(run)
    tracked = read_json(root / 'watch.json').get('tracked') or {}
    directory = Path(locks_dir) if locks_dir else root / 'control/locks'
    holders, waiters = [], {}
    for p in sorted(list(directory.glob('*.holder.json')) + list(directory.glob('*.holders/*.json'))):
        name = (p.parent.name.removesuffix('.holders') + (':shared' if read_json(p).get('shared') else '')) if p.parent.name.endswith('.holders') else p.name.removesuffix('.holder.json').removesuffix('.lock')
        if '.slot-' in name:
            name = canonical_lock(name)
        holders.append((name, (p, read_json(p))))
    for d in sorted(directory.glob('*.waiters')):
        name = d.name.removesuffix('.waiters').removesuffix('.lock')
        waiters[name] = [(p, read_json(p)) for p in sorted(d.glob('*.json'))]
    findings = []

    def add(ticket, rule, owner='parent', **evidence):
        findings.append({'ticket':ticket, 'rule':rule, 'owner':owner, 'evidence':evidence})

    quota_path = root / 'control/quota-reading-status.json'
    quota = read_json(quota_path)
    since, observed = stamp(quota.get('unavailable_since')), stamp(quota.get('observed_at'))
    if (quota.get('budget_judgment') == 'unavailable' and since is not None
            and observed is not None and since <= observed <= now
            and observed - since > quota_unavailable_minutes * 60):
        add('<quota>', 'quota-reading-unavailable', path=str(quota_path),
            unavailable_since=since, observed_at=observed,
            duration_seconds=observed-since, threshold_minutes=quota_unavailable_minutes,
            reason=quota.get('reason'), last_valid_reading_at=quota.get('last_valid_reading_at'),
            last_valid_reading=quota.get('last_valid_reading'), automatic=False)

    from hostdag_fix_regression import recurrence_findings
    findings.extend(recurrence_findings(tickets))

    # Reuse the observer's predicates and grace clock. The library's read-only
    # mode leaves stale metadata and event logs to the owning watcher.
    key = (str(root.resolve()), str(directory.resolve()))
    observed = _LOCK_OBSERVATIONS.setdefault(key, {})
    watch = read_json(root / 'watch.json')
    for clock in ('resource_lock_wait_since', 'resource_lock_incomplete_since'):
        observed.setdefault(clock, {}).update(watch.get(clock) or {})
    observed['state_path'] = str(root / 'watch.json')
    for event in lock_events(observed, now, read_only=True, locks_dir=directory):
        if event['trigger'] == 'missing-holder':
            add('<unregistered>', 'lock-held-no-holder', 'L1', path=event['path'],
                pids=sorted({r['pid'] for r in event['owners']}),
                processes=event.get('processes', []))
        elif event['trigger'] == 'lane-acquiring':
            for waiter in event['waiters']:
                add(waiter.get('ticket') or '<unregistered>', 'lane-acquiring',
                    path=event['path'], waiter=waiter, informational=True)

    unit_states = {}
    for name, (p, h) in holders:
        tid = h.get('ticket') or '<unregistered>'
        reason = phase_mismatch(str(h.get('phase','')), name, str(tid))
        if reason:
            add(tid,'lock-phase-mismatch',path=str(p),phase=h.get('phase'),lock=name,reason=reason)
        # Duration verdicts (expected-min-2x/max-min) belong to the lock
        # observer alone; a second lint finding would page the same overrun twice.
        if h and not h.get('core'):
            for wp,w in waiters.get(name,[]):
                queued = stamp(w.get('queued_at'))
                if w.get('core') and queued is not None and now-queued > 600:
                    add(w.get('ticket') or '<unregistered>','core-starved','L1',holder=str(p),waiter=str(wp),
                        holder_ticket=tid,queued_at=queued)

    if view.freeze_gate.freeze_install_receipt(root):
        # Bind the switch operation to an existing row for the same CLI CAS path.
        owner = next(iter(tickets), None)
        if owner:
            add(owner, 'freeze-gate-clear', automatic=True)
    reported_steps = set()
    for tid,t in tickets.items():
        if not isinstance(t,dict):
            continue
        if 'parent send failed' in str(t.get('blocker') or '').lower():
            add(tid, 'parent-send-failed', blocker=t['blocker'])
        state = ts.derive_state(t,tickets)
        stale = view.freeze_gate.stale_exempt(root, t)
        if stale and state not in ts.TERMINAL | {ts.FROZEN_GATE}:
            add(tid, 'freeze-gate-stale-exempt', automatic=True, **stale)
        if view.critical_tool(t):
            from hostdag_lint_actions import degrade_schema, degrade_proposal
            try:degrade_schema(root,t)
            except (ValueError,OSError,TypeError,KeyError) as exc:
                add(tid,'degrade-plan-invalid',severity='P2',reason=str(exc))
                if state==ts.CREATED and not t.get('degrade_plan'):
                    add(tid,'review-cap-plan-missing',severity='P1',missing=['degrade_plan'])
            step = (t.get('freeze_exempt') or {}).get('blocked_step')
            proposal=degrade_proposal(root,run,tid,t,now)
            if proposal and (not step or step not in reported_steps):
                add(tid,'review-cap-degrade-due',severity='P1',trigger_at=proposal['trigger_at'],reason=proposal['trigger'],option=proposal['option'],blocked_step=step)
                if step:reported_steps.add(step)
            if tid=='TOOL-POLICY-16-REVIEW-CAP':
                at,reason=view.review_cap_trigger(t)
                if at is not None and now>=at and state not in ts.TERMINAL:
                    add(tid,'review-cap-escalate-fable',severity='P1',reason=reason)
        if state in ts.DEGRADE_STATES:
            execution=t.get('degrade_execution') or {}
            add(tid,'degrade-coordinator-action',state=state,receipt_path=execution.get('receipt_path'),unit=execution.get('unit'))
            if state==ts.DEGRADING and execution.get('unit'):
                details=unit_details(execution['unit'])
                if details and details.get('active_state') in ('failed','inactive'):
                    add(tid,'degrade-execution-unconfirmed',severity='P1',unit=execution['unit'],receipt_path=execution.get('receipt_path'))
            continue
        if state in ts.DONE:
            bypass = post_hook_acceptance_problem(root, tid, t, tickets)
            if bypass:
                add(tid, 'accepted-bypassed-hook', severity='P1', blocking=False, **bypass)
        if state in ts.TERMINAL:
            continue
        blocker = str(t.get('blocker') or '')
        until = re.search(r'\bWAIT-UNTIL:(\S+)', blocker) if state in ts.WAITING else None
        if until:
            try:
                deadline = datetime.fromisoformat(until[1].replace('Z', '+00:00')).timestamp()
            except (ValueError, OverflowError, OSError):
                deadline = None
            if deadline is not None and now - deadline >= 1800:
                add(tid, 'wait-until-expired', state=state, blocker=blocker,
                    wait_until=until[1], deadline_at=deadline, expired_minutes=(now-deadline)/60)
        for unit in queued_units(('queued ' if state == ts.WAITING_RESOURCE else '') + str(t.get('blocker') or '')):
            if unit not in unit_states:
                unit_states[unit] = unit_state(unit)
            if unit_states[unit] in ('failed', 'inactive'):
                add(tid, 'queued-but-unit-failed', unit=unit, active_state=unit_states[unit],
                    blocker=t.get('blocker'))
        updated = stamp(t.get('updated_at'))
        if not str(t.get('title') or '').strip():
            add(tid,'no-title',title=t.get('title'))
        if state == ts.CREATED and not t.get('executor') and dispatch_ready(t, tickets, root, now):
            created = stamp(t.get('created_at')) or updated
            if created is not None and now-created > 300:
                add(tid,'created-not-dispatched',created_at=created,deps=t.get('deps',[]))
        base = evidence_dir(root,t)
        result = base / 'result.json' if base else None
        result_time = mtime(result) if result else None
        if state == ts.RUNNING and updated is not None:
            heartbeat = running_heartbeat(read_json(result) if result else {}, now, unit_states)
            if heartbeat is False or (heartbeat is None and (now-updated > 1800 or
                    (result_time and result_time > updated and now-result_time > 300))):
                add(tid,'running-no-write',updated_at=updated,result_path=str(result) if result_time else None,
                    result_mtime=result_time)
        if state == ts.REVIEWING:
            for failure in review_failures(root, base, tid, t, tracked, updated, now):
                add(tid, 'verdict-not-returned', **failure)
        if state == ts.RUNNING:
            observation = tracked.get(tid) or {}
            if observation.get('thread') != t.get('executor'):
                observation = {}  # ignore a previous executor's terminal state
            cli = read_json(base/'cli-run.json') if base else {}
            if not cli and base:
                cli = read_json(base/'cli/cli-run.json')
            dispatched = (stamp(t.get('dispatched_at')) or stamp(cli.get('startedAt'))
                          or stamp(observation.get('direction_anchor')) or updated)
            error = str(cli.get('error') or t.get('dispatch_error') or '')
            failed = str(cli.get('status','')).lower() in ('failed','error') or any(
                x in error.lower() for x in ('serveroverloaded','at capacity','start failed'))
            if observation.get('last_status') in ('error','failed','not_found') and not observation.get('server_seen_turn'):
                failed = True
                error = error or 'watcher: ' + observation['last_status']
            has_session = cli.get('threadId') or cli.get('sessionId') or t.get('executor')
            if dispatched is not None and now-dispatched >= 300 and (failed or not has_session):
                error_kind = next((x for x in ('serverOverloaded','at capacity','start failed') if x.lower() in error.lower()),
                                  'startup/session missing or failed')
                add(tid,'executor-start-failed',dispatched_at=dispatched,dispatch_error=error_kind,
                    path=str(base/'cli-run.json') if cli else None)
        wait = str(t.get('wait_on') or '')
        if wait.startswith('heavy-slot:'):
            requested = wait.split(':',1)[1].removesuffix('.lock')
            rows = waiters.get(requested,[])
            if not any(w.get('ticket') == tid for _,w in rows):
                add(tid,'lock-wait-not-queued',wait_on=wait,locks_dir=str(directory))
        done = deps_all_done(root, run, tid, now)
        if done:
            add(tid, 'deps-all-done', ready_since=done['ready_since'], wait_on=wait,
                dependency_states={name: ts.derive_state(dep) for name, dep in done['dependencies'].items()})
        deps = set(t.get('deps') or [])
        waited = wait.split(':',1)[1] if wait.startswith('ticket:') else None
        if waited:
            deps.add(waited)
        for dep in sorted(deps):
            other = tickets.get(dep)
            ds = ts.derive_state(other) if isinstance(other,dict) else 'MISSING'
            if ds in (ts.FAILED,ts.CANCELED,'MISSING'):
                add(tid,'dep-dead',dependency=dep,dependency_state=ds)
            elif dep == waited and ds in ts.DONE:
                locations = [name for name, body in (run.get('batches') or {}).items()
                             if tid in (body.get('tickets') or {})]
                batch = locations[0] if len(locations) == 1 else None
                revision = view.done_wait_revision(run, batch, tid) if batch is not None else None
                add(tid, 'wait-on-done', wait_on=wait, dependency_state=ds,
                    batch=batch, expected_revision=revision)
        chain = t.get('chain_id')
        groups = {}
        for a in t.get('attempts') or []:
            if not isinstance(a,dict):
                continue
            key = (a.get('chain_id') or chain,a.get('error_class'),a.get('method'))
            ident = a.get('id') or a.get('attempt')
            if not all(key) or ident is None:
                continue
            previous = groups.setdefault(key,{})
            if ident in previous:
                continue
            if len(previous) >= 2 and (a.get('classification') == 'INVALID_TOOL' or
                                      a.get('status') in ('DISPATCHED','RUNNING')):
                add(tid,'same-tool-error-3rd','L1',chain_id=key[0],error_class=key[1],method=key[2],
                    previous_attempts=list(previous),attempt=ident)
                break
            if a.get('classification') == 'INVALID_TOOL':
                previous[ident] = True
            else:
                previous.clear()  # successful/intervening different outcome breaks the streak
        if base and base.is_dir() and not base.is_symlink():
            for folder,dirs,files in os.walk(base,followlinks=False):
                dirs[:] = sorted(d for d in dirs if not (Path(folder)/d).is_symlink())
                for name in sorted(files):
                    p = Path(folder)/name
                    try:
                        if p.is_symlink() or not p.is_file():
                            continue
                        stat = p.stat()
                        if stat.st_size >= LARGE:
                            add(tid,'evidence-large-file',path=str(p),size=stat.st_size,mtime=stat.st_mtime)
                    except OSError:
                        continue
    for batch, body in run.get('batches', {}).items():
        for tid in body.get('tickets', {}):
            for rule in ('verdict-not-registered', 'wait-on-decided'):
                plan = auto_plan(root, run, batch, tid, rule)
                if plan:
                    add(tid, rule, batch=batch, expected_revision=plan['revision'], **plan['proof'])
                elif rule == 'verdict-not-registered':
                    t = body['tickets'][tid]
                    reviews = t.get('reviews') or {}
                    if (ts.derive_state(t) != ts.REVIEWING or not t.get('candidate')
                            or (reviews and all(r.get('verdict') for r in reviews.values()))):
                        continue
                    base = evidence_dir(root, t)
                    paths = list(base.glob('review*/verdict.json')) + [base / 'verdict.json'] if base else []
                    for path in paths:
                        payload, at = read_json(path), mtime(path)
                        verdict = str(payload.get('verdict', '')).upper()
                        if not at or now-at < 300 or verdict not in ('PASS', 'FAIL', 'CHANGES', 'NOT PASS'):
                            continue
                        if payload.get('candidate') and not same_candidate(payload['candidate'], t['candidate']):
                            continue
                        bindings = [(axis, r) for axis, r in reviews.items() if r.get('evidence')
                                    and resolve(root, r['evidence']).resolve() in (path.resolve(), path.parent.resolve())]
                        if any(r.get('candidate') and not same_candidate(r['candidate'], t['candidate']) for _, r in bindings):
                            continue
                        # Registration belongs to the file's bound axis, not
                        # the whole review set. An unfinished standards review
                        # must not resurrect an already registered spec PASS.
                        payload_axis = payload.get('axis') or payload.get('review_axis')
                        if len(bindings) == 1:
                            axis, registered = bindings[0]
                            recorded = str(registered.get('verdict', '')).upper()
                            normalized = 'FAIL' if verdict in ('CHANGES', 'NOT PASS') else verdict
                            if (recorded == normalized and payload_axis in (None, axis)
                                    and all(registered.get(k) is None or str(registered[k]) == str(t.get(k))
                                            for k in ('attempt', 'round'))
                                    and all(payload.get(k) is None or str(payload[k]) == str(t.get(k))
                                            for k in ('attempt', 'round'))):
                                continue
                        observation = [tracked.get(f'{tid}-review-{axis}') or {} for axis, r in reviews.items()
                                       if (tracked.get(f'{tid}-review-{axis}') or {}).get('thread') == r.get('thread')]
                        if any(o.get('terminal') is False for o in observation):
                            continue
                        if any(payload.get(k) is not None and str(payload[k]) != str(t.get(k)) for k in ('attempt', 'round')):
                            continue
                        if any(f['ticket'] == tid and f['rule'] == 'verdict-not-returned'
                               and f['evidence'].get('path') == str(path) for f in findings):
                            break
                        add(tid, rule, path=str(path), verdict=verdict, automatic=False,
                            reason='缺当前 candidate/axis/attempt/round 显式绑定或存在更新结果；交父核对')
                        break
    leftover_rows = json_rows(root / 'control/leftover-ledger.jsonl')
    findings.extend(leftover_findings(leftover_rows, tickets, now))
    latest, opened = {}, {}
    for row in leftover_rows:
        ident = row.get('id')
        if ident:
            latest[ident] = row
            if row.get('kind') == 'live-patch' and row.get('status') == 'OPEN':
                opened.setdefault(ident, stamp(row.get('at')))
            else:
                opened.pop(ident, None)
    for ident, row in latest.items():
        at = opened.get(ident)
        if row.get('kind') == 'live-patch' and row.get('status') == 'OPEN' and at is not None and now-at > 86400:
            add(row.get('scheduled_ticket') or ident, 'live-patch-unscheduled', id=ident,
                opened_at=at, env=row.get('env'), object=row.get('object'), solidify_target=row.get('solidify_target'))
    findings.extend(legacy_lock_findings((root / 'control/board-impact').glob('*.md')))
    findings.extend(decision_findings(root, run, findings, now))
    disabled = set(disabled_rules) | {r for r in os.environ.get('HOST_DAG_LINT_DISABLE_RULES', '').split(',') if r}
    if HOT_LINK_RULE not in disabled:
        findings.extend(hot_link_findings(root))
    if 'chain-result-latency' not in disabled:
        findings.extend(chain_result_latency_findings(root, run, now))
    if 'idle_nonchain_slots' not in disabled:
        try:
            findings.extend(idle_nonchain_slot_findings(tickets, root))
        except Exception:  # fail-open: a broken advisory rule never hides other findings
            pass
    if 'accepted_not_in_release_head' not in disabled:
        try:
            findings.extend(accepted_not_in_release_head_findings(tickets, root))
        except Exception:  # fail-open
            pass
    return findings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',required=True,type=Path)
    parser.add_argument('--locks',type=Path)
    parser.add_argument('--source', action='append', type=Path, default=[], help='contract/script to check for legacy names (advisory)')
    parser.add_argument('--now',type=float)
    parser.add_argument('--quota-unavailable-minutes', type=float, default=5,
                        help='report an observed unavailable streak longer than N minutes (default 5)')
    parser.add_argument('--disable-rule', action='append', default=[], help='skip an optional rule (repeatable; also HOST_DAG_LINT_DISABLE_RULES=a,b)')
    args = parser.parse_args(argv)
    if not math.isfinite(args.quota_unavailable_minutes) or args.quota_unavailable_minutes < 0:
        parser.error('--quota-unavailable-minutes must be finite and nonnegative')
    findings = inspect_run(args.run,locks_dir=args.locks,now=args.now,
                           quota_unavailable_minutes=args.quota_unavailable_minutes,
                           disabled_rules=args.disable_rule)
    actions = []
    root = args.run if args.run.is_dir() else args.run.parent
    disabled = set(args.disable_rule)
    # Thaw first, then at most one ticket freeze; mutations go through board CLI.
    for finding in sorted(findings, key=lambda f: f['rule'] != 'freeze-gate-clear'):
        rule = finding['rule']
        if rule not in view.freeze_gate.FREEZE_RULES or rule in disabled:
            continue
        run = read_json_retry(root / 'run.json')
        tid = finding['ticket']
        batches = [b for b, body in run.get('batches', {}).items() if tid in body.get('tickets', {})]
        if len(batches) != 1:
            continue
        plan = auto_plan(root, run, batches[0], tid, rule)
        if not plan:
            continue
        command = [sys.executable, str(Path(__file__).with_name('host-dag-view.py')),
                   '--run', str(root), 'ticket', 'set', '--batch', batches[0], '--ticket', tid,
                   '--lint-action', rule, '--expected-revision', plan['revision']]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=30)
        actions.append({'rule': rule, 'exit': proc.returncode, 'stdout': proc.stdout, 'stderr': proc.stderr})
        if rule == 'freeze-gate-stale-exempt' or proc.returncode:
            break
    if actions:
        findings = inspect_run(args.run, locks_dir=args.locks, now=args.now,
                               quota_unavailable_minutes=args.quota_unavailable_minutes,
                               disabled_rules=args.disable_rule)
    findings.extend(legacy_lock_findings(args.source))
    print(json.dumps({'findings':findings, 'freeze_gate_actions':actions},ensure_ascii=False,indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())

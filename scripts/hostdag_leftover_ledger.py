#!/usr/bin/env python3
"""Append-only leftover ledger: strict writes, non-destructive migration and review proposals.

Usage: hostdag_leftover_ledger.py append --ledger FILE --input ROW_OR_ARRAY.json
       hostdag_leftover_ledger.py migrate --ledger OLD --output-dir NEW [--triage FILE]
       hostdag_leftover_ledger.py weekly --ledger FILE [--now ISO_OR_EPOCH]
       hostdag_leftover_ledger.py candidates --ledger FILE --run FILE [--triage FILE]

Migration never decides closure: incomplete/ambiguous states become OPEN pending
advisor review, with the original row and all normalization assumptions retained.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
from hostdag_json import atomic_json
import re
import time
from zoneinfo import ZoneInfo

ZONE = ZoneInfo('Asia/Taipei')
CHAINS = ('其他',)
CORE = set(CHAINS[:-1])
KINDS = ('defect', 'tooling', 'created_resource', 'followup', 'live-patch', 'roadmap')
STATES = ('OPEN', 'SCHEDULED', 'CLOSED', 'DROPPED')
REQUIRED = ('id', 'chain', 'kind', 'severity', 'owner', 'trigger', 'status')
# Explicit semantic mapping. Unknown strings are never matched by prefix.
OPEN_STATES = '''OPEN|IN_PROGRESS|待执行|待规划|进行中|记录|RESOLVED-PENDING-REVIEW|PARTIAL_STOPPED|待裁定|等实测|修复中|仍残留，已只读核对|交协调者判定|已恢复-审查中|物理资源已清理；journal仍deleting|交父分层处理|交父处理已确认创建|源码已修-待实机|LEFTOVER|SCOPED缺口|READY_FOR_PARENT_APPLY|CLOSED-UNFINISHED|NOT_VERIFIED|DEFERRED|TRACKED|SUPERSEDED|INCONCLUSIVE|部分解决|CLOSED_SOURCE_PENDING_RUNTIME|PARTIAL|已决定|NEEDS_USER|CLOSED_EXPORT_PENDING_FUTURE_CUTOVER|CLOSED_SOURCE_OPEN_HISTORICAL_PROVENANCE|DECIDED_POLICY_WITH_PROVENANCE_LIMIT|已记录|DEFER|DUP|DEFERRED_NEW_FEATURE|GAP|ROADMAP|BLOCKED|BLOCKED_ENV|WAITING_USER|DECIDED|EXTERNALLY_BLOCKED|OUT_OF_SCOPE|SCOPED_PASS|NOT_MERGED|RECORDED|MERGED|REVIEW_PASS|RETAINED|ACTIVE|DEFERRED_NEXT_DELIVERY|PENDING|READY|IN_PROGRESS_PARTIAL|RUNNING|待派|open|retained|disabled_retained|adopted_retained|retained_not_held_by_this_ticket|mitigated|retained-adopted|retained-private-rollback'''.split('|')
CLOSED_STATES = '''CLOSED|RESOLVED|已关闭-无需改动|已关闭-上游限制|已修复|已解决|已解决（路径）|CLOSED_SOURCE|CLOSED_DOCUMENTATION|CLOSED_EVIDENCE|CLOSED_SCOPED_RUNTIME|CLEANED|SATISFIED|CLOSED_BY_USER|cleaned_verified|finished_no_active_process|closed|removed; NotFound read back|released; read back RELEASED|exited; unit gone; clean_chrome context exited|deleted; NotFound read back|RELEASED, read back|inactive, ExecMainStatus=0, context exited|released|stopped|removed|exited-collected|removed (copy in control/; never committed)|never persisted (--dry-run=server; absence read back)|closed-removed|product delete read back; PV data removed; ledger CM kvop-27a2d2… kept by product (state=deleted)|inactive, Result=success, ExecMainStatus=0, context exited|inactive; control/browser-closed.json'''.split('|')
STATUS_MAP = {s: 'OPEN' for s in OPEN_STATES}
STATUS_MAP.update({s: 'CLOSED' for s in CLOSED_STATES})
STATUS_MAP.update({'': 'OPEN', 'SCHEDULED': 'SCHEDULED', '转票': 'SCHEDULED',
                   'DISPATCHED': 'SCHEDULED', 'PLANNED': 'SCHEDULED', 'DROPPED': 'DROPPED', 'NOT_REACHED': 'OPEN'})
TICKET = re.compile(r'\b(?:V\d+[A-Z]*|ARM|OFFLINE\d*)-[A-Z0-9]+(?:-[A-Z0-9]+)*\b')


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def stamp(value):
    try:
        if isinstance(value, bool): return None
        if isinstance(value, (float, int)):
            return float(value) if math.isfinite(value) else None
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return (dt if dt.tzinfo else dt.replace(tzinfo=ZONE)).timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError):
        return None


def ticket_id(value):
    return nonempty(value) and re.fullmatch(r'[A-Z0-9]+(?:-[A-Z0-9]+)+', value) is not None


def validate(row):
    if not isinstance(row, dict): raise ValueError('record must be an object')
    for key in REQUIRED:
        if not nonempty(row.get(key)): raise ValueError(f'{key}: required nonempty string')
    for key, choices in [('chain', CHAINS), ('kind', KINDS), ('severity', ('P1', 'P2', 'P3')), ('status', STATES)]:
        if row[key] not in choices: raise ValueError(f'{key}: expected one of {choices}')
    if row['kind'] == 'live-patch':
        for key in ('env', 'object', 'live_value', 'solidify_target'):
            if (key not in row or row[key] is None or row[key] == ''
                    or (key != 'live_value' and not nonempty(row[key]))):
                raise ValueError(f'live-patch requires {key}')
    if 'at' in row and stamp(row['at']) is None: raise ValueError('at: invalid timestamp')
    if row['status'] == 'SCHEDULED':
        if not ticket_id(row.get('scheduled_ticket', row.get('ticket'))):
            raise ValueError('SCHEDULED requires scheduled_ticket (or legacy ticket alias)')
        if row.get('scheduled_ticket') and row.get('ticket') and row['scheduled_ticket'] != row['ticket']:
            raise ValueError('scheduled_ticket and ticket conflict')
    if row['status'] == 'CLOSED':
        closure = row.get('closure')
        if not isinstance(closure, dict) or not (ticket_id(closure.get('ticket')) or nonempty(closure.get('deletion_readback'))):
            raise ValueError('CLOSED requires closure.ticket or closure.deletion_readback')
    if row['status'] == 'DROPPED' and row['chain'] in CORE:
        raise ValueError('core-chain leftovers cannot be DROPPED')
    if row['status'] == 'DROPPED' and not (nonempty(row.get('decision_id')) and nonempty(row.get('reason'))):
        raise ValueError('DROPPED requires advisor/Fable decision_id and reason')


def validate_append(row):
    """Admission rules apply only to new writes, never historical rows."""
    validate(row)
    if row['kind'] == 'followup' and row['severity'] == 'P3':
        raise ValueError('P3 followup rejected: record it in the ticket conclusion only')
    if row['severity'] == 'P2' and row['kind'] in ('defect', 'tooling'):
        if not nonempty(row.get('evidence')):
            raise ValueError('P2 defect/tooling requires evidence path (nonempty string)')
    if row['kind'] == 'created_resource':
        for key in ('cleanup_owner', 'due_at'):
            if not nonempty(row.get(key)):
                raise ValueError(f'created_resource requires {key}')
    if row['kind'] == 'roadmap' and row['chain'] in CORE:
        raise ValueError('core-chain leftovers cannot be roadmap')


def lifecycle_master(row):
    # Explicit master identity; member rows merely referring to it still count.
    return row.get('kind') == 'created_resource' and str(row.get('id', '')).startswith('LIFECYCLE-')


def read_rows(path):
    if not path.exists(): return []
    rows = []
    for line, text in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
        if text.strip():
            row = json.loads(text)
            if not isinstance(row, dict): raise ValueError(f'{path}:{line}: expected object')
            rows.append(row)
    return rows


def dump(path, value):
    atomic_json(path, value)


@contextmanager
def writer_lock(path):
    # Stable across atomic replacement; all v2 writers and activate share this lock.
    with path.with_name(path.name + '.write.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def append(path, rows):
    for row in rows: validate_append(row)
    rows = [dict(r, at=r.get('at', time.time())) for r in rows]
    payload = ''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in rows)
    with writer_lock(path), path.open('a+', encoding='utf-8') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0)
        existing = stream.read()
        for text in existing.splitlines():
            if text.strip() and not isinstance(json.loads(text), dict):
                raise ValueError('existing ledger record must be an object')
        if existing and not existing.endswith('\n'): raise ValueError('ledger lacks trailing newline')
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def activate(path, staged, backup, expected_sha256):
    """CAS replacement in a writer-idle window; never overwrite intervening appends."""
    payload = staged.read_bytes()
    rows = [json.loads(s) for s in payload.decode('utf-8').splitlines() if s.strip()]
    for row in rows: validate(row)
    if not payload or not payload.endswith(b'\n'): raise ValueError('staged ledger must end in newline')
    with writer_lock(path), path.open('rb') as original:
        fcntl.flock(original, fcntl.LOCK_EX)
        before = original.read()
        digest = hashlib.sha256(before).hexdigest()
        if digest != expected_sha256: raise ValueError('central ledger changed; remigrate new rows before activation')
        with backup.open('xb') as saved:
            saved.write(before); saved.flush(); os.fsync(saved.fileno())
        # Reject legacy appends while preparing, then publish through the same
        # symlink-safe implementation as the other metadata writers.
        def unchanged():
            if path.read_bytes() != before:
                raise ValueError('legacy writer changed central ledger during activation; stop writer and retry')
        from hostdag_json import atomic_text
        atomic_text(path, payload.decode('utf-8'), durable=True, before_replace=unchanged)
        receipt = dict(before_sha256=digest, after_sha256=hashlib.sha256(payload).hexdigest(),
                       before_lines=len(before.splitlines()), after_lines=len(payload.splitlines()), backup=str(backup))
        if path.read_bytes() != payload: raise ValueError('post-activation readback changed unexpectedly')
        return receipt


def chain_for(row):
    chain = row.get('chain', '')
    if chain in CHAINS: return chain
    return '其他'


def triage_rows(path):
    return json.loads(path.read_text(encoding='utf-8')) if path else []


def legacy_closure(value):
    """Translate an existing closure reference, preserving its complete scope/prose.

    This preserves a historical closure claim; it does not approve a new one.
    Only ticket references inside the closure itself count, never owner/next.
    """
    if isinstance(value, dict) and (ticket_id(value.get('ticket')) or nonempty(value.get('deletion_readback'))):
        return value
    if isinstance(value, dict):
        text = json.dumps({k: value[k] for k in ('path', 'evidence') if k in value}, ensure_ascii=False)
    elif isinstance(value, str):
        text = value
    else:
        return value
    tickets = list(dict.fromkeys(TICKET.findall(text)))
    if tickets:
        return dict(ticket=tickets[0], referenced_tickets=tickets, original_closure=value,
                    scope=value.get('scope') if isinstance(value, dict) else value,
                    claim='historical closure only; no new acceptance')
    return value


def normalize(rows, triage=None):
    line_ids = {h['line']: t['id'] for t in (triage or []) for h in t.get('history', []) if 'line' in h}
    merged, out, issues = {}, [], []
    for line, original in enumerate(rows, 1):
        identity = original.get('id') if nonempty(original.get('id')) else line_ids.get(line)
        if not identity:
            identity_text = json.dumps({k: original[k] for k in ('ticket', 'source', 'object', 'description', 'title', 'resource') if k in original}, sort_keys=True, ensure_ascii=False)
            # Without an identifying subject, do not merge unrelated anonymous records.
            if identity_text == '{}': identity_text = json.dumps(original, sort_keys=True, ensure_ascii=False)
            identity = 'LEGACY-' + hashlib.sha256(identity_text.encode()).hexdigest()[:16]
        row = {**merged.get(identity, {}), **original, 'id': identity}
        merged[identity] = dict(row)  # inherit sparse updates, never normalized defaults/proof
        assumptions = []
        old_chain = row.get('chain')
        row['chain'] = chain_for(row)
        if old_chain != row['chain']: assumptions.append('chain inferred/aliased; advisor review required')
        kind = row.get('kind')
        if kind not in KINDS:
            kind = 'created_resource' if 'object' in row or 'cleanup_owner' in row else ('tooling' if row.get('category') == 'tooling' else 'followup')
            assumptions.append('kind inferred from resource/category, otherwise followup')
        row['kind'] = kind
        if row.get('severity') not in ('P1', 'P2', 'P3'):
            row['severity'] = 'P2'; assumptions.append('missing severity: provisional P2, advisor review required')
        for key, default in [('owner', row.get('cleanup_owner') or row.get('ticket') or 'advisor'),
                             ('trigger', row.get('next') or row.get('due_at') or '下次遗留复盘时由顾问确认处理触发条件')]:
            if not nonempty(row.get(key)):
                row[key] = str(default); assumptions.append(f'{key} inferred; advisor review required')
        legacy_status = row.get('status', '')
        status = STATUS_MAP.get(str(legacy_status))
        if not legacy_status: assumptions.append('missing status: conservative OPEN')
        problem = None
        if status is None: problem = f'unmapped status: {legacy_status!r}'
        row['status'] = status or 'OPEN'
        if row['status'] == 'CLOSED':
            row['closure'] = legacy_closure(row.get('closure'))
        try: validate(row)
        except ValueError as error:
            problem = str(error); row['status'] = 'OPEN'
        if stamp(row.get('at')) is None:
            problem = 'missing/invalid timestamp'; row.pop('at', None)
        if problem:
            issues.append(dict(id=identity, line=line, legacy_status=legacy_status, reason=problem))
        row['migration'] = dict(line=line, legacy_status=legacy_status, assumptions=assumptions,
                                review_required=bool(problem or assumptions), legacy=original)
        out.append(row)
    return out, issues


def migrate(source, output, triage=None):
    # Snapshot once so concurrent legacy writers cannot give backup/output different inputs.
    raw = source.read_bytes()
    rows = [json.loads(s) for s in raw.decode('utf-8').splitlines() if s.strip()]
    if any(not isinstance(r, dict) for r in rows): raise ValueError('legacy records must be objects')
    normalized, issues = normalize(rows, triage_rows(triage))
    for row in normalized: validate(row)
    output.mkdir(parents=True, exist_ok=False)
    backup, ledger = output / 'leftover-ledger.original.jsonl', output / 'leftover-ledger.v2.jsonl'
    backup.write_bytes(raw)
    ledger.write_text(''.join(json.dumps(r, ensure_ascii=False)+'\n' for r in normalized), encoding='utf-8')
    report = dict(backup=str(backup), ledger=str(ledger), source=str(source), source_sha256=hashlib.sha256(raw).hexdigest(),
                  rows=len(rows), items=len({r['id'] for r in normalized}), mapping=STATUS_MAP, unmapped=issues,
                  provisional_defaults='P2 / advisor / next review; not approved severity or ownership',
                  synthetic_identity='triage history line mapping, otherwise stable subject hash; advisor must review collisions')
    dump(output / 'migration.json', report)
    return report


def latest(rows, now):
    result = {}
    for row in rows:
        at = stamp(row.get('at'))
        if at is not None and at <= now: result[row['id']] = row
    return result


def summarize(rows, now, window=86400):
    groups = defaultdict(lambda: dict(new=0, closed=0, unclosed=0))
    current = latest(rows, now)
    debt_rows = [r for r in rows if current.get(r['id'], {}).get('kind') != 'roadmap']
    first, prior, closed_ids = set(), {}, set()
    total = dict(new=0, closed=0, unclosed=0)
    for row in debt_rows:
        at = stamp(row.get('at'))
        if at is None or at > now: continue
        identity = row['id']; group = groups[f'{row["chain"]}/{row["severity"]}']
        if identity not in first and now-window < at:
            total['new'] += 1; group['new'] += 1
        if row['status'] == 'CLOSED' and prior.get(identity) not in ('CLOSED', 'DROPPED') and now-window < at and identity not in closed_ids:
            total['closed'] += 1; group['closed'] += 1; closed_ids.add(identity)
        first.add(identity); prior[identity] = row['status']
    for row in current.values():
        if row.get('kind') != 'roadmap' and row['status'] in ('OPEN', 'SCHEDULED'):
            total['unclosed'] += 1; groups[f'{row["chain"]}/{row["severity"]}']['unclosed'] += 1
    total['roadmap'] = sum(r.get('kind') == 'roadmap' for r in current.values())
    total['statuses'] = {status: sum(r['status'] == status for r in current.values()) for status in STATES}
    total['groups'] = dict(sorted(groups.items()))
    total['details'] = [r for r in current.values() if r.get('kind') != 'roadmap' and r['status'] in ('OPEN', 'SCHEDULED') and (r['severity'] == 'P1' or r['chain'] in CORE)]
    total['missing_timestamp'] = sorted({r['id'] for r in rows if stamp(r.get('at')) is None})
    return total


def weekly(rows, now):
    return [dict(id=r['id'], proposed_status='DROPPED', reason='P3 超过 14 天无状态更新，建议周复盘批量裁定',
                 last_at=r['at'], age_days=round((now-stamp(r['at']))/86400, 2), decision_required=True,
                 required_authority='advisor decision_id (Fable for product/core exceptions)')
            for r in latest(rows, now).values() if r['severity'] == 'P3' and r['status'] in ('OPEN', 'SCHEDULED')
            and now-stamp(r['at']) > 14*86400]


def ticket_records(run):
    for prefix, tickets in [('tickets', run.get('tickets', {}))] + [
        (f'batches/{b}/tickets', value.get('tickets', {})) for b, value in run.get('batches', {}).items() if isinstance(value, dict)]:
        if isinstance(tickets, dict):
            for name, row in tickets.items():
                if isinstance(row, dict): yield name, row, f'#/{prefix}/{name}'


def candidates(rows, run, triage=None):
    accepted = {name: dict(ticket=name, state=r.get('state', r.get('status')), run_pointer=pointer,
                           evidence=r.get('evidence'), at=r.get('integrated_at', r.get('accepted_at', r.get('updated_at'))))
                for name, r, pointer in ticket_records(run) if r.get('state', r.get('status')) in ('ACCEPTED', 'INTEGRATED')}
    scheduled = {name: r for name, r, _ in ticket_records(run) if r.get('state', r.get('status')) not in ('CANCELLED', 'CANCELED', 'SUPERSEDED', 'CLOSED', 'ACCEPTED', 'INTEGRATED', 'PASS', 'SKIPPED')}
    triaged = {t['id']: t for t in (triage or [])}
    result = dict(resource_closures=[], lifecycle_groups=[], historical_coverage=[], core_defer=[], defer_review_pool=[])
    history = defaultdict(list)
    for row in rows: history[row['id']].append(row)
    for identity, row in {r['id']: r for r in rows}.items():
        legacy = row.get('migration', {}).get('legacy', row)
        text = json.dumps(history[identity], ensure_ascii=False)
        initial = history[identity][0]
        created_at = stamp(initial.get('at'))
        refs = set(TICKET.findall(text))
        tri = triaged.get(identity, {})
        if tri.get('closureTicket'): refs.add(tri['closureTicket'])
        covers = [accepted[t] for t in sorted(refs) if t in accepted and (stamp(accepted[t]['at']) or 0) >= (created_at or 0)]
        base = dict(id=identity, chain=row['chain'], severity=row['severity'], current_status=row['status'],
                    ledger_line=row.get('migration', {}).get('line'), decision_required=True)
        if row['kind'] == 'created_resource':
            closure = row.get('closure')
            proof = legacy.get('receipt') or (closure.get('deletion_readback') if isinstance(closure, dict) else None)
            status_text = str(legacy.get('status', ''))
            deleted = re.search(r'clean|remov|delet|releas|inactive|exit|关闭|删除|清理|释放', status_text, re.I)
            if deleted and not proof:
                paths = re.findall(r'(?:/?[\w.-]+/)+[\w.-]+\.(?:json|md|log)', status_text)
                if paths: proof = paths[0]
                elif re.search(r'read.back|NotFound|读回', status_text, re.I):
                    proof = f'original ledger line {base["ledger_line"]}: {status_text}'
            if proof and deleted:
                result['resource_closures'].append(dict(base, proposed_status='CLOSED', deletion_readback=proof, verification='recorded reference only; advisor must verify original readback'))
            if re.search(r'retain|长期|生命周期', str(legacy), re.I):
                result['lifecycle_groups'].append(dict(base, proposed_group='随开发集群生命周期', object=legacy.get('object', legacy.get('resource')), cleanup_owner='advisor', due_at='长期保留，随开发集群生命周期'))
        at = created_at
        if at is not None and at < stamp('2026-10-24T00:00:00+08:00'):
            result['historical_coverage'].append(dict(base, accepted_matches=covers,
                original_p1_oct21=any(h['severity']=='P1' and stamp(h.get('at')) is not None and datetime.fromtimestamp(stamp(h['at']), ZONE).date().isoformat()=='2026-10-21' for h in history[identity]),
                created_at=initial.get('at'), triage_closure_ticket=tri.get('closureTicket'), duplicate_of=tri.get('dupOf'),
                suggestion=('OPEN; duplicate link does not close master ' + str(tri['dupOf'])) if tri.get('dupOf') else ('CLOSED after scope/readback review' if covers else 'OPEN; no explicit later accepted coverage found')))
        deferred = str(row.get('migration', {}).get('legacy_status', '')).upper() in ('DEFER', 'DEFERRED', 'DEFERRED_NEXT_DELIVERY')
        if row['status'] in ('OPEN', 'SCHEDULED') and deferred:
            choices = sorted(t for t in refs if t in scheduled and chain_for(dict(scheduled[t], ticket=t, chain=scheduled[t].get('chain_id', ''))) == row['chain'])
            if not choices and row['chain'] in CORE:
                choices = sorted(t for t, r in scheduled.items() if chain_for(dict(r, ticket=t, chain=r.get('chain_id', ''))) == row['chain'])
            proposal = dict(base, proposed_status='SCHEDULED', suggested_tickets=choices,
                proposed_new_ticket=None if choices else 'FOLLOWUP-' + re.sub('[^A-Z0-9-]', '-', identity.upper()),
                new_ticket_requires_creation=not bool(choices),
                reason='核对当前票合同；无覆盖票时由父创建明确后续票，不把缺票当不可抗力',
                drop_alternative='仅经证实不可抗力且有 Fable decision_id 时可改为 DROPPED',
                required_authority='L1 scope check; Fable for unavoidable-drop exception')
            result['defer_review_pool'].append(proposal)
            if row['chain'] in CORE: result['core_defer'].append(proposal)
    groups = defaultdict(list)
    for item in result['lifecycle_groups']:
        obj = str(item.get('object', ''))
        host = leftover_host(obj)
        groups[host].append(item['id'])
    result['lifecycle_consolidation'] = [dict(cluster=host, proposed_id=f'LIFECYCLE-{host}', members=ids,
        cleanup_owner='advisor', due_at='长期保留，随开发集群生命周期', decision_required=True) for host, ids in groups.items()]
    result['accepted_ticket_index'] = accepted
    return result


def leftover_host(obj):
    """Group a leftover by host: an explicit host:<name> tag, else a name listed in
    HOST_DAG_LEFTOVER_HOSTS (comma-separated, matched as a substring), else review."""
    tagged = re.search(r'\bhost:([A-Za-z0-9_.-]+)', obj)
    if tagged:
        return tagged[1]
    hosts = [h.strip() for h in os.environ.get('HOST_DAG_LEFTOVER_HOSTS', '').split(',') if h.strip()]
    return next((h for h in hosts if h in obj), 'host needs review')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('append', 'migrate', 'weekly', 'summary', 'candidates', 'validate', 'activate'))
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--input', type=Path)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--triage', type=Path)
    parser.add_argument('--run', type=Path)
    parser.add_argument('--backup', type=Path)
    parser.add_argument('--expected-sha256')
    parser.add_argument('--now', default=str(time.time()))
    args = parser.parse_args(argv)
    try:
        try: now = stamp(float(args.now))
        except ValueError: now = stamp(args.now)
        if now is None: raise ValueError('invalid --now')
        if args.command == 'append':
            if not args.input: raise ValueError('append requires --input')
            value = json.loads(args.input.read_text(encoding='utf-8'))
            append(args.ledger, value if isinstance(value, list) else [value]); return 0
        if not args.ledger.is_file(): raise FileNotFoundError(args.ledger)
        if args.command == 'activate':
            if not (args.input and args.backup and args.expected_sha256): raise ValueError('activate requires --input --backup --expected-sha256')
            result = activate(args.ledger, args.input, args.backup, args.expected_sha256)
        elif args.command == 'migrate':
            if not args.output_dir: raise ValueError('migrate requires --output-dir (must not exist)')
            result = migrate(args.ledger, args.output_dir, args.triage)
        else:
            rows = read_rows(args.ledger)
            for row in rows: validate(row)
            if args.command == 'validate': result = dict(valid=True, rows=len(rows), items=len({r['id'] for r in rows}))
            elif args.command == 'weekly': result = weekly(rows, now)
            elif args.command == 'summary': result = summarize(rows, now)
            else:
                if not args.run: raise ValueError('candidates requires --run')
                result = candidates(rows, json.loads(args.run.read_text(encoding='utf-8')), triage_rows(args.triage))
        print(json.dumps(result, ensure_ascii=False, indent=2)); return 0
    except (ValueError, OSError) as error:
        parser.error(str(error))


if __name__ == '__main__':
    raise SystemExit(main())

"""Conflict policy shared by parent dispatch and the ticket-local freeze guard.

Declarations live in run.json ticket fields (conflict_classes and
review_conflict_classes), or in the contract header (Board-Freeze-Classes and
Board-Freeze-Review-Classes). A review suffix selects the review declaration;
it never grants permission by itself. See board-freeze.md for the wire format.
"""

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hostdag_json import read_json
from hostdag_resource_locks import canonical_lock

SAFE = frozenset({"read-only", "source-review", "read-only/source-review", "remote"})
CONFLICT = frozenset({"local-cluster", "page-lock", "watch-json-write"})
REVIEW = re.compile(r"^(.*)-review-(spec|standards)$")
TERMINAL = frozenset(
    {
        "ACCEPTED",
        "INTEGRATED",
        "CLOSED",
        "CANCELLED",
        "CANCELED",
        "COMPLETED",
        "DONE",
        "SUPERSEDED",
        "REJECTED",
        "DEFERRED",
        "FAILED",
        "ABORTED",
        "STOPPED",
    }
)


FREEZE_BLOCKS = frozenset({'release', 'gate'})
RELEASE_STEPS = ()
PREFLIGHT_INCUMBENTS = frozenset()
FREEZE_RULES = ('freeze-gate-clear', 'freeze-gate-stale-exempt')


def freeze_stamp(value):
    from datetime import datetime
    import math
    try:
        n = float(value)
    except (ValueError, TypeError):
        try:
            n = datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
        except (ValueError, TypeError, OverflowError):
            return None
    return n if math.isfinite(n) else None


def freeze_path(root, value):
    p = Path(str(value))
    return p if p.is_absolute() else root / p


def release_steps(root):
    data = read_json(root / 'control/release-steps.json')
    steps = data.get('steps', [])
    if not isinstance(steps, list) or any(not isinstance(s, dict) or not s.get('id') for s in steps):
        raise ValueError('invalid release-steps.json: steps must contain id records')
    if len({s['id'] for s in steps}) != len(steps):
        raise ValueError('duplicate release step id')
    return {s['id']: s for s in steps}


def receipt_data(path):
    try:
        value = read_json(path)
        return value if isinstance(value, dict) else {}
    except (ValueError, OSError):
        return {}


def receipt_pass(root, value, step):
    if not isinstance(value, str) or not value.strip():
        return False
    path = freeze_path(root, value)
    data = receipt_data(path)
    return (path.is_file() and data.get('blocked_step') == step
            and str(data.get('status') or data.get('verdict') or '').upper() == 'PASS')


def freeze_install_receipt(root):
    marker = root / 'control/freeze-gate'
    if not marker.exists():
        return None
    freeze = read_json(marker)
    since = freeze_stamp(freeze.get('since'))
    target = freeze.get('clears_on')
    if since is None or not isinstance(target, str) or not target.strip():
        return None
    path = freeze_path(root, target)
    data = receipt_data(path)
    if (receipt_pass(root, target, 'gate.install')
            and data.get('ticket') == 'GATE-ACCEPT'
            and data.get('latest_package') is True and path.stat().st_mtime >= since):
        return {'marker': freeze, 'receipt': str(path), 'data': data,
                'mtime_ns': path.stat().st_mtime_ns}
    return None


def stale_exempt(root, row):
    if not (root / 'control/freeze-gate').exists() or not row.get('freeze_exempt'):
        return None
    step = row['freeze_exempt'].get('blocked_step')
    record = release_steps(root).get(step, {})
    if receipt_pass(root, record.get('pass_receipt'), step):
        return {'blocked_step': step, 'pass_receipt': record['pass_receipt']}
    return None


def freeze_gate_problem(root, board, tid, row, batch=None, now=None):
    """Pure admission predicate. Board CLI calls under RunLock; shell reuses it.

    Decision receipts (JSON kind=decision or advisor/ruling Markdown) do not
    expire. All other evidence is failure-class and must be at most 24h old.
    """
    import hostdag_ticket_state as ts
    root = Path(root)
    marker = root / 'control/freeze-gate'
    if not marker.exists() or str(row.get('kind') or '').lower() not in {'tool', 'ops'}:
        return None
    try:
        freeze = read_json(marker)
        if (not freeze.get('reason') or freeze_stamp(freeze.get('since')) is None
                or not str(freeze.get('clears_on') or '').strip()):
            return 'FREEZE_GATE: invalid switch'
        ex = row.get('freeze_exempt')
        if not isinstance(ex, dict) or any(not isinstance(ex.get(k), str) or not ex[k].strip()
                for k in ('blocks','blocked_step','failure_receipt','declared_by','declared_at')):
            return 'FREEZE_GATE: missing_exempt fields'
        if ex['blocks'] not in FREEZE_BLOCKS or freeze_stamp(ex['declared_at']) is None:
            return 'FREEZE_GATE: invalid blocks/declared_at'
        registry = release_steps(root)
        if ex['blocked_step'] not in registry:
            return 'FREEZE_GATE: unknown blocked_step'
        path = freeze_path(root, ex['failure_receipt'])
        if not path.is_file() or path.stat().st_size == 0:
            return 'FREEZE_GATE: failure_receipt missing or empty'
        data = receipt_data(path)
        if data.get('blocked_step') and data['blocked_step'] != ex['blocked_step']:
            return 'FREEZE_GATE: receipt step mismatch'
        if str(data.get('status') or data.get('verdict') or '').upper() == 'PASS':
            return 'FREEZE_GATE: stale_exempt PASS is not failure evidence'
        decision_receipt = data.get('kind') == 'decision'
        if path.suffix == '.md':
            with path.open() as stream:
                header = stream.read(256)
            decision_receipt = header.startswith('advisor/log.jsonl') or header.startswith('# Fable')
        now = time.time() if now is None else now
        if not decision_receipt and not 0 <= now - path.stat().st_mtime <= 86400:
            return 'FREEZE_GATE: failure_receipt expired'
        record = registry[ex['blocked_step']]
        if receipt_pass(root, record.get('pass_receipt'), ex['blocked_step']):
            return 'FREEZE_GATE: stale_exempt'
        incumbents = set(record.get('incumbents') or []) & PREFLIGHT_INCUMBENTS if ex['blocked_step']=='recut.preflight' else set()
        def incumbent(name, ticket):
            declaration = freeze_stamp((ticket.get('freeze_exempt') or {}).get('declared_at'))
            return name in incumbents and declaration is not None and declaration <= freeze_stamp(freeze['since'])
        groups = [(None, board.get('tickets') or {})] + [(b, body.get('tickets') or {})
                  for b, body in (board.get('batches') or {}).items()]
        for bn, rows in groups:
            for other, ticket in rows.items():
                if other == tid and (batch is None or bn == batch):
                    continue
                same = (ticket.get('freeze_exempt') or {}).get('blocked_step') == ex['blocked_step']
                if (same and ts.derive_state(ticket) not in ts.TERMINAL | {ts.FROZEN_GATE}
                        and not (incumbent(tid, row) and incumbent(other, ticket))):
                    return 'FREEZE_GATE: step_busy ' + ex['blocked_step'] + ' by ' + other
        return None
    except (OSError, ValueError, TypeError, AttributeError) as exc:
        return 'FREEZE_GATE: cannot establish admission: ' + str(exc)


def canonical_class(name):
    """Keep the access mode while sharing the resource-lock alias authority."""
    return canonical_lock(name) + (":shared" if name.endswith(":shared") else "")


def decision(values, reason="scope declaration"):
    classes = set()
    for value in values:
        if isinstance(value, str):
            value = [item.strip() for item in value.split(",")]
        if (
            not isinstance(value, list)
            or not value
            or any(
                not isinstance(item, str) or (
                    item not in SAFE | CONFLICT
                    and not (item.removesuffix(':shared') in SAFE
                             and canonical_class(item) in SAFE)
                )
                for item in value
            )
        ):
            return {"classes": [], "preconfirmed": False, "reason": "invalid " + reason}
        classes.update(canonical_class(item) for item in value)
    return {
        "classes": sorted(classes),
        "preconfirmed": bool(classes) and classes <= SAFE,
        "reason": reason if classes else "undeclared scope",
    }


def header(path, review=False):
    """Read only metadata before the first level-two/body heading."""
    name = "Board-Freeze-Review-Classes:" if review else "Board-Freeze-Classes:"
    values = []
    for line in Path(path).read_text().splitlines():
        if line.startswith("## "):
            break
        if line.startswith(name):
            values.append(line[len(name) :].strip())
    return values


# Remote target patterns are project-specific; the public defaults match nothing.
TARGET_REMOTE = re.compile(r'(?!x)x')
WRITE_REMOTE = re.compile(r'(?!x)x')
WRITE_LOCK_REMOTE = re.compile(r'(?!x)x')

WRITE_LOCK = re.compile(r'^Write-Lock:\s*([a-z0-9:-]+)(?=$|[\s（(])', re.M)
VALID_WRITE_LOCKS = frozenset({
    })


def impact_decision(result, paths, *, impact_paths=()):
    """Only explicit impact declarations and structured target/action fields grant scope.
    Contract prose is documentation, not a write-intent signal.
    """
    impact_paths = {Path(p) for p in impact_paths}
    for path in paths:
        # Contracts append the shared rules, which mention remote writes generically and
        # would flag every ticket; only the ticket's own text declares its scope.
        text = Path(path).read_text().split("\n## 通用规则", 1)[0]
        declared = WRITE_LOCK.findall(text)
        if len(declared) != len(re.findall(r'^Write-Lock:', text, re.M)) or any(name not in VALID_WRITE_LOCKS for name in declared):
            return {**result, 'preconfirmed': False, 'reason': 'invalid Write-Lock declaration'}
        if Path(path) in impact_paths:
            write_scope = text
        else:
            write_scope = '\n'.join(line for line in text.splitlines() if re.match(
                r'^(?:Target(?:s)?|Write-Target|Target-Environment|Operation|Action|目标|目标环境|操作|写入目标)\s*[:：]', line, re.I))
        return {
            "classes": [],
            "preconfirmed": False,
            "reason": "unreadable scope declaration",
        }


def active(row):
    status = str(
        row.get("state", row.get("last_status", row.get("status", "UNKNOWN")))
    ).upper()
    return (
        row.get("terminal") is not True
        and status not in TERMINAL
        and not (
            status.startswith(("WAITING", "WAIT_"))
            or status in {"PENDING", "READY", "BLOCKED", "PAUSED"}
        )
    )


def classify_active(ticket, board, watch):
    """A REVIEWING parent represents its review only after its executor stopped."""
    rows = rows_for(board, ticket)
    if len(rows) == 1 and str(rows[0].get("state", "")).upper() == "REVIEWING":
        executor = watch.get("tracked", {}).get(ticket)
        # Missing executor evidence cannot prove local execution has stopped.
        if executor is not None and not active(executor):
            return classify(ticket + "-review-spec", board, watch)
    return classify(ticket, board, watch)


def action_key(ticket, thread, board, batch):
    rows = rows_for(board, ticket, batch)
    if len(rows) == 1:
        for axis in ("spec", "standards"):
            if rows[0].get("reviews", {}).get(axis, {}).get("thread") == thread:
                return ticket + "-review-" + axis
    return ticket


def append(path, row):
    with path.open("a") as stream:
        stream.write(json.dumps(row, ensure_ascii=False) + "\n")


def freeze_scope(row, paths):
    """Headers also work before a proposed dispatch has a run.json row."""
    result = dict(row)
    for key, names in (('target_environment', ('Board-Freeze-Environment:', 'Target-Environment:')),
                       ('chain', ('Board-Freeze-Chain:',))):
        values = {str(result[key])} if result.get(key) else set()
        if key == 'target_environment' and row.get('environment'):
            values.add(str(row['environment']))
        for path in paths:
            for line in Path(path).read_text().splitlines():
                if line.startswith('## '):
                    break
                for name in names:
                    if line.startswith(name):
                        values.add(line[len(name):].strip())
        # Conflicting declarations cannot prove a dispatch is independent.
        result[key] = next(iter(values)) if len(values) == 1 else None
    result.pop('environment', None)
    return result


def freeze_blocks(result, marker, row):
    """Intersect the declared conflict class, environment and chain.

    Missing ticket scope cannot prove independence. Old markers retain the
    reviewed local conflict classes; exempt lists still grant nothing.
    """
    classes = set(decision([result['classes']])['classes'])
    if not classes or result['reason'].startswith(('invalid', 'unreadable', 'ambiguous', 'undeclared')):
        return True
    frozen = marker.get('conflict_classes', marker.get('classes', sorted(CONFLICT)))
    declaration = decision([frozen], 'freeze classes')
    if not declaration['classes']:
        return True
    if not any(canonical_lock(left) == canonical_lock(right)
               and not (left.endswith(':shared') and right.endswith(':shared'))
               for left in classes for right in declaration['classes']):
        return False
    for marker_key, row_keys in (('environment', ('target_environment', 'environment')),
                                 ('chain', ('chain',))):
        wanted = marker.get(marker_key)
        actual = next((row.get(k) for k in row_keys if row.get(k)), None)
        # Environment and chain names are project-defined labels (local, staging, host-a, ...).
        valid = lambda v: isinstance(v, str) and bool(re.fullmatch(r'[A-Za-z0-9_.-]+', v))
        if wanted and actual and valid(wanted) and valid(actual) and wanted != actual:
            return False
    return True


def check_run(run, ticket, action, batch=None, contract=None, requires=None):
    impact = run / "control/board-impact" / (ticket + ".md")
    impact = impact if impact.exists() else None
    try:
        board = read_json(run / "run.json")
    except (OSError, ValueError):
        board = {}
    base_ticket = REVIEW.fullmatch(ticket)[1] if REVIEW.fullmatch(ticket) else ticket
    rows = rows_for(board, base_ticket, batch)
    if (run / 'control/freeze-gate').exists() and len(rows) != 1:
        print('FREEZE_GATE: missing or ambiguous registered ticket', file=sys.stderr)
        return 3
    problem = freeze_gate_problem(run, board, base_ticket, rows[0], batch) if len(rows)==1 else None
    if problem:
        print(problem, file=sys.stderr)
        return 3
    result = classify(ticket, board, batch=batch, contract=contract, impact=impact)
    marker = run / "control/locks/board-freeze.json"
    # Parent-owned metadata cutover is an explicit dispatch freeze; it does
    # not depend on a GATE supervisor. Only its explicit thaw releases it.
    if marker.exists():
        try:
            metadata_freeze = read_json(marker)
        except (OSError, ValueError):
            print("BOARD-FREEZE: unreadable marker", file=sys.stderr)
            return 3
        if metadata_freeze.get("kind") == "hot-paths-seg1":
            print("BOARD-FREEZE: metadata cutover holds all dispatch", file=sys.stderr)
            return 3
    probe = subprocess.run(
        [
            "pgrep",
            "-f",
            "BATCH-GATE-CHAIN/run-0820/continuation-[0-9]*/control/supervisor.py",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if probe.returncode == 1:
        if marker.exists():
            append(
                run / "control/board-freeze-stale-warnings.jsonl",
                {
                    "at": time.time(),
                    "ticket": ticket,
                    "action": action,
                    "marker": str(marker),
                    "warning": "board-freeze marker present but GATE supervisor not running; marker treated as stale (not deleted)",
                },
            )
            print(
                "BOARD-FREEZE: stale marker ignored (supervisor not running); warning logged",
                file=sys.stderr,
            )
        return 0
    # A live supervisor freezes dispatch even while it is creating the marker.
    # An unsuccessful process probe must not be mistaken for an idle supervisor.
    try:
        board = read_json(run / "run.json")
        result = classify(ticket, board, batch=batch, contract=contract, impact=impact)
        if requires:
            result = decision(
                [result["classes"], requires], "scope and wrapper requirements"
            )
    except (OSError, ValueError):
        result = decision([], "unreadable board")
    try:
        freeze = read_json(marker) if marker.exists() else {}
        rows = rows_for(board, ticket, batch)
        # Review keys use the same chain/environment as their implementation.
        if not rows and REVIEW.fullmatch(ticket):
            rows = rows_for(board, REVIEW.fullmatch(ticket)[1], batch)
        row = rows[0] if len(rows) == 1 else {}
        paths = [p for p in (row.get('scope'), contract, impact) if p]
        row = freeze_scope(row, paths)
        blocked = freeze_blocks(result, freeze, row)
    except (OSError, ValueError, TypeError, AttributeError):
        blocked = True
    if not blocked and probe.returncode == 0:
        print(json.dumps({"ticket": ticket, "action": action, **result}))
        return 0
    queue = run / "control/board-freeze-queue.jsonl"
    append(
        queue,
        {
            "at": time.time(),
            "ticket": ticket,
            "action": action,
            "batch": batch,
            "contract": str(contract) if contract else None,
            **result,
            "reason": "board-freeze active: " + result["reason"],
            "status": "QUEUED",
            "supervisor_probe_exit": probe.returncode,
        },
    )
    print(f"BOARD-FREEZE: {ticket} {action} queued (see {queue})", file=sys.stderr)
    return 3


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--ticket", required=True)
    parser.add_argument("--batch")
    parser.add_argument("--contract", type=Path)
    parser.add_argument("--action", default="dispatch")
    parser.add_argument("--requires", action="append", choices=sorted(CONFLICT))
    parser.add_argument(
        "--thread", help="Resolve registered review thread to its action key; no writes"
    )
    args = parser.parse_args()
    try:
        if args.thread:
            board = read_json(args.run / "run.json")
            print(action_key(args.ticket, args.thread, board, args.batch))
            return 0
        return check_run(
            args.run, args.ticket, args.action, args.batch, args.contract, args.requires
        )
    except (OSError, ValueError, TypeError, AttributeError) as error:
        print(
            f"BOARD-FREEZE: cannot establish admission: {type(error).__name__}",
            file=sys.stderr,
        )
        return 3


if __name__ == "__main__":
    sys.exit(main())

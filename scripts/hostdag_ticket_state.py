"""One explicit state per Host DAG ticket, and the transitions between two run.json snapshots.

Design: docs/local-dev/host-dag-ticket-state-machine-20261015.md (Flink mapping: run = cluster,
批次 = Job, 票 = Task, 轮次 = Execution attempt, named resource locks = Slots).

The coordinator writes ``state`` (and ``attempt``, ``accept_scope``) through ``host-dag ticket set``.
Tickets written before that, or by a coordinator that only sets the old fields, get a state
derived from ``status`` / ``outcome`` / ``blocker`` / ``reviews`` / ``deps``; nothing is migrated.
"""

from __future__ import annotations

import os
import re
from typing import Any

# Shared chain membership for resource-lock priority and DAG lint.
CORE_CHAINS = frozenset(c.strip() for c in os.environ.get('HOST_DAG_CORE_CHAINS', '').split(',') if c.strip())

FROZEN_GATE = "FROZEN_GATE"
DEGRADING = "DEGRADING"
READY_DEGRADE_A = "READY_DEGRADE_A"
CANDIDATE_BLOCKED = "CANDIDATE_BLOCKED"
DEGRADE_STATES = frozenset({DEGRADING, READY_DEGRADE_A, CANDIDATE_BLOCKED})
CREATED = "CREATED"
WAITING_DEPENDENCY = "WAITING_DEPENDENCY"
WAITING_RESOURCE = "WAITING_RESOURCE"
RUNNING = "RUNNING"
REVIEWING = "REVIEWING"
NEEDS_USER = "NEEDS_USER"
EXTERNALLY_BLOCKED = "EXTERNALLY_BLOCKED"
ACCEPTED = "ACCEPTED"
INTEGRATED = "INTEGRATED"
FAILED = "FAILED"
CANCELED = "CANCELED"

STATES = (DEGRADING, READY_DEGRADE_A, CANDIDATE_BLOCKED, FROZEN_GATE, CREATED, WAITING_DEPENDENCY, WAITING_RESOURCE, RUNNING, REVIEWING, NEEDS_USER,
          EXTERNALLY_BLOCKED, ACCEPTED, INTEGRATED, FAILED, CANCELED)
DONE = frozenset({ACCEPTED, INTEGRATED})
TERMINAL = frozenset({ACCEPTED, INTEGRATED, FAILED, CANCELED})
WAITING = frozenset({WAITING_DEPENDENCY, WAITING_RESOURCE})
SCOPES = ("FULL", "SCOPED", "REDUCED")
SCOPE_LABEL = {"FULL": "完整", "SCOPED": "限定", "REDUCED": "缩减"}

ALIASES = {"CANCELLED": CANCELED, "SUPERSEDED": CANCELED, "DONE": ACCEPTED, "CLOSED": ACCEPTED}
WAIT_KIND = re.compile(r"WAIT-(USER|RESOURCE|DEPENDENCY)\b")
WAIT_STATE = {"USER": NEEDS_USER, "RESOURCE": WAITING_RESOURCE, "DEPENDENCY": WAITING_DEPENDENCY}


def _upper(value: object) -> str:
    return str(value or "").strip().upper()


def explicit_state(ticket: dict[str, Any]) -> str | None:
    value = _upper(ticket.get("state"))
    value = ALIASES.get(value, value)
    return value if value in STATES else None


def all_tickets(run: dict[str, Any]) -> dict[str, Any]:
    """Resolve ids across all batches, including closed/hidden batches."""
    return {name: rec for body in (run.get("batches") or {}).values()
            for name, rec in ((body or {}).get("tickets") or {}).items()}


def dependency_status(ticket: dict[str, Any], tickets: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Unaccepted and missing prerequisites; never recurse through dependency cycles."""
    deps = ticket.get("deps") or []
    missing = [d for d in deps if not isinstance(tickets.get(d), dict)]
    pending = [d for d in deps if d in missing or derive_state(tickets[d]) not in DONE]
    return pending, missing


def derive_state(ticket: dict[str, Any], tickets: dict[str, Any] | None = None) -> str:
    """Written state, with CREATED gated by dependencies; otherwise derive the legacy fields."""
    written = explicit_state(ticket)
    if written:
        if written == CREATED and tickets is not None and dependency_status(ticket, tickets)[0]:
            return WAITING_DEPENDENCY
        return written
    status = _upper(ticket.get("status"))
    if status in {"ACCEPTED", "DONE", "CLOSED"}:
        return ACCEPTED
    if status in {"SUPERSEDED", "CANCELLED", "CANCELED"}:
        return CANCELED
    if status == "FAILED":
        return FAILED
    outcome = _upper(ticket.get("outcome"))
    if outcome == "NEEDS_DECISION":
        return NEEDS_USER
    if outcome == "EXTERNALLY_BLOCKED":
        return EXTERNALLY_BLOCKED
    if outcome == "READY_FOR_ACCEPTANCE":
        return REVIEWING
    blocker = str(ticket.get("blocker") or "")
    kind = WAIT_KIND.search(blocker)
    if kind:
        return WAIT_STATE[kind.group(1)]
    if blocker.startswith("WAIT-UNTIL:"):
        return WAITING_RESOURCE  # a time window is a slot too
    if tickets is not None and dependency_status(ticket, tickets)[0]:
        return WAITING_DEPENDENCY
    if status == "CLAIMED":
        reviews = ticket.get("reviews") or {}
        if reviews and any(not (r or {}).get("verdict") for r in reviews.values()):
            return REVIEWING
        return RUNNING
    return CREATED


def attempt_of(ticket: dict[str, Any]) -> int:
    """Executions actually dispatched. Old tickets have only ``round``, which also counts admission bounces."""
    for key in ("attempt", "round"):
        try:
            if ticket.get(key) is not None:
                return max(0, int(ticket[key]))
        except (TypeError, ValueError):
            continue
    return 0


def accept_scope(ticket: dict[str, Any]) -> str | None:
    scope = _upper(ticket.get("accept_scope"))
    return scope if scope in SCOPES else None


def snapshot(run: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """``{"<batch>/<ticket>": {...}}`` for every batch ticket: small enough to keep in watch.json."""
    out: dict[str, dict[str, Any]] = {}
    everything = all_tickets(run)
    batches = run.get("batches")
    for batch, body in (batches.items() if isinstance(batches, dict) else ()):
        tickets = (body or {}).get("tickets") or {}
        if not isinstance(tickets, dict):
            continue
        for name, rec in tickets.items():
            if not isinstance(rec, dict):
                continue
            out[f"{batch}/{name}"] = {
                "state": derive_state(rec, everything), "attempt": attempt_of(rec), "scope": accept_scope(rec),
                "explicit": explicit_state(rec) is not None, "title": rec.get("title") or "",
                "blocker": rec.get("blocker") or "", "evidence": rec.get("evidence") or "",
            }
    return out


def transitions(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Every ticket whose state changed (a ticket new in ``after`` comes from None)."""
    changes = []
    for key, now in after.items():
        old = (before.get(key) or {}).get("state")
        if old != now["state"]:
            batch, _, ticket = key.partition("/")
            changes.append({"batch": batch, "ticket": ticket, "from": old, "to": now["state"], **{
                k: now[k] for k in ("attempt", "scope", "title", "blocker", "evidence")}})
    return changes


def batch_states(snap: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    batches: dict[str, list[str]] = {}
    for key, rec in snap.items():
        batches.setdefault(key.partition("/")[0], []).append(rec["state"])
    return batches


def batch_finished(states: list[str]) -> bool:
    """Every ticket ACCEPTED/INTEGRATED, canceled ones aside, and at least one accepted."""
    live = [s for s in states if s != CANCELED]
    return bool(live) and all(s in DONE for s in live)


def finished_batches(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]) -> list[str]:
    """Batches that became finished between the two snapshots."""
    was = {b for b, s in batch_states(before).items() if batch_finished(s)}
    return sorted(b for b, s in batch_states(after).items() if batch_finished(s) and b not in was)


# ---------------------------------------------------------------- dry run


def _iso(value: object) -> float | None:
    import datetime as dt
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def replay(run: dict[str, Any], alerts: list[dict[str, Any]], since: float) -> list[dict[str, Any]]:
    """What the state-driven watcher would have pushed since ``since``, rebuilt from one run.json.

    run.json keeps no history, so ACCEPTED entries come from ``accepted_at`` and batch results from
    the last acceptance in a finished batch. Coordinator alerts go through the wording fallback with
    the tickets the state machine already covered at that moment, exactly as alert() decides live.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from hostdag_self_heal import telegram_worthy, ACCEPTED_TITLE

    pushes: list[dict[str, Any]] = []
    accepted: dict[str, float] = {}
    for batch, body in (run.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        stamps = []
        for name, rec in tickets.items():
            at = _iso(rec.get("accepted_at")) if derive_state(rec, tickets) in DONE else None
            if at:
                accepted[name] = at
                stamps.append(at)
                if at >= since:
                    label = SCOPE_LABEL.get(accept_scope(rec) or "")
                    pushes.append({"at": at, "via": "state", "key": f"accepted:{name}",
                                   "title": f"{name} 已验收" + (f"（{label}）" if label else "")})
            if derive_state(rec, tickets) == NEEDS_USER:
                pushes.append({"at": _iso(rec.get("updated_at")) or since, "via": "state",
                               "key": f"needs_user:{name}:1", "title": f"{name} 需要你决定（当前）"})
    sent: set[str] = {p["key"] for p in pushes}
    for rec in alerts:
        at = float(rec.get("at") or 0)
        if at < since or not str(rec.get("key", "")).startswith("coordinator:"):
            continue
        covered = {n for n, t in accepted.items() if t <= at}
        title = str(rec.get("title") or "")
        if ACCEPTED_TITLE.search(title) and any(name in title for name in covered):
            continue  # already represented by the receipt acceptance event
        if not telegram_worthy(rec["key"], title, covered, states=snapshot(run)):
            continue
        key = str(rec["key"]).removeprefix("coordinator:")
        if any(key == k or key == f"coordinator:{k}" for k in sent):
            continue
        sent.add(key)
        pushes.append({"at": at, "via": "wording-fallback", "key": key, "title": title})
    return sorted(pushes, key=lambda p: p["at"])


def main(argv: list[str] | None = None) -> int:
    import argparse
    import json
    import time
    p = argparse.ArgumentParser(description="Ticket state machine: show states, diff snapshots, replay a day.")
    sub = p.add_subparsers(dest="cmd", required=True)
    sh = sub.add_parser("show", help="derived state of every batch ticket")
    sh.add_argument("run")
    df = sub.add_parser("diff", help="transitions and finished batches between two run.json files")
    df.add_argument("before")
    df.add_argument("after")
    rp = sub.add_parser("replay", help="what state-driven Telegram would have pushed since a date")
    rp.add_argument("run")
    rp.add_argument("--alerts", help="alerts.jsonl beside the run (default: next to run.json)")
    rp.add_argument("--since", required=True, help="ISO date/time, e.g. 2026-10-15")
    args = p.parse_args(argv)

    def load(path: str) -> dict[str, Any]:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)

    if args.cmd == "show":
        for key, rec in snapshot(load(args.run)).items():
            print(f"{key:28} {rec['state']:20} attempt={rec['attempt']}{'' if rec['explicit'] else ' (derived)'}")
    elif args.cmd == "diff":
        before, after = snapshot(load(args.before)), snapshot(load(args.after))
        print(json.dumps({"transitions": transitions(before, after),
                          "finished_batches": finished_batches(before, after)}, ensure_ascii=False, indent=1))
    else:
        import os
        alerts_path = args.alerts or os.path.join(os.path.dirname(os.path.abspath(args.run)), "alerts.jsonl")
        alerts = []
        try:
            with open(alerts_path, encoding="utf-8") as handle:
                for line in handle:
                    try:
                        alerts.append(json.loads(line))
                    except ValueError:
                        continue
        except OSError:
            pass
        since = _iso(args.since if "T" in args.since else args.since + "T00:00:00")
        if since is None:
            p.error(f"--since: not an ISO time: {args.since}")
        for push in replay(load(args.run), alerts, since):
            stamp = time.strftime("%m-%d %H:%M", time.localtime(push["at"]))
            print(f"{stamp}  {push['via']:16} {push['key']:40} {push['title']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

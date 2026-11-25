"""Host DAG controller, stage B3 (shadow): reconcile, shadow outbox, board diff, projection, health.

Design: docs/local-dev/dag-state-machine-design-20261019.md §2 (controller), §3.4 (health), §6
(pending/outbox), §7.1 (audit export, projection), §8 stage B (shadow, shadow-diff), §13 (runtime,
per-cycle connections, statvfs guard, quiesce, backup).

One cycle (``Controller.cycle``), every 30 seconds:
1. open a fresh connection to ``<run>/sm/state.db`` (never held across cycles, §13.3);
2. run the B2 resolvers on the core's World and the run's files, plus the controller's own health
   facts (executor_unknown after a 10-minute grace), and submit them as facts, one transaction each;
3. drain the outbox in SHADOW: compute what the program would do for every open outbox row and
   every outstanding decision (launch / dispatch / resume / deliver / nudge / escalate) and only
   *record* it in ``sm/outbox-shadow.jsonl``;
4. reconcile the core against the live board (run.json through ``hostdag_ticket_state.snapshot``,
   watch.json ``tracked``) and record diffs in ``sm/shadow-diff.jsonl`` (the 24 h zero-diff gate);
5. export new audit events, write ``sm/projection.json`` and ``sm/heartbeat.json``, back up.

Shadow means: no Telegram, no thread send/start, no ``ticket set``, no page-window launch, no
run.json write. This module imports no process, network or notification API, and every file it
writes goes through ``SmFiles``, which refuses any path outside ``<run>/sm/`` (and the optional
backup directory). The core's own mode (``meta.mode``) never turns real delivery on here: a core
mode database makes the controller record an alert and deliver nothing (real delivery is stage C).
"""

from __future__ import annotations

from hostdag_json import public_path

import argparse
import fcntl
import hashlib
import json
import os
import signal
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator, Mapping, Sequence

import hostdag_dag_core as c
import hostdag_dag_resolvers as r
import hostdag_ticket_state as ts
from hostdag_dag_core import Json, JsonObj

SM_DIR = "sm"
DB_NAME = "state.db"
PROJECTION = "projection.json"
HEARTBEAT = "heartbeat.json"
STATE = "controller-state.json"
LOCK = "controller.lock"
DIFF_LOG = "shadow-diff.jsonl"
OUTBOX_LOG = "outbox-shadow.jsonl"
NOTES_LOG = "notes.jsonl"
ALERTS_LOG = "alerts.jsonl"
AUDIT_LOG = "audit.jsonl"
PROJECTION_SCHEMA = "host-dag-dag-sm/projection/1"

INTERVAL_SECONDS = 30
EXECUTOR_UNKNOWN_GRACE_SECONDS = 10 * 60  # §3.4: unknown > 10 min -> executor_unknown decision
NUDGE_SECONDS = 20 * 60  # §6.1: a decision is nudged every 20 minutes, escalated after NUDGE_LIMIT
BACKUP_SECONDS = 10 * 60  # §13.3 optional disaster copy to the internal disk
SLOW_CYCLE_SECONDS = 10.0  # §13.4 B3: a cycle whose submits take longer is a disk alert
SLOW_SUBMIT_SECONDS = 5.0  # §13.3: one commit above 5 s is a disk alert
DEFAULT_BACKUP_ROOT = Path.home() / ".local/state/host-dag-dag"
WATCH_REVIEW_MARK = "-review-"  # hostdag_dag_view.WATCH_REVIEW_MARK (not imported: that module spawns processes)
FREE_PAGE_STATUSES = ("RESUMED", "HOLD")  # local-dev/pagewindow/locks.FREE_STATUSES


class ControllerError(Exception):
    """A controller command could not do what it was asked (the message says why)."""


# ---------------------------------------------------------------- files (the only writer)


_OPEN_FLAGS = os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


class SmFiles:
    """Every write of the controller. Paths are checked against the allowed roots before any I/O,
    so a bug that builds a path into run.json, control/ or the advisor log fails instead of writing.

    ``sm_dir`` must be a real directory named by its own path: a symlinked ``sm`` (to elsewhere, or
    back to the run dir) is refused on every write (review F4), and files are opened O_NOFOLLOW."""

    def __init__(self, sm_dir: Path, extra_roots: Sequence[Path] = ()) -> None:
        self.sm_dir = Path(sm_dir)
        self.roots = [self.sm_dir.parent.resolve() / self.sm_dir.name, *(Path(p).resolve() for p in extra_roots)]

    def check_dir(self) -> None:
        sm = self.sm_dir
        if sm.is_symlink():
            raise ControllerError(f"refusing to write through {sm}: it is a symlink")
        if sm.exists() and sm.resolve() != sm.parent.resolve() / sm.name:
            raise ControllerError(f"refusing to write through {sm}: it resolves to {sm.resolve()}")

    def path(self, name: str | Path) -> Path:
        self.check_dir()
        target = Path(name) if Path(name).is_absolute() else self.sm_dir / name
        resolved = target.resolve()
        if not any(resolved == root or resolved.is_relative_to(root) for root in self.roots):
            raise ControllerError(f"refusing to write {resolved}: outside {[str(x) for x in self.roots]}")
        return target

    def write_json(self, name: str | Path, data: Json) -> None:
        target = self.path(name)
        target.parent.mkdir(parents=True, exist_ok=True)
        self.check_dir()
        tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        tmp.unlink(missing_ok=True)
        fd = os.open(tmp, _OPEN_FLAGS | os.O_WRONLY | os.O_EXCL, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
        os.replace(tmp, target)

    def append(self, name: str, rows: Sequence[JsonObj]) -> None:
        """Append whole lines. A torn tail left by a crash mid-append (no final newline) is cut back
        to the last newline first (review F2), so the next line is never glued to a fragment."""
        if not rows:
            return
        target = self.path(name)
        data = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows).encode("utf-8")
        fd = os.open(target, _OPEN_FLAGS | os.O_RDWR | os.O_APPEND, 0o644)
        try:
            size = os.fstat(fd).st_size
            if size and os.pread(fd, 1, size - 1) != b"\n":
                os.ftruncate(fd, _last_newline_end(fd, size))
            while data:
                data = data[os.write(fd, data):]
        finally:
            os.close(fd)

    def read_json(self, name: str) -> JsonObj | None:
        try:
            data = json.loads((self.sm_dir / name).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def jsonl(self, name: str) -> list[JsonObj]:
        """Whole lines only: a torn last line from a crash is ignored."""
        try:
            text = (self.sm_dir / name).read_text(encoding="utf-8")
        except OSError:
            return []
        rows: list[JsonObj] = []
        for raw in text.splitlines():
            try:
                row = json.loads(raw)
            except ValueError:
                continue
            if isinstance(row, dict):
                rows.append(row)
        return rows

    def last_seq(self, name: str) -> int:
        """seq of the last whole line of an audit export (0 when empty or missing). Reads backwards
        in blocks until a parsable whole line, so a last line of any size is found (review F5)."""
        try:
            handle = open(self.sm_dir / name, "rb")
        except OSError:
            return 0
        with handle:
            pos = handle.seek(0, os.SEEK_END)
            head = b""  # the start of the earliest line read so far (not known to be whole yet)
            torn = True  # the bytes after the last newline are not a whole line
            while pos > 0:
                step = min(65536, pos)
                pos -= step
                handle.seek(pos)
                parts = (handle.read(step) + head).split(b"\n")
                head, lines = parts[0], parts[1:]
                if torn and lines:
                    lines, torn = lines[:-1], False
                for raw in reversed(lines):
                    seq = _seq_of(raw)
                    if seq is not None:
                        return seq
            if not torn:
                seq = _seq_of(head)
                if seq is not None:
                    return seq
        return 0

    @contextmanager
    def lock(self, blocking: bool, timeout: float = 60.0) -> Iterator[bool]:
        """The controller lock: one cycle (or one quiesce checkpoint) at a time."""
        target = self.path(LOCK)
        fd = os.open(target, _OPEN_FLAGS | os.O_WRONLY | os.O_APPEND, 0o644)
        with os.fdopen(fd, "a", encoding="utf-8") as handle:
            got = False
            deadline = time.monotonic() + timeout
            while True:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    got = True
                    break
                except BlockingIOError:
                    if not blocking or time.monotonic() >= deadline:
                        break
                    time.sleep(0.1)
            try:
                yield got
            finally:
                if got:
                    fcntl.flock(handle, fcntl.LOCK_UN)


def _last_newline_end(fd: int, size: int) -> int:
    """Offset just after the last newline in the first ``size`` bytes of ``fd`` (0 when none)."""
    pos = size
    while pos > 0:
        step = min(65536, pos)
        pos -= step
        found = os.pread(fd, step, pos).rfind(b"\n")
        if found >= 0:
            return pos + found + 1
    return 0


def _seq_of(raw: bytes) -> int | None:
    try:
        row = json.loads(raw)
    except ValueError:
        return None
    seq = row.get("seq") if isinstance(row, dict) else None
    return seq if isinstance(seq, int) and not isinstance(seq, bool) else None


# ---------------------------------------------------------------- small helpers


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="microseconds")


def _epoch(iso: str) -> float:
    return c.parse_at(iso).timestamp()


def _digest(value: object) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _num(value: Json) -> float:
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0


def _read_json(path: Path) -> JsonObj | None:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def default_run_id(run_dir: Path) -> str:
    """The run's id: the run directory's name (``<runs>/<id>/local-dev-dag`` -> ``<id>``). B7 seeds
    state.db with the same id; pass --run-id when a run is laid out differently."""
    run_dir = public_path(run_dir)
    return run_dir.parent.name if run_dir.name == "local-dev-dag" else run_dir.name


def page_free(registry: JsonObj) -> bool:
    """locks.page_free, read only (the library keeps the only claim)."""
    return registry.get("owner") in (None, "") and registry.get("paused") is False \
        and registry.get("status") in FREE_PAGE_STATUSES


# ---------------------------------------------------------------- health facts


def health_ops(world: c.World, at: str, grace_seconds: int = EXECUTOR_UNKNOWN_GRACE_SECONDS) -> r.Resolution:
    """The controller's own facts. executor_unknown: one clock.observed carrying the grace, only when
    some current executor has been unknown for longer than the grace and has no decision for that
    episode yet (a steady state submits nothing). Unknown reviewers are reported, not decided."""
    out = r.Resolution()
    due = c.executor_unknown_due(world, at, grace_seconds)
    if due:
        payload: JsonObj = {"executor_unknown_after": grace_seconds,
                            "due": [[name, e.execution_id, e.observed_at] for name, e in due]}
        out.ops.append(r.fact_op("clock.observed", "controller", None, payload, at))
    now = _epoch(at)
    for name in sorted(world.tickets):
        t = world.tickets[name]
        if t.phase == c.CLOSED:
            continue
        current = t.current_executor()
        for e in t.executions:
            if e.observed != "unknown":
                continue
            age = now - _epoch(e.observed_at)
            if e is current and age < grace_seconds:
                out.note("health", "executor_unknown_grace", name, e.execution_id,
                         f"unknown since {e.observed_at}; executor_unknown after {grace_seconds // 60} min")
            elif e.role == "reviewer" and age >= grace_seconds:
                out.note("health", "reviewer_unknown", name, e.execution_id,
                         f"{e.axis} reviewer unknown since {e.observed_at}")
    return out


# ---------------------------------------------------------------- shadow outbox


@dataclass(frozen=True)
class WouldDo:
    """One thing the program would have done this cycle; ``key`` is stable for the same intent."""
    key: str
    verb: str
    ticket: str
    action_id: str
    kind: str
    detail: JsonObj

    def to_json(self, at: str) -> JsonObj:
        return {"at": at, "key": self.key, "verb": self.verb, "ticket": self.ticket, "action_id": self.action_id,
                "kind": self.kind, "detail": self.detail, "mode": "shadow"}


OUTBOX_VERB = {"resume": "would_thread_send", "request_reviews": "would_start_reviewer",
               "run_window": "would_run_window", "register_watch": "would_register_watch",
               "push": "would_push", "cleanup": "would_stop_session"}


def shadow_outbox(world: c.World, at: str, registry: JsonObj | None,
                  board_accepted: Mapping[str, str | None] | None = None) -> list[WouldDo]:
    """What delivery would do now, computed and never executed (§6.2, §5). Pure.

    run_window: the first eligible entry of the page queue launches only when the registry shows the
    page free; the others record why they wait (an eligible entry behind the head, or a busy page,
    is ``would_wait``). A run_window without spec_digest would launch unverified (B2 M4): the record
    says so. Decisions: deliver once, then a nudge every 20 minutes, escalate after NUDGE_LIMIT.
    HD-01: ready-idle tickets (``ready_idle_would``) and unstructured or due external blocks
    (``external_would``) follow the same cadence."""
    out: list[WouldDo] = []
    now = _epoch(at)
    free = registry is not None and page_free(registry)
    launched = False
    for name, p, reasons in c.page_queue(world, at):
        detail: JsonObj = {"entry_id": p.bound.get("entry_id"), "spec": p.payload.get("spec"),
                           "spec_digest": p.payload.get("spec_digest"), "priority": p.payload.get("priority")}
        why = list(reasons)
        if not why and not free:
            why.append("page registry not free" if registry is not None else "page registry unreadable")
        if not why and launched:
            why.append("behind the launched head of the queue")
        if not p.payload.get("spec_digest"):
            detail["warning"] = "no spec_digest: CLEAN would not continue automatically"
        verb = "would_wait" if why else OUTBOX_VERB["run_window"]
        if not why:
            launched = True
        detail["blockers"] = list(why)
        out.append(WouldDo(f"{p.action_id}:{verb}:{_digest(why)}", verb, name, p.action_id, p.kind, detail))
    for name in sorted(world.tickets):
        t = world.tickets[name]
        for p in t.pending:
            if p.mode == c.OUTBOX and p.open and p.kind != "run_window":
                verb = OUTBOX_VERB.get(p.kind, f"would_{p.kind}")
                detail = {"bound": dict(p.bound), "payload": p.payload}
                if p.kind == "push":
                    detail["telegram_key"] = p.payload.get("reason")
                out.append(WouldDo(f"{p.action_id}:{verb}", verb, name, p.action_id, p.kind, detail))
            elif p.mode == c.DECIDE and p.status in c.OPEN_PENDING:
                base: JsonObj = {"allowed_actions": list(p.allowed_actions), "bound": dict(p.bound),
                                 "created_at": p.created_at}
                out.append(WouldDo(f"{p.action_id}:would_deliver", "would_deliver", name, p.action_id, p.kind, base))
                age = now - _epoch(p.created_at)
                # A held decision (claimed with a reason) is not nudged until it is released.
                nudges = 0 if p.status == c.CLAIMED_P else min(int(age // NUDGE_SECONDS), c.NUDGE_LIMIT)
                for n in range(1, nudges + 1):
                    out.append(WouldDo(f"{p.action_id}:would_nudge:{n}", "would_nudge", name, p.action_id, p.kind,
                                       {**base, "nudge": n}))
                if nudges >= c.NUDGE_LIMIT:
                    out.append(WouldDo(f"{p.action_id}:would_escalate", "would_escalate", name, p.action_id, p.kind,
                                       base))
    out.extend(ready_idle_would(world, at, board_accepted))
    out.extend(external_would(world, at))
    return out


def _cadence(action_id: str, verb: str, ticket: str, kind: str, since: str, at: str, detail: JsonObj) -> list[WouldDo]:
    """One first intent, then a nudge every NUDGE_SECONDS after ``since`` and an escalation after
    NUDGE_LIMIT nudges: the same cadence as an undelivered decision. Keys carry ``action_id``, which
    names the episode, so a new episode starts over and an old one is never recorded twice."""
    out = [WouldDo(f"{action_id}:{verb}", verb, ticket, action_id, kind, detail)]
    nudges = max(0, min(int((_epoch(at) - _epoch(since)) // NUDGE_SECONDS), c.NUDGE_LIMIT))
    for n in range(1, nudges + 1):
        out.append(WouldDo(f"{action_id}:would_nudge:{n}", "would_nudge", ticket, action_id, kind, {**detail, "nudge": n}))
    if nudges >= c.NUDGE_LIMIT:
        out.append(WouldDo(f"{action_id}:would_escalate", "would_escalate", ticket, action_id, kind, detail))
    return out


READY_VERB = {"authorize": "would_authorize", "dispatch": "would_dispatch", "triage": "would_dispatch"}


def ready_idle_would(world: c.World, at: str, board_accepted: Mapping[str, str | None] | None = None,
                     idle_seconds: int = c.READY_IDLE_SECONDS) -> list[WouldDo]:
    """HD-01 A: a ticket that is ready and idle for ``idle_seconds`` (``hostdag_dag_core.ready_idle``)
    would be put in front of the coordinator (authorize a deferred ticket, dispatch / triage an
    authorized one), then nudged on the decision cadence. One episode per (ticket, readiness start)."""
    out: list[WouldDo] = []
    for hit in c.ready_idle(world, at, idle_seconds, board_accepted):
        flagged = r.iso_utc(_epoch(hit.since) + idle_seconds)
        detail: JsonObj = {"need": hit.need, "since": hit.since, "basis": hit.basis,
                           "board_only": list(hit.board_only), "why": list(hit.reasons)}
        action_id = f"ready:{hit.ticket}:{_digest([hit.since, hit.need, list(hit.board_only)])}"
        out.extend(_cadence(action_id, READY_VERB[hit.need], hit.ticket, "ready_idle", flagged, at, detail))
    return out


def external_would(world: c.World, at: str) -> list[WouldDo]:
    """HD-01 B: an external block must say who owns it, what we tried and when to look again. A wait
    missing any of these would be put to the advisor as a question at once ("is this really external,
    and what was tried?"); a structured one whose ``recheck_at`` passed would be put back to be
    re-judged. Episodes are per wait revision (a reworded block starts over)."""
    out: list[WouldDo] = []
    now = _epoch(at)
    for name in sorted(world.tickets):
        t = world.tickets[name]
        if t.phase == c.CLOSED:
            continue
        for w in t.open_waits():
            if w.kind != "external":
                continue
            base: JsonObj = {"wait_id": w.wait_id, "revision": w.revision, "ref": w.ref[:300],
                             "owner": w.detail.get("owner"), "attempts": w.detail.get("attempts"),
                             "recheck_at": w.detail.get("recheck_at"),
                             "legacy_import": bool(w.detail.get("legacy_import"))}
            gaps = c.external_gaps(w)
            if gaps:
                detail: JsonObj = {**base, "missing": list[Json](gaps),
                          "question": "external block without " + "/".join(gaps) + ": is it really outside us, "
                          "who owns it, what did we try, and when do we look again?"}
                revised = w.detail.get("revised_at")
                if w.revision == 1:
                    since = w.added_at
                else:  # revised before revised_at existed: the ticket's last change is the closest bound
                    since = revised if isinstance(revised, str) else t.updated_at
                out.extend(_cadence(f"ext:{w.wait_id}:r{w.revision}:unstructured", "would_ask", name,
                                    "external_unstructured", since, at, detail))
                continue
            recheck = str(w.detail["recheck_at"])
            if _epoch(recheck) <= now:
                detail = {**base, "question": f"recheck_at {recheck} passed: does the external block still hold?"}
                out.extend(_cadence(f"ext:{w.wait_id}:r{w.revision}:recheck:{_digest(recheck)}", "would_ask", name,
                                    "external_recheck", recheck, at, detail))
    return out


def legacy_capacity_would(notes: Sequence[r.Note]) -> list[WouldDo]:
    """HD-01 C: a legacy capacity wait (run.json ``wait_on capacity:...``) whose stated need a fresh
    probe sample meets would wake the coordinator once per (ticket, wait_on, need)."""
    out: list[WouldDo] = []
    for n in notes:
        if n.resolver == "capacity" and n.kind == "legacy_capacity_met" and n.ticket:
            action_id = f"legacy-capacity:{n.ticket}:{_digest([n.ref, n.detail])}"
            out.append(WouldDo(f"{action_id}:would_nudge", "would_nudge", n.ticket, action_id, "legacy_capacity_met",
                               {"wait_on": n.ref, "why": n.detail}))
    return out


def board_accepted_at(run: JsonObj | None) -> dict[str, str | None]:
    """ticket -> legacy accepted_at (None when not recorded) for tickets accepted on run.json."""
    board, _ = board_tickets(run)
    closures = r.legacy_closures({"batches": {"all": {"tickets": {n: rec for n, (_, rec) in board.items()}}}})
    out: dict[str, str | None] = {}
    for name, kind in closures.items():
        if kind != "accepted":
            continue
        rec = board[name][1]
        when = rec.get("accepted_at") or rec.get("updated_at")
        try:
            out[name] = when if isinstance(when, str) and c.parse_at(when) else None
        except c.CoreError:
            out[name] = None
    return out


# ---------------------------------------------------------------- board reconciliation (shadow diff)


CORE_CLASS = {"ACCEPTED": "accepted", "INTEGRATED": "accepted", "FAILED": "failed", "CANCELED": "canceled",
              "CREATED": "idle", "READY": "idle", "RUNNING": "running", "REVIEWING": "reviewing",
              "WAITING_DEPENDENCY": "waiting", "WAITING_RESOURCE": "waiting", "NEEDS_USER": "waiting",
              "EXTERNALLY_BLOCKED": "waiting"}
BOARD_CLASS = {ts.CREATED: "idle", ts.WAITING_DEPENDENCY: "waiting", ts.WAITING_RESOURCE: "waiting",
               ts.NEEDS_USER: "waiting", ts.EXTERNALLY_BLOCKED: "waiting", ts.RUNNING: "running",
               ts.REVIEWING: "reviewing", ts.ACCEPTED: "accepted", ts.INTEGRATED: "accepted",
               ts.FAILED: "failed", ts.CANCELED: "canceled"}


@dataclass(frozen=True)
class Diff:
    kind: str
    ticket: str
    core: Json
    board: Json

    @property
    def key(self) -> str:
        return f"{self.kind}:{self.ticket}"

    def to_json(self) -> JsonObj:
        return {"key": self.key, "kind": self.kind, "ticket": self.ticket, "core": self.core, "board": self.board}


def board_tickets(run: JsonObj | None) -> tuple[dict[str, tuple[str, JsonObj]], dict[str, list[str]]]:
    """(ticket -> (batch, record), ticket -> batches for a name found in more than one batch) from
    run.json batches (the live board until stage C). A duplicate keeps its first batch for the
    comparison and is reported, never silently overwritten (review F6)."""
    out: dict[str, tuple[str, JsonObj]] = {}
    seen: dict[str, list[str]] = {}
    batches = run.get("batches") if run is not None else None
    if not isinstance(batches, dict):
        batches = {}
    for batch, body in batches.items():
        tickets = body.get("tickets") if isinstance(body, dict) else None
        if isinstance(tickets, dict):
            for name, rec in tickets.items():
                if isinstance(rec, dict):
                    seen.setdefault(str(name), []).append(str(batch))
                    out.setdefault(str(name), (str(batch), rec))
    # Earlier runs keep pre-batch tickets at the top level. Preserve them under a named root batch.
    root_tickets = run.get("tickets") if run else None
    for name, rec in (root_tickets if isinstance(root_tickets, dict) else {}).items():
        if isinstance(rec, dict):
            seen.setdefault(str(name), []).append("__root__")
            out.setdefault(str(name), ("__root__", rec))
    return out, {n: b for n, b in seen.items() if len(b) > 1}


def watch_live(watch: JsonObj | None) -> set[str]:
    """Tickets whose executor the watcher tracks as running (same rule as hostdag_dag_view.mark_live)."""
    tracked = watch.get("tracked") if watch is not None else None
    if not isinstance(tracked, dict):
        return set()
    return {str(k) for k, rec in tracked.items()
            if isinstance(rec, dict) and not rec.get("terminal") and WATCH_REVIEW_MARK not in str(k)}


def core_class(t: c.Ticket, world: Mapping[str, c.Ticket]) -> str | None:
    """The comparison class of the core record. RUNNING only because reviewers run (REVIEW phase, no
    executor) is the board's REVIEWING (review F1)."""
    lab = c.label(t, world)
    if lab == "RUNNING" and t.phase == c.REVIEW and not t.active("executor"):
        return "reviewing"
    return CORE_CLASS.get(lab)


def board_diff(world: c.World, run: JsonObj | None, watch: JsonObj | None) -> list[Diff]:
    return board_compare(world, run, watch)[0]


def board_compare(world: c.World, run: JsonObj | None, watch: JsonObj | None) -> tuple[list[Diff], int]:
    """Core vs board, ticket by ticket. The board state is ``hostdag_ticket_state.snapshot`` of run.json
    (exactly what the watcher stores as watch.json ``ticket_states``), compared by class (idle, waiting,
    running, reviewing, accepted, failed, canceled) because the legacy vocabulary has no READY/CREATED
    split; closure kind (superseded vs canceled), attempt, deps, batch and executor liveness (watch.json
    ``tracked``) are compared exactly. Pure; sorted by key. Also returns how many tickets were present
    on both sides (``compared``; the 24 h gate needs it above zero)."""
    diffs: list[Diff] = []
    if run is None:
        return [Diff("board_unreadable", "*", None, "run.json unreadable")], 0
    board, duplicates = board_tickets(run)
    for name, where in sorted(duplicates.items()):
        diffs.append(Diff("duplicate_on_board", name, None, list(where)))
    compared = 0
    snap = ts.snapshot(run)
    live = watch_live(watch)
    for name in sorted(set(board) | set(world.tickets)):
        t = world.tickets.get(name)
        if name not in board:
            if t is not None:
                diffs.append(Diff("missing_on_board", name, c.label(t, world.tickets), None))
            continue
        batch, rec = board[name]
        if t is None:
            diffs.append(Diff("missing_in_core", name, None, snap.get(f"{batch}/{name}", {}).get("state")))
            continue
        compared += 1
        board_state = str(snap.get(f"{batch}/{name}", {}).get("state") or ts.derive_state(rec))
        core_label = c.label(t, world.tickets)
        if core_class(t, world.tickets) != BOARD_CLASS.get(board_state):
            diffs.append(Diff("state", name, core_label, board_state))
        core_closure = t.closure.kind if t.closure is not None else None
        # Either side closed (review F6); read from the compared record, not a later duplicate.
        board_closure = r.legacy_closures({"batches": {batch: {"tickets": {name: rec}}}}).get(name)
        if core_closure != board_closure:
            diffs.append(Diff("closure", name, core_closure, board_closure))
        board_attempt = ts.attempt_of(rec)
        if board_attempt != t.attempt:
            diffs.append(Diff("attempt", name, t.attempt, board_attempt))
        raw_deps = rec.get("deps")
        board_deps: list[Json] = [d for d in sorted(str(x) for x in raw_deps if isinstance(x, str))] \
            if isinstance(raw_deps, list) else []
        core_deps: list[Json] = [d for d in sorted(t.deps)]
        if board_deps != core_deps:
            diffs.append(Diff("deps", name, core_deps, board_deps))
        if batch != t.batch:
            diffs.append(Diff("batch", name, t.batch, batch))
        core_live = bool(t.active("executor"))
        if t.phase != c.CLOSED and core_live != (name in live):
            diffs.append(Diff("executor", name, "running" if core_live else "not running",
                              "tracked" if name in live else "not tracked"))
    return sorted(diffs, key=lambda d: d.key), compared


# ---------------------------------------------------------------- projection


def projection(world: c.World, at: str, applied_seq: int, notes: Sequence[r.Note],
               queue: Sequence[tuple[str, c.Pending, list[str]]]) -> JsonObj:
    """The core's view for humans and the coordinator; never read back by the controller."""
    tickets: JsonObj = {}
    labels: dict[str, int] = {}
    for name in sorted(world.tickets):
        t = world.tickets[name]
        lab = c.label(t, world.tickets)
        labels[lab] = labels.get(lab, 0) + 1
        tickets[name] = {
            "batch": t.batch, "title": t.title, "label": lab, "phase": t.phase, "scheduling": t.scheduling,
            "revision": t.revision, "attempt": t.attempt, "deps": list(t.deps),
            "unmet_deps": [d for d in t.deps if not c.accepted(world.tickets.get(d))],
            "waits": [{"wait_id": w.wait_id, "kind": w.kind, "ref": w.ref, "since": w.added_at, "note": w.note,
                       **({"external": {k: w.detail.get(k) for k in c.EXTERNAL_FIELDS},
                           "missing": list[Json](c.external_gaps(w))} if w.kind == "external" else {})}
                      for w in t.open_waits()],
            "executions": [{"execution_id": e.execution_id, "role": e.role, "axis": e.axis, "observed": e.observed,
                            "since": e.observed_at, "session": e.session}
                           for e in t.executions if e.observed in c.ACTIVE | {"unknown"}],
            "candidate": t.candidate.digest if t.candidate is not None else None,
            "closure": t.closure.kind if t.closure is not None else None,
            "pending": [{"action_id": p.action_id, "kind": p.kind, "mode": p.mode, "status": p.status,
                         "created_at": p.created_at, "allowed_actions": list(p.allowed_actions)}
                        for p in t.pending if p.status in c.OUTSTANDING],
            "legacy": c.legacy_projection(t, world.tickets)}
    return {"schema": PROJECTION_SCHEMA, "run_id": world.meta.run_id, "mode": world.meta.mode,
            "epoch": world.meta.authority_epoch, "applied_seq": applied_seq, "at": at,
            "read_only": "written by the B3 controller; edit nothing here", "paused": dict(world.meta.paused),
            "labels": dict(sorted(labels.items())), "tickets": tickets,
            "page_queue": [{"ticket": n, "action_id": p.action_id, "entry_id": p.bound.get("entry_id"),
                            "blockers": list(why)} for n, p, why in queue],
            "notes": [n.to_json() for n in notes]}


# ---------------------------------------------------------------- controller


@dataclass
class Config:
    run_dir: Path
    run_id: str
    backup_root: Path | None = DEFAULT_BACKUP_ROOT
    grace_seconds: int = EXECUTOR_UNKNOWN_GRACE_SECONDS
    alias_table: r.AliasTable | None = None
    statvfs: Callable[[str], r._Statvfs] = os.statvfs

    @property
    def sm_dir(self) -> Path:
        return Path(self.run_dir) / SM_DIR

    @property
    def db(self) -> Path:
        return self.sm_dir / DB_NAME

    @property
    def backup_dir(self) -> Path | None:
        return None if self.backup_root is None else Path(self.backup_root) / self.run_id


@dataclass
class CycleReport:
    at: str
    status: str  # ok | skipped | disk_low | error
    detail: str = ""
    submitted: int = 0
    accepted: int = 0
    rejected: list[str] = field(default_factory=list)
    would: int = 0
    diffs: int = 0
    notes: int = 0
    seconds: float = 0.0

    def to_json(self) -> JsonObj:
        return {"at": self.at, "status": self.status, "detail": self.detail, "submitted": self.submitted,
                "accepted": self.accepted, "rejected": list(self.rejected), "would": self.would,
                "diffs": self.diffs, "notes": self.notes, "seconds": round(self.seconds, 3)}


Crash = Callable[[str], None]  # test hook: called at every persistence boundary of a cycle
QUIESCED_SKIP = "skipped"


def _existing_ancestor(path: Path) -> Path:
    for candidate in (path, *path.parents):
        if candidate.exists():
            return candidate
    return Path("/")


def _no_crash(point: str) -> None:
    return None


class Controller:
    """The B3 shadow controller. Holds no connection between cycles."""

    def __init__(self, cfg: Config, clock: Callable[[], str] = now_iso, crash: Crash = _no_crash) -> None:
        self.cfg = cfg
        self.clock = clock
        self.crash = crash
        extra = [cfg.backup_dir] if cfg.backup_dir is not None else []
        self.files = SmFiles(cfg.sm_dir, extra)
        self.table = cfg.alias_table

    def present(self) -> bool:
        return (Path(self.cfg.run_dir) / "run.json").is_file() and self.cfg.db.is_file()

    def quiesced(self) -> bool:
        return (self.cfg.sm_dir / c.QUIESCE_MARKER).exists()

    def cycle(self) -> CycleReport:
        started = time.monotonic()
        at = self.clock()
        # Nothing is written (not even a heartbeat) unless the run disk and the database are there:
        # an unmounted USB disk must not grow a fresh sm/ tree under its mount point.
        if not self.present():
            return CycleReport(at, "skipped", f"run dir or {self.cfg.db} missing (disk unmounted or not seeded)")
        if self.quiesced():
            return CycleReport(at, QUIESCED_SKIP, "quiesced; run `resume` after re-plugging the run disk")
        try:
            with self.files.lock(blocking=False) as got:
                if not got:
                    return CycleReport(at, "skipped", "another controller holds sm/controller.lock")
                # Re-checked under the lock (review F3): quiesce may have finished between the check
                # above and taking the lock; it takes the same lock, so after this check it waits for us.
                if self.quiesced():
                    return CycleReport(at, QUIESCED_SKIP, "quiesced while waiting for the lock")
                report = self._locked_cycle(at, started)
        except Exception as exc:  # review F7: an unexpected failure still leaves an alert and a heartbeat
            return self._failed(at, started, exc)
        report.seconds = time.monotonic() - started
        return report

    def _failed(self, at: str, started: float, exc: Exception) -> CycleReport:
        report = CycleReport(at, "error", f"{type(exc).__name__}: {exc}", seconds=time.monotonic() - started)
        if self.quiesced() or not self.present():
            return report  # quiesced or the disk is gone: write nothing
        try:
            self.files.append(ALERTS_LOG, [{"at": at, "kind": "cycle_error", "detail": report.detail}])
            self.files.write_json(HEARTBEAT, self._heartbeat(at, report, self.files.read_json(STATE) or {}, None))
        except Exception as again:
            report.detail += f"; alert/heartbeat not written: {type(again).__name__}: {again}"
        return report

    def _locked_cycle(self, at: str, started: float) -> CycleReport:
        cfg, files = self.cfg, self.files
        state = files.read_json(STATE) or {}
        disk = r.disk_status(cfg.sm_dir, cfg.statvfs)
        if not disk.ok:
            # §13.3: below the floor nothing more goes to state.db or the projection; one alert per episode.
            if state.get("disk_ok", True):
                files.append(ALERTS_LOG, [{"at": at, "kind": "disk_low", "path": disk.path, "detail": disk.reason,
                                           "free_bytes": disk.free_bytes}])
                state["disk_ok"] = False
                files.write_json(STATE, state)
            return CycleReport(at, "disk_low", disk.reason)
        try:
            store = c.SqliteStore.open(cfg.db, cfg.run_id)
        except c.QuiescedError as exc:  # quiesce started after the check above: write nothing (F3)
            return CycleReport(at, QUIESCED_SKIP, str(exc))
        except c.StorageError as exc:
            files.append(ALERTS_LOG, [{"at": at, "kind": "storage", "detail": str(exc)}])
            files.write_json(HEARTBEAT, self._heartbeat(at, CycleReport(at, "error", str(exc)), state, disk))
            return CycleReport(at, "error", str(exc))
        report = CycleReport(at, "ok")
        alerts: list[JsonObj] = []
        if not state.get("disk_ok", True):
            alerts.append({"at": at, "kind": "disk_ok", "path": disk.path, "detail": "free space back"})
            state["disk_ok"] = True
        try:
            world = store.world()
            run = _read_json(Path(cfg.run_dir) / "run.json")
            watch = _read_json(Path(cfg.run_dir) / "watch.json")
            table = self.table if self.table is not None else r.AliasTable.load()
            res = r.reconcile(world, cfg.run_dir, at, table=table, legacy_run=run)
            res.extend(health_ops(world, at, cfg.grace_seconds))
            mode = world.meta.mode
            applied: list[r.Applied] = []
            submit_seconds = 0.0
            if mode == "legacy":
                res.note("controller", "legacy_mode", None, "meta.mode", "authority is legacy: facts not submitted")
            else:
                for op in res.ops:
                    t0 = time.monotonic()
                    applied.extend(r.apply(store, [op]))
                    spent = time.monotonic() - t0
                    submit_seconds += spent
                    if spent > SLOW_SUBMIT_SECONDS:
                        alerts.append({"at": at, "kind": "slow_commit", "op_id": op.op_id, "seconds": round(spent, 3)})
                    self.crash("after_submit")
            if submit_seconds > SLOW_CYCLE_SECONDS:
                alerts.append({"at": at, "kind": "slow_cycle", "submits": len(applied),
                               "seconds": round(submit_seconds, 3)})
            report.submitted = len(applied)
            report.accepted = sum(1 for a in applied if a.receipt is not None and a.receipt.status == "accepted")
            report.rejected = [f"{a.op.op_id}: {a.error}" for a in applied if a.error]
            world = store.world()
            events = store.events(files.last_seq(AUDIT_LOG))
            applied_seq = max([s for s, _, _, _ in events], default=files.last_seq(AUDIT_LOG))
            backup_due = cfg.backup_dir is not None and (
                _epoch(at) - _num(state.get("backup_epoch")) >= BACKUP_SECONDS)
            if backup_due and cfg.backup_dir is not None:
                self._backup(store, cfg.backup_dir, at, state, alerts)
        except (c.StorageError, OSError) as exc:
            files.append(ALERTS_LOG, [*alerts, {"at": at, "kind": "storage", "detail": f"{type(exc).__name__}: {exc}"}])
            report.status, report.detail = "error", f"{type(exc).__name__}: {exc}"
            files.write_json(HEARTBEAT, self._heartbeat(at, report, state, disk))
            return report
        finally:
            store.close()
        self.crash("after_db")
        # ---- everything below writes only controller files (sm/)
        files.append(AUDIT_LOG, [{"seq": s, "kind": k, "ticket": t, "payload": p} for s, k, t, p in events])
        self.crash("after_audit")
        registry = _read_json(Path(cfg.run_dir) / r.REGISTRY_REL)
        if mode == "core":
            would: list[WouldDo] = []
            if not state.get("core_mode"):  # once per episode (review F9)
                alerts.append({"at": at, "kind": "core_mode", "detail": "meta.mode=core: this controller only runs "
                               "shadow delivery (stage C adds real delivery); nothing delivered or recorded"})
            state["core_mode"] = True
        else:
            state["core_mode"] = False
            would = shadow_outbox(world, at, registry, board_accepted_at(run))
            would.extend(legacy_capacity_would(res.notes))
        recorded = {str(x.get("key")) for x in files.jsonl(OUTBOX_LOG)}
        files.append(OUTBOX_LOG, [w.to_json(at) for w in would if w.key not in recorded])
        report.would = len(would)
        self.crash("after_outbox")
        diffs, compared = board_compare(world, run, watch)
        report.diffs = len(diffs)
        self._record_diffs(at, diffs, state, compared, len(world.tickets))
        self.crash("after_diff")
        seen_notes = state.get("notes") if isinstance(state.get("notes"), dict) else {}
        assert isinstance(seen_notes, dict)
        current: dict[str, Json] = {}
        fresh: list[JsonObj] = []
        for note in res.notes:
            key = _digest(note.to_json())
            current[key] = True
            if key not in seen_notes:
                fresh.append({"at": at, **note.to_json()})
        files.append(NOTES_LOG, fresh)
        state["notes"] = current
        report.notes = len(res.notes)
        files.write_json(PROJECTION, projection(world, at, applied_seq, res.notes, c.page_queue(world, at)))
        self.crash("after_projection")
        if alerts:
            files.append(ALERTS_LOG, alerts)
        report.seconds = time.monotonic() - started
        files.write_json(HEARTBEAT, self._heartbeat(at, report, state, disk, applied_seq, mode))
        files.write_json(STATE, state)
        return report

    def _backup(self, store: c.SqliteStore, backup_dir: Path, at: str, state: JsonObj,
                alerts: list[JsonObj]) -> None:
        """§13.3 disaster copy. Skipped (one alert per episode) when the backup filesystem is below the
        same free-space floor; leftover temp copies of killed runs are removed first (review F9)."""
        bdisk = r.disk_status(_existing_ancestor(backup_dir), self.cfg.statvfs)
        if not bdisk.ok:
            if state.get("backup_disk_ok", True):
                alerts.append({"at": at, "kind": "backup_disk_low", "path": bdisk.path, "detail": bdisk.reason})
            state["backup_disk_ok"] = False
            return
        if not state.get("backup_disk_ok", True):
            alerts.append({"at": at, "kind": "backup_disk_ok", "path": bdisk.path, "detail": "free space back"})
        state["backup_disk_ok"] = True
        target = self.files.path(backup_dir / "state.backup.db")
        target.parent.mkdir(parents=True, exist_ok=True)
        for stale in sorted(backup_dir.glob(".state.backup.*.db")):  # we hold the controller lock
            self.files.path(stale).unlink(missing_ok=True)
        tmp = self.files.path(backup_dir / f".state.backup.{os.getpid()}.db")
        try:
            store.backup(tmp)
            os.replace(tmp, target)
        finally:
            tmp.unlink(missing_ok=True)
        state["backup_epoch"], state["backup_at"] = _epoch(at), at

    def _record_diffs(self, at: str, diffs: Sequence[Diff], state: JsonObj, compared: int, tickets: int) -> None:
        """One ``cycle`` line per cycle (the zero-diff gate counts these, with how many tickets were
        compared and how many the core holds), plus ``opened``/``changed``/``resolved`` lines when a
        diff appears, changes value or goes away."""
        before = state.get("open_diffs") if isinstance(state.get("open_diffs"), dict) else {}
        assert isinstance(before, dict)
        now: dict[str, Json] = {}
        rows: list[JsonObj] = []
        for d in diffs:
            digest = _digest(d.to_json())
            now[d.key] = digest
            if d.key not in before:
                rows.append({"type": "diff", "status": "opened", "at": at, **d.to_json()})
            elif before[d.key] != digest:
                rows.append({"type": "diff", "status": "changed", "at": at, **d.to_json()})
        for key in sorted(set(before) - set(now)):
            rows.append({"type": "diff", "status": "resolved", "at": at, "key": key})
        by_kind: dict[str, int] = {}
        for d in diffs:
            by_kind[d.kind] = by_kind.get(d.kind, 0) + 1
        rows.append({"type": "cycle", "at": at, "diffs": len(diffs), "by_kind": dict(sorted(by_kind.items())),
                     "compared": compared, "tickets": tickets, "digest": _digest(sorted(now.items()))})
        self.files.append(DIFF_LOG, rows)
        state["open_diffs"] = now

    def _heartbeat(self, at: str, report: CycleReport, state: JsonObj, disk: r.DiskStatus | None,
                   applied_seq: int | None = None, mode: str | None = None) -> JsonObj:
        return {"at": at, "pid": os.getpid(), "run_id": self.cfg.run_id, "mode": mode, "delivery": "shadow",
                "applied_seq": applied_seq, "cycle": report.to_json(),
                "disk": None if disk is None else {"free_bytes": disk.free_bytes, "total_bytes": disk.total_bytes,
                                                   "ok": disk.ok},
                "backup_at": state.get("backup_at"), "interpreter": sys.executable,
                "sqlite": sqlite3.sqlite_version}


# ---------------------------------------------------------------- quiesce / resume


def quiesce(cfg: Config, at: str, *, reason: str = "", timeout: float = 60.0) -> JsonObj:
    """§13.3 before unplugging the run disk: marker first (every core writer's connect() refuses from
    now on), then wait for the controller's cycle to end, checkpoint the WAL with TRUNCATE, close, and
    record the result in the marker. Leaves the marker in place: the database stays closed until
    ``resume``."""
    files = SmFiles(cfg.sm_dir)
    if not cfg.db.is_file():
        raise ControllerError(f"{cfg.db} does not exist")
    marker = cfg.sm_dir / c.QUIESCE_MARKER
    record: JsonObj = {"at": at, "reason": reason, "pid": os.getpid(), "phase": "draining"}
    files.write_json(marker, record)
    with files.lock(blocking=True, timeout=timeout) as got:
        if not got:
            raise ControllerError(f"controller cycle still running after {timeout}s; marker left at {marker}")
        conn = c.connect(cfg.db, cfg.run_id, allow_quiesced=True)
        try:
            row: tuple[int, int, int] = (1, -1, -1)
            for _ in range(20):
                row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if row[0] == 0:
                    break
                time.sleep(0.25)
        finally:
            conn.close()
    wal = cfg.db.with_name(cfg.db.name + "-wal")
    wal_size = wal.stat().st_size if wal.exists() else 0
    record.update({"phase": "quiesced" if row[0] == 0 and wal_size == 0 else "checkpoint_incomplete",
                   "checkpoint": {"busy": row[0], "log": row[1], "checkpointed": row[2]}, "wal_bytes": wal_size})
    files.write_json(marker, record)
    if record["phase"] != "quiesced":
        raise ControllerError(f"WAL checkpoint incomplete ({record['checkpoint']}, wal {wal_size} bytes); "
                              "another writer still holds the database; marker left in place")
    return record


def resume(cfg: Config, at: str) -> JsonObj:
    """Remove the quiesce marker after the database opens cleanly (run_id, schema, quick_check)."""
    marker = cfg.sm_dir / c.QUIESCE_MARKER
    if not marker.exists():
        return {"at": at, "resumed": False, "detail": "not quiesced"}
    conn = c.connect(cfg.db, cfg.run_id, allow_quiesced=True)
    try:
        check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
    finally:
        conn.close()
    if check != "ok":
        raise ControllerError(f"quick_check: {check}; marker kept")
    SmFiles(cfg.sm_dir).path(marker).unlink()
    return {"at": at, "resumed": True, "quick_check": check}


# ---------------------------------------------------------------- shadow-diff report (the 24 h gate)


def diff_report(rows: Sequence[JsonObj], at: str, hours: float = 24.0, max_gap_seconds: float = 300.0) -> JsonObj:
    """Count the shadow-diff stream over the last ``hours``: cycles, cycles with a diff, the largest
    gap between cycles, distinct diff keys opened, by kind. ``zero_diff`` holds when every cycle of a
    window that is covered end to end (first cycle within ``max_gap_seconds`` of the window start, no
    gap above it, last cycle within it of now) shows no diff and compared at least one ticket present on
    both sides while the core held tickets."""
    end = _epoch(at)
    start = end - hours * 3600
    cycles = [x for x in rows if x.get("type") == "cycle" and isinstance(x.get("at"), str)
              and start <= _epoch(str(x["at"])) <= end]
    stamps = sorted(_epoch(str(x["at"])) for x in cycles)
    gaps = [b - a for a, b in zip([start, *stamps], [*stamps, end])] if stamps else [end - start]
    nonzero = [x for x in cycles if x.get("diffs")]
    # Review F6: a cycle that compared nothing (empty board or empty core) proves nothing.
    blind = [x for x in cycles if _num(x.get("compared")) <= 0 or _num(x.get("tickets")) <= 0]
    opened = [x for x in rows if x.get("type") == "diff" and x.get("status") in ("opened", "changed")
              and isinstance(x.get("at"), str) and start <= _epoch(str(x["at"])) <= end]
    by_kind: dict[str, int] = {}
    for x in opened:
        by_kind[str(x.get("kind"))] = by_kind.get(str(x.get("kind")), 0) + 1
    covered = bool(stamps) and max(gaps) <= max_gap_seconds
    return {"at": at, "hours": hours, "cycles": len(cycles), "cycles_with_diffs": len(nonzero),
            "cycles_without_comparison": len(blind),
            "max_diffs": int(max((_num(x.get("diffs")) for x in cycles), default=0)),
            "max_gap_seconds": round(max(gaps), 1), "covered": covered,
            "distinct_keys": len({str(x.get("key")) for x in opened}), "opened_by_kind": dict(sorted(by_kind.items())),
            "zero_diff": covered and not nonzero and not blind}


# ---------------------------------------------------------------- CLI


def _pinned(executable: str) -> bool:
    try:
        return Path(executable).resolve() == c.SM_PYTHON.resolve()
    except OSError:
        return False


def main(argv: Sequence[str] | None = None, *, clock: Callable[[], str] = now_iso) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--run-dir", type=Path, default=os.environ.get("HOST_DAG_DAG_RUN_DIR"),
                        help="<run>/local-dev-dag (env HOST_DAG_DAG_RUN_DIR)")
    parser.add_argument("--run-id", default=os.environ.get("HOST_DAG_DAG_RUN_ID") or None,
                        help="state.db meta.run_id (default: the run directory's name)")
    parser.add_argument("--allow-unpinned", action="store_true",
                        help=f"run on an interpreter other than {c.SM_PYTHON} (tests, development)")
    sub = parser.add_subparsers(dest="command", required=True)
    loop = sub.add_parser("run", help="shadow controller loop (one cycle every --interval seconds)")
    loop.add_argument("--interval", type=float, default=INTERVAL_SECONDS)
    loop.add_argument("--cycles", type=int, default=0, help="stop after N cycles (0: run until SIGTERM)")
    loop.add_argument("--no-backup", action="store_true")
    once = sub.add_parser("once", help="one shadow cycle, report as JSON")
    once.add_argument("--no-backup", action="store_true")
    q = sub.add_parser("quiesce", help="checkpoint the WAL, close, leave the QUIESCED marker (before unplugging)")
    q.add_argument("--reason", default="")
    q.add_argument("--timeout", type=float, default=60.0)
    sub.add_parser("resume", help="verify the database and remove the QUIESCED marker")
    sub.add_parser("status", help="heartbeat, projection counts and open diffs (read only)")
    rep = sub.add_parser("diff-report", help="count the shadow-diff stream (24 h zero-diff gate)")
    rep.add_argument("--hours", type=float, default=24.0)
    rep.add_argument("--max-gap", type=float, default=300.0)
    init = sub.add_parser("init", help="create an empty sm/state.db for this run (refuses an existing one)")
    init.add_argument("--yes", action="store_true", help="required: creating the database is explicit")
    sub.add_parser("seed", help="atomically import legacy records, adopt existing work (no launches)")
    sub.add_parser("shadow", help="rehearse seed on a new read-only-input copy inside worktree .tmp/")
    sub.add_parser("seed-backfill-external", help="HD-01: add legacy EXTERNALLY_BLOCKED waits a pre-HD-01 seed "
                   "dropped (repeatable; core commands, no launches)")
    deploy = sub.add_parser("deploy-check", help="read-only wrapper/unit/contract deployment preflight")
    deploy.add_argument("--wrapper", type=Path, default=Path.home() / ".local/bin/host-dag")
    deploy.add_argument("--observer-unit", type=Path,
                        default=Path(f"/run/user/{os.getuid()}/systemd/transient/host-dag-watch-20260926.service"))
    deploy.add_argument("--executor-contract", type=Path)
    deploy.add_argument("--reviewer-contract", type=Path)
    args = parser.parse_args(argv)
    if args.command == "deploy-check":
        from hostdag_dag_deploy import check
        result = check(args.wrapper, args.observer_unit, args.executor_contract, args.reviewer_contract)
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0 if result["ready"] else 1
    if args.run_dir is None:
        parser.error("--run-dir (or HOST_DAG_DAG_RUN_DIR) is required")
    try:
        c.check_sqlite_version()
    except c.SqliteVersionError as exc:
        print(f"refusing to run: {exc}", file=sys.stderr)
        return 2
    if not args.allow_unpinned and not _pinned(sys.executable):
        print(f"refusing to run: interpreter {sys.executable} is not the pinned {c.SM_PYTHON} (design §13.2); "
              "pass --allow-unpinned for tests", file=sys.stderr)
        return 2
    run_dir = Path(args.run_dir)
    cfg = Config(run_dir, args.run_id or default_run_id(run_dir))
    if getattr(args, "no_backup", False):
        cfg.backup_root = None
    at = clock()
    try:
        if args.command in ("seed", "shadow"):
            from hostdag_dag_seed import seed, shadow
            result = seed(cfg, at) if args.command == "seed" else shadow(run_dir, cfg.run_id, at)
            print(json.dumps(result, ensure_ascii=False, indent=1))
        elif args.command == "seed-backfill-external":
            from hostdag_dag_seed import backfill_external
            print(json.dumps(backfill_external(cfg, at), ensure_ascii=False, indent=1))
        elif args.command == "once":
            print(json.dumps(Controller(cfg, clock).cycle().to_json(), ensure_ascii=False))
        elif args.command == "run":
            return run_loop(cfg, clock, args.interval, args.cycles)
        elif args.command == "quiesce":
            print(json.dumps(quiesce(cfg, at, reason=args.reason, timeout=args.timeout), ensure_ascii=False))
        elif args.command == "resume":
            print(json.dumps(resume(cfg, at), ensure_ascii=False))
        elif args.command == "status":
            files = SmFiles(cfg.sm_dir)
            proj = files.read_json(PROJECTION) or {}
            state = files.read_json(STATE) or {}
            open_diffs = state.get("open_diffs")
            print(json.dumps({"heartbeat": files.read_json(HEARTBEAT), "labels": proj.get("labels"),
                              "applied_seq": proj.get("applied_seq"),
                              "quiesced": (cfg.sm_dir / c.QUIESCE_MARKER).exists(),
                              "open_diffs": sorted(open_diffs) if isinstance(open_diffs, dict) else []},
                             ensure_ascii=False, indent=1))
        elif args.command == "diff-report":
            rows = SmFiles(cfg.sm_dir).jsonl(DIFF_LOG)
            print(json.dumps(diff_report(rows, at, args.hours, args.max_gap), ensure_ascii=False, indent=1))
        elif args.command == "init":
            if not args.yes:
                parser.error("init creates sm/state.db; pass --yes")
            c.init_db(cfg.db, cfg.run_id, at=at)
            print(f"created {cfg.db} for run {cfg.run_id}")
    except (ControllerError, c.CoreError) as exc:
        print(f"{args.command}: {exc}", file=sys.stderr)
        return 1
    return 0


def run_loop(cfg: Config, clock: Callable[[], str], interval: float, cycles: int = 0,
             sleep: Callable[[float], None] = time.sleep) -> int:
    """Cycle until SIGTERM/SIGINT (finishing the current cycle) or ``cycles`` cycles. A failed cycle is
    reported and the next one reopens everything (§13.3: nothing is held across cycles)."""
    stop = {"now": False}

    def handler(signum: int, frame: object) -> None:
        stop["now"] = True

    previous = {sig: signal.signal(sig, handler) for sig in (signal.SIGTERM, signal.SIGINT)}
    controller = Controller(cfg, clock)
    done = 0
    try:
        while not stop["now"]:
            t0 = time.monotonic()
            try:
                report = controller.cycle()
                print(json.dumps(report.to_json(), ensure_ascii=False), flush=True)
            except Exception as exc:  # keep the loop alive; systemd sees the trace in the journal
                print(f"cycle failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            done += 1
            if cycles and done >= cycles:
                break
            remaining = interval - (time.monotonic() - t0)
            while remaining > 0 and not stop["now"]:
                sleep(min(remaining, 1.0))
                remaining -= 1.0
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)
    return 0


if __name__ == "__main__":
    sys.exit(main())

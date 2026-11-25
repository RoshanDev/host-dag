"""Host DAG authority core, stage B1 (shadow): records, guards, reducer, SQLite transactions.

Design: docs/local-dev/dag-state-machine-design-20261019.md (§3 state model, §4 typed waits,
§6 pending actions, §7 storage). A ticket's state is an authoritative record computed by this
module from commands (coordinator/user intent) and facts (what already happened). One command or
fact is one SQLite transaction: BEGIN IMMEDIATE -> read -> guard/reduce (pure) -> write ticket
records + audit events + op receipt + pending rows -> COMMIT.

Stage B1 is shadow only. Nothing here reads run.json, starts a session, touches the page-window
registry or sends a notification: pending rows of mode ``outbox`` are written and never executed
(the controller of stage B3 executes them). ``run.json.batches`` stays the live truth until the
authority switch of stage C.

``ticket.integrate`` is the deliberate I/O exception: it verifies Git landing itself before
writing and records its complete proof in the audit op. Offline MemoryStore.from_audit
reconstructs trusted recorded facts without I/O. Other reductions run no subprocess.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence, Union

_MISSING_DELIVERY_WARNED: set[str] = set()


def _warn_missing_delivery(module: str) -> None:
    if module not in _MISSING_DELIVERY_WARNED:
        print(f"WARNING: {module} unavailable; skipping release checks", file=sys.stderr)
        _MISSING_DELIVERY_WARNED.add(module)

def _release_coverage_importable() -> bool:
    """A skipped kind proof is only acceptable where the delivery module really is absent (portable use)."""
    import importlib.util
    return importlib.util.find_spec("release_coverage") is not None


Json = Union[None, bool, int, float, str, list["Json"], dict[str, "Json"]]
JsonObj = dict[str, Json]

SCHEMA_VERSION = 1
MIN_SQLITE = (3, 51, 3)  # WAL-reset corruption fix (design §11.4)
# Design §13.2: every process that opens state.db runs this exact interpreter (CPython 3.14.7, SQLite
# 3.53.1). The one place the path is written; unit files spell it as %h/<SM_PYTHON_REL> (tests check).
SM_PYTHON_REL = ".local/share/uv/python/cpython-3.14.7-linux-x86_64-gnu/bin/python3.14"
SM_PYTHON = Path.home() / SM_PYTHON_REL
BUSY_TIMEOUT_MS = 5000
# Stage B3 `quiesce`: while this file exists next to state.db no writer opens the database (connect()
# refuses), so the USB run disk can be unplugged after the WAL checkpoint (design §13.3).
QUIESCE_MARKER = "QUIESCED"

# ---------------------------------------------------------------- vocabulary

WORK, REVIEW, CLOSED = "WORK", "REVIEW", "CLOSED"
PHASES = (WORK, REVIEW, CLOSED)
CLOSURE_KINDS = ("accepted", "failed", "canceled", "superseded")
SCOPES = ("FULL", "SCOPED", "REDUCED")
DEFERRED, AUTHORIZED = "deferred", "authorized"
ROLES = ("executor", "reviewer")
AXES = ("spec", "standards")
VERDICTS = ("PASS", "FAIL")
SEVERITIES = ("P0", "P1", "P2", "P3")
NEW_ROUND_SEVERITIES = frozenset({"P0", "P1"})
# orphaned: a reviewer whose candidate was replaced or whose round was reopened; never active,
# its late facts are stale, and a cleanup action stops its session.
OBSERVED = ("starting", "running", "unknown", "finished", "orphaned")
ACTIVE = frozenset({"starting", "running"})
EXEC_MODES = ("new_attempt", "resume", "adopt", "review")

WAIT_KINDS = ("ticket", "batch", "question", "page-window", "capacity", "lock", "quota",
              "library-issue", "time", "external")
COORDINATOR_WAIT_KINDS = frozenset({"ticket", "batch", "time", "external"})  # wait.add
GATE_WAIT_KINDS = frozenset({"capacity", "lock", "quota", "library-issue"})  # resource.observed only
WAIVABLE = frozenset({"external", "time", "ticket", "capacity"})
FACT_SATISFIABLE = frozenset({"time", "capacity", "lock", "quota", "library-issue"})  # wait.satisfied
CONTINUATIONS = ("none", "resume", "request_reviews", "run_window", "apply_decision", "decide")

QUEUED, DELIVERED, CLAIMED_P, DONE_P, SUPERSEDED_P, ESCALATED = (
    "queued", "delivered", "claimed", "done", "superseded", "escalated")
PENDING_STATUSES = (QUEUED, DELIVERED, CLAIMED_P, DONE_P, SUPERSEDED_P, ESCALATED)
OPEN_PENDING = frozenset({QUEUED, DELIVERED, CLAIMED_P})
OUTBOX, DECIDE = "outbox", "decide"
OUTBOX_KINDS = ("resume", "request_reviews", "run_window", "register_watch", "push", "cleanup")
DECIDE_KINDS = ("dispatch", "triage_result", "triage_reviews", "apply_decision", "unresolvable_wait",
                "executor_unknown", "recovery", "prepare_window", "clear_hold", "answer_user",
                "window_result", "window_lost", "window_check_failed", "window_launch_failed", "next_step")
NUDGE_LIMIT = 3
DECIDE_RESPONSES = ("hold", "escalate", "acknowledge", "answer")

MODES = ("shadow", "legacy", "freeze", "core")

COMMANDS = ("ticket.classify", "ticket.create", "ticket.amend", "ticket.authorize", "execution.request", "question.ask",
            "decision.respond", "wait.add", "wait.update", "wait.waive", "pagewindow.enqueue",
            "ticket.close", "ticket.integrate", "authority.switch", "resource.unpause", "pagewindow.policy",
            "migration.seed")
FACTS = ("execution.observed", "review.observed", "question.answered", "wait.satisfied",
         "resource.observed", "clock.observed", "pending.progress")
OP_KINDS = COMMANDS + FACTS

COMMAND_ACTORS = frozenset({"coordinator", "user", "controller", "seed"})
FACT_ACTORS = frozenset({"watcher", "advisor", "controller", "pagewindow", "probe", "seed"})
ACTORS = tuple(sorted(COMMAND_ACTORS | FACT_ACTORS))
ACTOR_OVERRIDE: dict[str, frozenset[str]] = {
    "migration.seed": frozenset({"seed"}),
    "authority.switch": frozenset({"user", "controller"}),
    "decision.respond": frozenset({"coordinator", "user"}),
    "resource.unpause": frozenset({"coordinator", "user"}),
}
# Outstanding decision: still needs a response (an escalated one is unresolved, only louder).
OUTSTANDING = OPEN_PENDING | {ESCALATED}

# Legacy display (§3.2): the label is a projection; scheduling and nudges never read it.
LABEL_STATE = {"external": "EXTERNALLY_BLOCKED", "user": "NEEDS_USER", "resource": "WAITING_RESOURCE",
               "dependency": "WAITING_DEPENDENCY"}


# ---------------------------------------------------------------- errors


class CoreError(Exception):
    """Base of everything this module raises on purpose."""


class ValidationError(CoreError):
    """The op is malformed (wrong kind, missing field, bad value)."""


class Rejected(CoreError):
    """A command guard refused the op, or a fact conflicts with what is recorded. Nothing written."""


class OpConflict(CoreError):
    """The op_id was already accepted with a different request."""


class InvariantError(CoreError):
    """A structural invariant would be broken; the transaction is rolled back."""


class StorageError(CoreError):
    """The database is missing, of another run, of an unsupported version, or unusable."""


class SqliteVersionError(StorageError):
    pass


class BusyError(StorageError):
    """SQLITE_BUSY persisted past the retry budget. Retry later with the same op_id."""


class QuiescedError(StorageError):
    """The database was quiesced for unplugging (``QUIESCED`` marker); resume it first."""


# ---------------------------------------------------------------- payload access (runtime validation)


def _req_str(p: Mapping[str, Json], key: str, choices: Iterable[str] | None = None) -> str:
    value = p.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{key}: required non-empty string")
    if choices is not None and value not in tuple(choices):
        raise ValidationError(f"{key}: must be one of {tuple(choices)}, got {value!r}")
    return value


def _opt_str(p: Mapping[str, Json], key: str, choices: Iterable[str] | None = None) -> str | None:
    if p.get(key) is None:
        return None
    return _req_str(p, key, choices)


def _str_list(p: Mapping[str, Json], key: str) -> list[str] | None:
    value = p.get(key)
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
        raise ValidationError(f"{key}: must be a list of non-empty strings")
    return [str(v) for v in value]


def _opt_int(p: Mapping[str, Json], key: str) -> int | None:
    value = p.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{key}: must be an integer")
    return value


def _flag(p: Mapping[str, Json], key: str) -> bool:
    value = p.get(key, False)
    if not isinstance(value, bool):
        raise ValidationError(f"{key}: must be true or false")
    return value


def _obj(value: Json, key: str) -> JsonObj:
    if not isinstance(value, dict):
        raise ValidationError(f"{key}: must be an object")
    return value


def _opt_obj(p: Mapping[str, Json], key: str) -> JsonObj | None:
    value = p.get(key)
    return None if value is None else _obj(value, key)


def _str_map(value: Json, key: str) -> dict[str, str]:
    obj = _obj(value, key)
    if not all(isinstance(v, str) for v in obj.values()):
        raise ValidationError(f"{key}: values must be strings")
    return {k: str(v) for k, v in obj.items()}


def parse_at(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValidationError(f"not an ISO-8601 time: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValidationError(f"time needs a UTC offset: {value!r}")
    return parsed


def canonical(value: Json) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def derive_id(op_id: str, tag: str) -> str:
    """Deterministic child id: replaying the same op gives the same ids (no uuid in the reducer)."""
    prefix = tag.split(":", 1)[0]
    return f"{prefix}-{hashlib.sha256((op_id + chr(0) + tag).encode()).hexdigest()[:16]}"


# ---------------------------------------------------------------- records


@dataclass
class Continuation:
    kind: str = "none"
    bound: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def load(value: Json) -> Continuation:
        obj = _obj(value, "on_satisfied")
        kind = _req_str(obj, "kind", CONTINUATIONS)
        bound = _str_map(obj.get("bound") or {}, "on_satisfied.bound")
        if kind == "resume" and "execution_id" not in bound:
            raise ValidationError("on_satisfied resume needs bound.execution_id")
        if kind == "decide" and bound.get("kind") not in DECIDE_KINDS:
            raise ValidationError(f"on_satisfied decide needs bound.kind in {DECIDE_KINDS}")
        if kind == "run_window" and "entry_id" not in bound:
            raise ValidationError("on_satisfied run_window needs bound.entry_id")
        return Continuation(kind, bound)


@dataclass
class Wait:
    wait_id: str
    kind: str
    ref: str
    revision: int
    source: str
    added_at: str
    note: str = ""
    on_satisfied: Continuation = field(default_factory=Continuation)
    detail: JsonObj = field(default_factory=dict)
    satisfied_at: str | None = None
    satisfied_by: JsonObj | None = None
    waived: dict[str, str] | None = None
    continued: bool = False

    @property
    def open(self) -> bool:
        return self.satisfied_at is None and self.waived is None

    @staticmethod
    def load(o: JsonObj) -> Wait:
        waived = o.get("waived")
        sat_by = o.get("satisfied_by")
        return Wait(_req_str(o, "wait_id"), _req_str(o, "kind", WAIT_KINDS), _req_str(o, "ref"),
                    _opt_int(o, "revision") or 1, _req_str(o, "source"), _req_str(o, "added_at"),
                    str(o.get("note") or ""), Continuation.load(o.get("on_satisfied") or {"kind": "none"}),
                    _opt_obj(o, "detail") or {}, _opt_str(o, "satisfied_at"),
                    None if sat_by is None else _obj(sat_by, "satisfied_by"),
                    None if waived is None else _str_map(waived, "waived"), _flag(o, "continued"))


@dataclass
class Execution:
    execution_id: str
    attempt: int
    role: str
    mode: str
    requested_at: str
    observed: str
    observed_at: str
    axis: str | None = None
    candidate_digest: str | None = None
    provider: str | None = None
    model: str | None = None
    effort: str | None = None
    session: str | None = None
    contract: str | None = None
    resume_of: str | None = None
    override_reason: str | None = None
    result: JsonObj | None = None
    segment: int = 0  # +1 per resume of the same session (design §3.1: resume keeps the record)
    history: list[JsonObj] = field(default_factory=list)  # earlier segments' results and resume sends

    @staticmethod
    def load(o: JsonObj) -> Execution:
        history = o.get("history") or []
        if not isinstance(history, list):
            raise ValidationError("history: must be a list")
        return Execution(_req_str(o, "execution_id"), _opt_int(o, "attempt") or 0, _req_str(o, "role", ROLES),
                         _req_str(o, "mode", EXEC_MODES), _req_str(o, "requested_at"),
                         _req_str(o, "observed", OBSERVED), _req_str(o, "observed_at"),
                         _opt_str(o, "axis", AXES), _opt_str(o, "candidate_digest"), _opt_str(o, "provider"),
                         _opt_str(o, "model"), _opt_str(o, "effort"), _opt_str(o, "session"),
                         _opt_str(o, "contract"), _opt_str(o, "resume_of"), _opt_str(o, "override_reason"),
                         _opt_obj(o, "result"), _opt_int(o, "segment") or 0, [_obj(h, "history") for h in history])


@dataclass
class Candidate:
    digest: str
    evidence: str
    from_execution: str

    @staticmethod
    def load(o: JsonObj) -> Candidate:
        return Candidate(_req_str(o, "digest"), str(o.get("evidence") or ""), _req_str(o, "from_execution"))


@dataclass
class Review:
    review_request_id: str
    axis: str
    candidate_digest: str
    execution_id: str
    verdict: str | None = None
    max_severity: str | None = None

    @staticmethod
    def load(o: JsonObj) -> Review:
        return Review(_req_str(o, "review_request_id"), _req_str(o, "axis", AXES), _req_str(o, "candidate_digest"),
                      _req_str(o, "execution_id"), _opt_str(o, "verdict", VERDICTS),
                      _opt_str(o, "max_severity", SEVERITIES))


@dataclass
class Pending:
    action_id: str
    kind: str
    mode: str
    allowed_actions: list[str]
    status: str
    created_at: str
    updated_at: str
    bound: dict[str, str] = field(default_factory=dict)
    payload: JsonObj = field(default_factory=dict)
    nudges: list[str] = field(default_factory=list)
    detail: str = ""
    done_by: str | None = None

    @property
    def open(self) -> bool:
        return self.status in OPEN_PENDING

    @staticmethod
    def load(o: JsonObj) -> Pending:
        mode = _req_str(o, "mode", (OUTBOX, DECIDE))
        return Pending(_req_str(o, "action_id"), _req_str(o, "kind", OUTBOX_KINDS if mode == OUTBOX else DECIDE_KINDS),
                       mode, _str_list(o, "allowed_actions") or [], _req_str(o, "status", PENDING_STATUSES),
                       _req_str(o, "created_at"), _req_str(o, "updated_at"),
                       _str_map(o.get("bound") or {}, "bound"), _opt_obj(o, "payload") or {},
                       _str_list(o, "nudges") or [], str(o.get("detail") or ""), _opt_str(o, "done_by"))


@dataclass
class Question:
    question_id: str
    revision: int
    to: str
    key: str
    title: str
    asked_at: str
    question_file: str | None = None
    answer: JsonObj | None = None

    @staticmethod
    def load(o: JsonObj) -> Question:
        return Question(_req_str(o, "question_id"), _opt_int(o, "revision") or 1, _req_str(o, "to", ("advisor", "user")),
                        _req_str(o, "key"), _req_str(o, "title"), _req_str(o, "asked_at"),
                        _opt_str(o, "question_file"), _opt_obj(o, "answer"))


@dataclass
class Closure:
    kind: str
    at: str
    scope: str | None = None
    reason: str = ""
    superseded_by: str | None = None
    candidate_digest: str | None = None
    affects_downstream: list[str] | None = None
    waive_reviews: str | None = None
    integrated: dict[str, str] | None = None

    @staticmethod
    def load(o: JsonObj) -> Closure:
        integrated = o.get("integrated")
        return Closure(_req_str(o, "kind", CLOSURE_KINDS), _req_str(o, "at"), _opt_str(o, "scope", SCOPES),
                       str(o.get("reason") or ""), _opt_str(o, "superseded_by"), _opt_str(o, "candidate_digest"),
                       _str_list(o, "affects_downstream"), _opt_str(o, "waive_reviews"),
                       None if integrated is None else _str_map(integrated, "integrated"))


@dataclass
class Ticket:
    ticket: str
    batch: str
    title: str
    created_at: str
    updated_at: str
    phase: str = WORK
    revision: int = 0
    scheduling: str = AUTHORIZED
    defer: str | None = None
    contract: str = ""
    notes: str = ""
    deps: list[str] = field(default_factory=list)
    waits: list[Wait] = field(default_factory=list)
    executions: list[Execution] = field(default_factory=list)
    attempt: int = 0
    candidate: Candidate | None = None
    reviews: list[Review] = field(default_factory=list)
    pending: list[Pending] = field(default_factory=list)
    questions: list[Question] = field(default_factory=list)
    closure: Closure | None = None
    imported: JsonObj = field(default_factory=dict)  # legacy claims, explicitly NOT first-hand evidence

    kind: str | None = None
    kind_proof: JsonObj | None = None
    defect_class: str | None = None
    regression: JsonObj | None = None
    production_write_fence: bool = False
    coverage_migration: JsonObj | None = None
    release_group: str | None = None
    requires_for_release: list[str] = field(default_factory=list)
    release_obligation: JsonObj | None = None

    def to_json(self) -> JsonObj:
        out: JsonObj = json.loads(json.dumps(asdict(self)))
        return out

    @staticmethod
    def load(o: JsonObj) -> Ticket:
        def objs(key: str) -> list[JsonObj]:
            value = o.get(key) or []
            if not isinstance(value, list):
                raise ValidationError(f"{key}: must be a list")
            return [_obj(v, key) for v in value]

        cand, closure = o.get("candidate"), o.get("closure")
        return Ticket(
            _req_str(o, "ticket"), _req_str(o, "batch"), _req_str(o, "title"), _req_str(o, "created_at"),
            _req_str(o, "updated_at"), _req_str(o, "phase", PHASES), _opt_int(o, "revision") or 0,
            _req_str(o, "scheduling", (DEFERRED, AUTHORIZED)), _opt_str(o, "defer"), str(o.get("contract") or ""),
            str(o.get("notes") or ""), _str_list(o, "deps") or [], [Wait.load(w) for w in objs("waits")],
            [Execution.load(e) for e in objs("executions")], _opt_int(o, "attempt") or 0,
            None if cand is None else Candidate.load(_obj(cand, "candidate")),
            [Review.load(r) for r in objs("reviews")], [Pending.load(p) for p in objs("pending")],
            [Question.load(q) for q in objs("questions")],
            None if closure is None else Closure.load(_obj(closure, "closure")), _opt_obj(o, "imported") or {},
            _opt_str(o, "kind"), copy.deepcopy(_opt_obj(o, "kind_proof")),
            _opt_str(o, "defect_class"), copy.deepcopy(_opt_obj(o, "regression")),
            o.get("production_write_fence") is True, copy.deepcopy(_opt_obj(o, "coverage_migration")),
            _opt_str(o, "release_group"), _str_list(o, "requires_for_release") or [],
            copy.deepcopy(_opt_obj(o, "release_obligation")))

    # ---- derived views over the record (pure)

    def open_waits(self) -> list[Wait]:
        return [w for w in self.waits if w.open]

    def active(self, role: str | None = None) -> list[Execution]:
        return [e for e in self.executions if e.observed in ACTIVE and (role is None or e.role == role)]

    def window_running(self) -> bool:
        """A page window of this ticket was acquired and has not been released."""
        return any(w.kind == "page-window" and w.satisfied_at is not None and w.waived is None
                   and w.detail.get("outcome") is None for w in self.waits)

    def outstanding(self, mode: str = "decide") -> list[Pending]:
        return [p for p in self.pending if p.mode == mode and p.status in OUTSTANDING]

    def execution(self, execution_id: str) -> Execution | None:
        return next((e for e in self.executions if e.execution_id == execution_id), None)

    def current_executor(self) -> Execution | None:
        return next((e for e in reversed(self.executions) if e.role == "executor"), None)

    def current_reviewer(self, axis: str) -> Execution | None:
        digest = self.candidate.digest if self.candidate else None
        return next((e for e in reversed(self.executions)
                     if e.role == "reviewer" and e.axis == axis and e.candidate_digest == digest), None)

    def reviews_for_current(self) -> dict[str, Review]:
        """Axis -> the latest review bound to the current candidate (only these count)."""
        out: dict[str, Review] = {}
        if self.candidate is None:
            return out
        for review in self.reviews:
            if review.candidate_digest == self.candidate.digest:
                out[review.axis] = review
        return out

    def new_round_allowed(self) -> bool:
        """A new executor round from REVIEW only for a FAIL at P0/P1 on the current candidate."""
        return any(r.verdict == "FAIL" and r.max_severity in NEW_ROUND_SEVERITIES
                   for r in self.reviews_for_current().values())

    def pending_by_id(self, action_id: str) -> Pending | None:
        return next((p for p in self.pending if p.action_id == action_id), None)

    def wait_by_id(self, wait_id: str) -> Wait | None:
        return next((w for w in self.waits if w.wait_id == wait_id), None)

    def question_by_id(self, question_id: str) -> Question | None:
        return next((q for q in self.questions if q.question_id == question_id), None)


def accepted(t: Ticket | None) -> bool:
    return t is not None and t.closure is not None and t.closure.kind == "accepted"


# ---------------------------------------------------------------- display projection (§3.2)


def wait_class(w: Wait, t: Ticket) -> str:
    if w.kind == "external":
        return "external"
    if w.kind == "question":
        q = t.question_by_id(w.ref)
        return "user" if q is not None and q.to == "user" else "resource"
    if w.kind in ("ticket", "batch"):
        return "dependency"
    return "resource"


def label(t: Ticket, world: Mapping[str, Ticket] | None = None) -> str:
    """Display name only (board, pushes, legacy ``state``)."""
    if t.closure is not None:
        if t.closure.kind == "accepted":
            return "INTEGRATED" if t.closure.integrated else "ACCEPTED"
        return {"failed": "FAILED", "canceled": "CANCELED", "superseded": "CANCELED"}[t.closure.kind]
    if t.scheduling == DEFERRED:
        return "CREATED"
    classes = {wait_class(w, t) for w in t.open_waits()}
    for cls in ("external", "user", "resource", "dependency"):
        if cls in classes:
            return LABEL_STATE[cls]
    if world is not None and any(not accepted(world.get(d)) for d in t.deps):
        return "WAITING_DEPENDENCY"
    if t.active("executor") or t.active("reviewer") or t.window_running():
        return "RUNNING"
    if t.phase == REVIEW:
        return "REVIEWING"
    return "READY"


def legacy_projection(t: Ticket, world: Mapping[str, Ticket] | None = None) -> JsonObj:
    """The old run.json fields this record implies (written by the stage B3 projector, not here)."""
    name = label(t, world)
    first = next(iter(t.open_waits()), None)
    status: str | None = None
    outcome: str | None = None
    if t.closure is not None:
        # Read back by hostdag_ticket_state.derive_state: FAILED -> FAILED, CANCELED -> CANCELED,
        # SUPERSEDED -> CANCELED (a superseded ticket is canceled with a named successor).
        status = {"accepted": "ACCEPTED", "failed": "FAILED", "canceled": "CANCELED",
                  "superseded": "SUPERSEDED"}[t.closure.kind]
    elif t.executions:
        status = "CLAIMED"
        if name == "NEEDS_USER":
            outcome = "NEEDS_DECISION"
        elif name == "EXTERNALLY_BLOCKED":
            outcome = "EXTERNALLY_BLOCKED"
    return {"state": None if name == "READY" else name, "status": status, "outcome": outcome,
            "wait_on": None if first is None else f"{first.kind}:{first.ref}",
            "waits": [f"{w.kind}:{w.ref}" for w in t.open_waits()],
            "round": t.attempt, "attempt": t.attempt,
            "superseded_by": t.closure.superseded_by if t.closure is not None else None,
            "accept_scope": t.closure.scope if t.closure is not None else None,
            **({"release_group": t.release_group, "requires_for_release": list(t.requires_for_release)}
               if t.release_group or t.requires_for_release else {}),
            **({"release_obligation": copy.deepcopy(t.release_obligation),
                "accepted_at": t.release_obligation["accepted_at"]} if t.release_obligation else {}),
            **({"kind": t.kind, "kind_proof": copy.deepcopy(t.kind_proof)} if t.kind is not None else {})}


# ---------------------------------------------------------------- ops


@dataclass(frozen=True)
class Op:
    """One command or fact. ``at`` is the submitter's clock reading and part of the fact."""
    op_id: str
    kind: str
    actor: str
    at: str
    ticket: str | None = None
    expected_revision: int | None = None
    epoch: int | None = None
    payload: JsonObj = field(default_factory=dict)
    # Tool-generated audit fact, not part of caller intent/idempotency. Live submit
    # always recomputes it; only the offline audit reconstruction reads it.
    integration_proof: JsonObj | None = None
    kind_proof: JsonObj | None = None

    def request(self) -> JsonObj:
        """What identifies the request for idempotency. ``at`` is excluded: a retry of the same op
        after a crash re-reads the clock, and that must stay a replay, not a conflict."""
        return {"kind": self.kind, "actor": self.actor, "ticket": self.ticket,
                "expected_revision": self.expected_revision, "epoch": self.epoch, "payload": self.payload}

    def digest(self) -> str:
        return hashlib.sha256(canonical(self.request()).encode()).hexdigest()

    def to_json(self) -> JsonObj:
        out = {"op_id": self.op_id, "at": self.at, **self.request()}
        if self.integration_proof is not None:
            out["integration_proof"] = copy.deepcopy(self.integration_proof)
        if self.kind_proof is not None:
            out["kind_proof"] = copy.deepcopy(self.kind_proof)
        return out

    @staticmethod
    def load(o: JsonObj) -> Op:
        return Op(_req_str(o, "op_id"), _req_str(o, "kind", OP_KINDS), _req_str(o, "actor", ACTORS),
                  _req_str(o, "at"), _opt_str(o, "ticket"), _opt_int(o, "expected_revision"),
                  _opt_int(o, "epoch"), _opt_obj(o, "payload") or {}, copy.deepcopy(_opt_obj(o, "integration_proof")), copy.deepcopy(_opt_obj(o, "kind_proof")))

    def validate(self) -> None:
        if self.kind not in OP_KINDS:
            raise ValidationError(f"unknown op kind {self.kind!r}")
        if self.actor not in ACTORS:
            raise ValidationError(f"unknown actor {self.actor!r}")
        parse_at(self.at)
        allowed = ACTOR_OVERRIDE.get(self.kind) or (COMMAND_ACTORS if self.kind in COMMANDS else FACT_ACTORS)
        if self.actor not in allowed:
            raise Rejected(f"{self.actor} may not submit {self.kind} (allowed: {sorted(allowed)})")
        json.dumps(self.payload)  # must be plain JSON

    def qualifier(self) -> str | None:
        if self.kind == "execution.request":
            role = self.payload.get("role")
            return "reviewer" if role == "reviewer" else str(self.payload.get("mode") or "")
        if self.kind == "ticket.close":
            return str(self.payload.get("kind") or "")
        if self.kind == "ticket.amend":
            for key in ("deps", "contract"):
                if key in self.payload:
                    return key
        return None


@dataclass
class Meta:
    run_id: str
    schema_version: int = SCHEMA_VERSION
    authority_epoch: int = 0
    mode: str = "shadow"
    # resource -> what holds it paused: "action:<recovery action_id>" or "question:<drift question_id>"
    paused: dict[str, str] = field(default_factory=dict)
    seed: JsonObj = field(default_factory=dict)  # batches (including empty), provenance, unattached history


@dataclass
class World:
    """What the reducer may read: every ticket, run meta, and which import keys are already in."""
    tickets: dict[str, Ticket]
    meta: Meta
    imported: frozenset[str] = frozenset()


IMPORT_TARGET = {"question.answered": "question_id", "wait.satisfied": "wait_id", "resource.observed": "entry_id"}


def import_key(op: Op) -> str | None:
    """Dedup key of an imported fact. One source record (e.g. one resolved-issue line) may release
    waits on several tickets, so the key is the source record *and* what it releases:
    (source_record, ticket, fact kind, target wait/question/entry, event)."""
    source = op.payload.get("source_record")
    if not isinstance(source, str) or not source:
        return None
    target: Json = op.payload.get(IMPORT_TARGET.get(op.kind, ""))
    if op.kind == "resource.observed" and target is None:
        gate = op.payload.get("gate")
        target = f"{gate.get('kind')}:{gate.get('ref')}" if isinstance(gate, dict) else None
    return canonical([source, op.ticket, op.kind, target, op.payload.get("event")])


@dataclass
class Event:
    kind: str
    ticket: str | None
    payload: JsonObj
    cause: str | None = None


@dataclass
class Receipt:
    op_id: str
    status: str  # accepted | noop | stale
    ticket: str | None = None
    revision: int | None = None
    seq: int | None = None
    detail: str = ""
    actions: list[str] = field(default_factory=list)
    replayed: bool = False

    def to_json(self) -> JsonObj:
        return {"op_id": self.op_id, "status": self.status, "ticket": self.ticket, "revision": self.revision,
                "seq": self.seq, "detail": self.detail, "actions": list(self.actions)}

    @staticmethod
    def load(o: JsonObj, replayed: bool = False) -> Receipt:
        actions = _str_list(o, "actions") or []
        return Receipt(_req_str(o, "op_id"), _req_str(o, "status"), _opt_str(o, "ticket"),
                       _opt_int(o, "revision"), _opt_int(o, "seq"), str(o.get("detail") or ""), actions, replayed)


@dataclass
class Transition:
    """Output of the pure reducer; the store writes exactly this."""
    receipt: Receipt
    changed: dict[str, Ticket]
    events: list[Event]
    imported: list[str]
    meta: Meta
    quiet: bool = False  # a no-change heartbeat: store the receipt, write no audit event


# ---------------------------------------------------------------- reducer


class _Tx:
    def __init__(self, world: World, op: Op) -> None:
        self.before = world.tickets
        self.tickets = copy.deepcopy(world.tickets)
        self.meta = copy.deepcopy(world.meta)
        self.imported_known = world.imported
        self.op = op
        self.kind_fact: JsonObj | None = None
        self.integration_fact: JsonObj | None = None
        self.events: list[Event] = []
        self.imported: list[str] = []
        self.actions: list[str] = []
        self.status = "accepted"
        self.detail = ""
        self.quiet = False
        self.key = import_key(op)
        self._tags: dict[str, int] = {}

    def already_imported(self) -> bool:
        return self.key is not None and self.key in self.imported_known

    def mark_imported(self) -> None:
        if self.key is not None and self.key not in self.imported:
            self.imported.append(self.key)

    # -- helpers

    def ticket(self, name: str | None = None) -> Ticket:
        name = name or self.op.ticket
        if not name:
            raise ValidationError(f"{self.op.kind} needs a ticket")
        t = self.tickets.get(name)
        if t is None:
            raise Rejected(f"unknown ticket {name}")
        return t

    def new_id(self, tag: str) -> str:
        n = self._tags.get(tag, 0)
        self._tags[tag] = n + 1
        return derive_id(self.op.op_id, f"{tag}:{n}")

    def emit(self, name: str, ticket: str | None, /, **payload: Json) -> None:
        self.events.append(Event(name, ticket, dict(payload), cause=self.op.op_id))

    def noop(self, detail: str) -> None:
        self.status, self.detail = "noop", detail

    def stale(self, detail: str) -> None:
        self.status, self.detail = "stale", detail

    def add_pending(self, t: Ticket, kind: str, mode: str, *, bound: dict[str, str] | None = None,
                    payload: JsonObj | None = None, detail: str = "", once: bool = False) -> Pending:
        """One open action per kind and binding. ``once``: settle-made actions are created only once
        per binding, whatever their status, so an escalated or held one is not re-created each op."""
        bound = bound or {}
        for p in t.pending:
            if p.kind == kind and p.bound == bound and (p.status in OUTSTANDING or once):
                return p
        allowed = allowed_actions(t, kind) if mode == DECIDE else outbox_tokens(kind)
        p = Pending(self.new_id(f"act:{t.ticket}:{kind}"), kind, mode, allowed, QUEUED, self.op.at, self.op.at,
                    bound, payload or {}, [], detail)
        t.pending.append(p)
        self.actions.append(p.action_id)
        self.emit("pending.created", t.ticket, action_id=p.action_id, kind=kind, mode=mode, bound=dict(bound))
        return p

    def close_pending(self, t: Ticket, p: Pending, status: str, why: str) -> None:
        p.status, p.updated_at, p.done_by = status, self.op.at, self.op.op_id
        self.emit(f"pending.{status}", t.ticket, action_id=p.action_id, kind=p.kind, why=why)

    def supersede(self, t: Ticket, kinds: Iterable[str], why: str) -> None:
        wanted = set(kinds)
        for p in t.pending:
            if p.status in OUTSTANDING and p.kind in wanted:
                self.close_pending(t, p, SUPERSEDED_P, why)

    def retire_reviews(self, t: Ticket, keep: str | None, why: str) -> None:
        """A candidate left review (replaced, or a new executor round opened): its still-running
        reviewers become orphaned (never active, late facts stale) with a cleanup action each, and its
        queued review dispatches and review triage are superseded. ``keep`` is the digest still under
        review (None: none)."""
        for e in t.executions:
            if e.role == "reviewer" and e.candidate_digest != keep and e.observed in ACTIVE | {"unknown"}:
                e.observed, e.observed_at = "orphaned", self.op.at
                self.emit("execution.orphaned", t.ticket, execution_id=e.execution_id, why=why)
                self.add_pending(t, "cleanup", OUTBOX, bound={"execution_id": e.execution_id},
                                 payload={"session": e.session, "role": "reviewer", "axis": e.axis})
        for p in t.pending:
            if p.status in OUTSTANDING and p.kind in ("request_reviews", "triage_reviews") \
                    and p.bound.get("candidate_digest") != keep:
                self.close_pending(t, p, SUPERSEDED_P, why)

    def satisfy(self, t: Ticket, w: Wait, source_record: str, value: JsonObj) -> None:
        w.satisfied_at = self.op.at
        w.satisfied_by = {"source_record": source_record, "op_id": self.op.op_id, "value": value}
        self.emit("wait.satisfied", t.ticket, wait_id=w.wait_id, kind=w.kind, ref=w.ref, source_record=source_record)

    def add_wait(self, t: Ticket, kind: str, ref: str, *, wait_id: str | None = None, note: str = "",
                 on_satisfied: Continuation | None = None, detail: JsonObj | None = None) -> Wait:
        w = Wait(wait_id or self.new_id(f"wait:{t.ticket}:{kind}"), kind, ref, 1, self.op.kind, self.op.at, note,
                 on_satisfied or Continuation(), detail or {})
        t.waits.append(w)
        self.emit("wait.added", t.ticket, wait_id=w.wait_id, kind=kind, ref=ref)
        return w

    def complete_by_command(self, t: Ticket) -> None:
        """Mark the open pending actions this command answers as done (§6.1: only the matching
        successful command does; an unrelated wait.add or note does not)."""
        kind, qual = self.op.kind, self.op.qualifier()
        tokens = {kind} | ({f"{kind}:{qual}"} if qual else set())
        for p in t.pending:
            live = p.status in OUTSTANDING if p.mode == DECIDE else p.open
            if not live or not tokens.intersection(p.allowed_actions):
                continue
            if p.mode == OUTBOX:
                if "axis" in p.bound and p.bound["axis"] != self.op.payload.get("axis"):
                    continue
                if "execution_id" in p.bound and p.bound["execution_id"] != self.op.payload.get("resume_of"):
                    continue
            self.close_pending(t, p, DONE_P, f"answered by {kind}" + (f":{qual}" if qual else ""))


def outbox_tokens(kind: str) -> list[str]:
    """Which successful command also completes an outbox row (others complete by receipt fact)."""
    return {"request_reviews": ["execution.request:reviewer"], "resume": ["execution.request:resume"]}.get(kind, [])


def allowed_actions(t: Ticket, kind: str) -> list[str]:
    """Generated from the current record (§6.1), never a generic table: WORK offers no accept,
    REVIEW offers a new round only for a P0/P1 FAIL on the current candidate."""
    review_close = ["ticket.close:accepted", "ticket.close:canceled", "ticket.close:failed",
                    "ticket.close:superseded", "question.ask", "ticket.amend:contract"]
    new_round = ["execution.request:new_attempt", "execution.request:resume"]
    if kind == "dispatch":
        base = ["execution.request:new_attempt", "question.ask", "ticket.close:canceled", "ticket.close:superseded"]
    elif kind == "triage_result":
        base = new_round + ["question.ask", "ticket.close:canceled", "ticket.close:failed", "ticket.close:superseded"]
    elif kind == "triage_reviews":
        base = review_close + (new_round if t.new_round_allowed() else [])
    elif kind in ("apply_decision", "next_step"):
        # No wait.add here: a wait with no continuation would consume the decision and stop the
        # ticket silently. No reviewer request either: requesting one axis is mechanical (outbox) and
        # must not close the ruling (review L5).
        if t.phase == REVIEW:
            base = review_close + (new_round if t.new_round_allowed() else [])
        else:
            base = new_round + ["question.ask", "ticket.close:canceled", "ticket.close:failed",
                                "ticket.close:superseded"]
    elif kind == "unresolvable_wait":
        base = ["wait.waive", "ticket.amend:deps", "ticket.close:canceled", "ticket.close:superseded"]
    elif kind == "executor_unknown":
        base = ["execution.request:adopt"] + new_round + ["ticket.close:canceled", "ticket.close:failed"]
    elif kind == "prepare_window":
        base = ["pagewindow.enqueue"]
    elif kind == "answer_user":
        return ["answer", "hold", "escalate"]
    elif kind == "recovery":
        # Only a resolution unpauses the page resource: recovered (acknowledge), re-run, or give up.
        base = ["acknowledge", "pagewindow.enqueue", "ticket.close:canceled", "ticket.close:failed"]
    else:  # recovery, clear_hold, window_*
        base = ["pagewindow.enqueue", "question.ask", "ticket.close:canceled", "ticket.close:failed", "acknowledge"]
        if t.phase == REVIEW:
            base += ["ticket.close:accepted"]
    if t.phase == REVIEW and not t.new_round_allowed():  # no route around the P0/P1 rule
        base = [a for a in base if a not in new_round]
    if t.phase != REVIEW:
        base = [a for a in base if a != "ticket.close:accepted"]
    return base + ["hold", "escalate"]


Handler = Callable[[_Tx], None]


def _ticket_create(tx: _Tx) -> None:
    p, name = tx.op.payload, tx.op.ticket
    if not name:
        raise ValidationError("ticket.create needs a ticket")
    if name in tx.tickets:
        existing = tx.tickets[name]
        raise Rejected(f"ticket {name} exists (revision {existing.revision}); use ticket.amend")
    title = _req_str(p, "title")
    deps = _str_list(p, "deps") or []
    scheduling = _req_str(p, "scheduling", (DEFERRED, AUTHORIZED))
    defer = _opt_str(p, "defer")
    if scheduling == DEFERRED:
        _check_defer(tx, defer)
    elif defer:
        raise ValidationError("defer only applies to scheduling=deferred")
    for dep in deps:
        if dep == name:
            raise Rejected(f"{name} cannot depend on itself")
        if dep not in tx.tickets:
            raise Rejected(f"dependency {dep} does not exist")
    tx.tickets[name] = Ticket(name, _req_str(p, "batch"), title, tx.op.at, tx.op.at, scheduling=scheduling,
                              defer=defer, contract=str(p.get("contract") or ""), notes=str(p.get("notes") or ""),
                              deps=deps)
    tx.emit("ticket.created", name, scheduling=scheduling, deps=list(deps))


def _check_defer(tx: _Tx, defer: str | None) -> None:
    """``defer`` names the predicate after which the ticket may be authorized (failure #2)."""
    if not defer:
        raise ValidationError("scheduling=deferred needs defer (ticket:X | batch:Y:bound | batch:Y:completed)")
    kind, _, rest = defer.partition(":")
    if kind == "ticket" and rest:
        if rest not in tx.tickets:
            raise Rejected(f"defer {defer}: no such ticket")
        return
    if kind == "batch" and rest.rpartition(":")[2] in ("bound", "completed") and rest.rpartition(":")[0]:
        return
    raise ValidationError(f"defer must be ticket:X, batch:Y:bound or batch:Y:completed, got {defer!r}")


AMENDABLE = ("release_group", "requires_for_release", "production_write_fence", "coverage_migration", "defect_class", "regression", "title", "contract", "notes", "deps", "defer")


def _ticket_amend(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    bad = [k for k in p if k not in AMENDABLE]
    if bad:
        raise ValidationError(f"ticket.amend cannot change {bad}; amendable: {AMENDABLE} "
                              "(phase/attempt/executions/state follow from commands and facts)")
    if not p:
        raise ValidationError("ticket.amend: nothing to change")
    if any(k in p for k in ("release_group", "requires_for_release")):
        try:
            from release_coverage import release_relationships
        except ImportError:
            _warn_missing_delivery("release_coverage")
            if "release_group" in p:
                t.release_group = p["release_group"]
            if "requires_for_release" in p:
                t.requires_for_release = list(p["requires_for_release"] or [])
        else:
            values = dict(release_group=t.release_group, requires_for_release=t.requires_for_release)
            values.update({k: p[k] for k in values if k in p})
            try:
                group, requires = release_relationships(values)
            except ValueError as exc:
                raise ValidationError(str(exc)) from exc
            if t.phase == CLOSED and (group != t.release_group or requires != t.requires_for_release):
                raise Rejected("RELEASE_OBLIGATION_LOCKED: closed ticket release relationships cannot change")
            t.release_group, t.requires_for_release = group, requires
    if t.phase == CLOSED and any(k in p for k in ("deps", "defer", "contract")):
        raise Rejected(f"{t.ticket} is closed; only title/notes can change")
    if "production_write_fence" in p:
        if p["production_write_fence"] is not True:
            raise Rejected("production_write_fence can only be enabled, never cleared")
        t.production_write_fence = True
    if "coverage_migration" in p:
        # Audit metadata only. It never substitutes for live kind/source/ruling checks.
        t.coverage_migration = copy.deepcopy(_opt_obj(p, "coverage_migration"))
    if "title" in p:
        t.title = _req_str(p, "title")
    if "notes" in p:
        t.notes = str(p.get("notes") or "")
    # Board-owned regression metadata is archived verbatim; acceptance validation
    # stays exclusively at ticket set, never in the core or audit replay.
    if "defect_class" in p:
        t.defect_class = _opt_str(p, "defect_class")
    if "regression" in p:
        t.regression = copy.deepcopy(_opt_obj(p, "regression"))
    if "contract" in p:
        t.contract = str(p.get("contract") or "")
    if "defer" in p:
        if t.scheduling != DEFERRED:
            raise Rejected("defer only applies to a deferred ticket")
        _check_defer(tx, _opt_str(p, "defer"))
        t.defer = _opt_str(p, "defer")
    if "deps" in p:
        deps = _str_list(p, "deps") or []
        for dep in deps:
            if dep == t.ticket or dep not in tx.tickets:
                raise Rejected(f"dependency {dep} is the ticket itself or does not exist")
        t.deps = deps
    tx.complete_by_command(t)


def _ticket_authorize(tx: _Tx) -> None:
    t = tx.ticket()
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is closed")
    if t.scheduling == AUTHORIZED:
        tx.noop("already authorized")
        return
    t.scheduling, t.defer = AUTHORIZED, None


def _execution_request(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    role = _req_str(p, "role", ROLES)
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is closed; no new executions")
    if t.scheduling != AUTHORIZED:
        raise Rejected(f"{t.ticket} is deferred; ticket.authorize first")
    explicit = _opt_str(p, "execution_id")
    if explicit and any(e.execution_id == explicit for x in tx.tickets.values() for e in x.executions):
        raise Rejected(f"execution_id {explicit} already exists")
    session = _opt_str(p, "session")
    if role == "reviewer":
        _request_reviewer(tx, t, p, explicit, session)
        return
    mode = _req_str(p, "mode", ("new_attempt", "resume", "adopt"))
    override = _opt_str(p, "override_reason")
    if mode == "adopt":
        if not session:
            raise ValidationError("adopt needs the existing session")
        same = next((e for e in t.executions if e.role == "executor" and e.session == session), None)
        if same is not None:
            tx.noop(f"session already tracked as {same.execution_id}")
            return
        if t.active("executor"):
            raise Rejected(f"{t.ticket} already has a running executor ({t.active('executor')[0].execution_id})")
        attempt = max(t.attempt, _opt_int(p, "attempt") or max(t.attempt, 1))
        observed = _opt_str(p, "observed", ("running", "unknown", "finished")) or "running"
    else:
        unmet = [d for d in t.deps if not accepted(tx.tickets.get(d))]
        if unmet:
            raise Rejected(f"{t.ticket} prerequisites not accepted: {', '.join(unmet)}")
        waits = t.open_waits()
        if waits:
            raise Rejected(f"{t.ticket} still waits on " + ", ".join(f"{w.kind}:{w.ref}" for w in waits))
        if t.active("executor"):
            raise Rejected(f"{t.ticket} already has a running executor ({t.active('executor')[0].execution_id})")
        if t.phase == REVIEW and not t.new_round_allowed() and not override:
            raise Rejected(f"{t.ticket} is in REVIEW: a new executor round needs a FAIL at P0/P1 on the current "
                           "candidate, or override_reason")
        for key in ("provider", "model", "effort", "contract"):
            _req_str(p, key)
        observed = "starting"
        if mode == "resume":
            prev_id = _req_str(p, "resume_of")
            prev = t.execution(prev_id)
            if prev is None or prev.role != "executor":
                raise Rejected(f"resume_of {prev_id} is not an executor execution of {t.ticket}")
            if prev is not t.current_executor():
                raise Rejected(f"resume_of {prev_id} is not the current executor execution")
            if session and prev.session and session != prev.session:
                raise Rejected("resume keeps the session; a different session is a new execution "
                               "(mode=new_attempt or adopt)")
            _resume_in_place(tx, t, prev, session, p, override)
            return
        attempt = t.attempt + 1
    e = Execution(explicit or tx.new_id(f"exe:{t.ticket}"), attempt, "executor", mode, tx.op.at, observed, tx.op.at,
                  provider=_opt_str(p, "provider"), model=_opt_str(p, "model"), effort=_opt_str(p, "effort"),
                  session=session, contract=_opt_str(p, "contract"), resume_of=_opt_str(p, "resume_of"),
                  override_reason=override)
    t.executions.append(e)
    t.attempt = max(t.attempt, attempt)
    tx.emit("execution.requested", t.ticket, execution_id=e.execution_id, mode=mode, attempt=attempt)
    tx.complete_by_command(t)
    if t.phase == REVIEW and mode != "adopt":
        t.phase = WORK
        tx.retire_reviews(t, None, "new executor round")
    tx.add_pending(t, "register_watch", OUTBOX, bound={"execution_id": e.execution_id},
                   payload={"session": session, "role": "executor"})
    tx.detail = e.execution_id


def _resume_in_place(tx: _Tx, t: Ticket, prev: Execution, session: str | None, p: JsonObj,
                     override: str | None) -> None:
    """Design §3.1: resuming the same session keeps its execution record. The send is recorded as a
    new segment of that execution; the finished result of the previous segment moves to history, and
    facts tagged with an older segment are stale."""
    prev.history.append({"segment": prev.segment, "result": prev.result, "observed": prev.observed,
                         "resumed_at": tx.op.at, "op_id": tx.op.op_id, "contract": _opt_str(p, "contract"),
                         "provider": _opt_str(p, "provider"), "model": _opt_str(p, "model"),
                         "effort": _opt_str(p, "effort"), "override_reason": override})
    prev.segment += 1
    prev.session = prev.session or session
    prev.result = None
    prev.observed, prev.observed_at = "starting", tx.op.at
    tx.emit("execution.resumed", t.ticket, execution_id=prev.execution_id, segment=prev.segment)
    tx.complete_by_command(t)
    if t.phase == REVIEW:
        t.phase = WORK
        tx.retire_reviews(t, None, "executor resumed")
    tx.detail = prev.execution_id


def _request_reviewer(tx: _Tx, t: Ticket, p: JsonObj, explicit: str | None, session: str | None) -> None:
    axis = _req_str(p, "axis", AXES)
    if t.phase != REVIEW or t.candidate is None:
        raise Rejected(f"{t.ticket} has no candidate under review")
    if axis in t.reviews_for_current() and t.reviews_for_current()[axis].verdict:
        raise Rejected(f"{axis} already has a verdict on candidate {t.candidate.digest}")
    running = t.current_reviewer(axis)
    if running is not None and running.observed in ACTIVE:
        raise Rejected(f"{axis} review already running ({running.execution_id})")
    mode = "adopt" if _opt_str(p, "mode") == "adopt" else "review"
    if mode == "adopt" and not session:
        raise ValidationError("adopt needs the existing session")
    if mode == "review":
        for key in ("provider", "model", "effort", "contract"):
            _req_str(p, key)
    e = Execution(explicit or tx.new_id(f"exe:{t.ticket}:{axis}"), t.attempt, "reviewer", mode, tx.op.at,
                  "running" if mode == "adopt" else "starting", tx.op.at, axis=axis,
                  candidate_digest=t.candidate.digest, provider=_opt_str(p, "provider"), model=_opt_str(p, "model"),
                  effort=_opt_str(p, "effort"), session=session, contract=_opt_str(p, "contract"))
    t.executions.append(e)
    review = Review(_opt_str(p, "review_request_id") or tx.new_id(f"rev:{t.ticket}:{axis}"), axis,
                    t.candidate.digest, e.execution_id)
    t.reviews.append(review)
    tx.emit("execution.requested", t.ticket, execution_id=e.execution_id, mode=mode, axis=axis,
            review_request_id=review.review_request_id, candidate_digest=t.candidate.digest)
    tx.complete_by_command(t)
    tx.add_pending(t, "register_watch", OUTBOX, bound={"execution_id": e.execution_id},
                   payload={"session": session, "role": "reviewer", "axis": axis})
    tx.detail = review.review_request_id


def _question_ask(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is closed")
    to, key, title = _req_str(p, "to", ("advisor", "user")), _req_str(p, "key"), _req_str(p, "title")
    open_same = next((q for q in t.questions if q.key == key and q.answer is None), None)
    if open_same is not None:
        tx.noop(f"question {key} is already open as {open_same.question_id}")
        tx.detail = open_same.question_id
        return
    qid = _opt_str(p, "question_id") or tx.new_id(f"q:{t.ticket}:{key}")
    if any(q.question_id == qid for x in tx.tickets.values() for q in x.questions):
        raise Rejected(f"question_id {qid} already exists")
    on_sat = p.get("on_satisfied")
    asked_at = _opt_str(p, "asked_at")  # B4 `question link`: the advisor question was asked earlier
    if asked_at is not None and parse_at(asked_at) > parse_at(tx.op.at):
        raise ValidationError(f"asked_at {asked_at} is after the op time {tx.op.at}")
    _ask(tx, t, qid, to, key, title, _opt_str(p, "question_file"),
         Continuation.load(on_sat) if on_sat is not None else None, asked_at=asked_at)
    tx.complete_by_command(t)
    tx.detail = qid


def _ask(tx: _Tx, t: Ticket, qid: str, to: str, key: str, title: str, qfile: str | None,
         on_sat: Continuation | None, *, asked_at: str | None = None) -> Question:
    revision = 1 + sum(1 for q in t.questions if q.key == key)
    q = Question(qid, revision, to, key, title, asked_at or tx.op.at, qfile)
    t.questions.append(q)
    tx.add_wait(t, "question", qid, note=title,
                on_satisfied=on_sat or Continuation("apply_decision", {"question_id": qid}))
    if to == "user":
        tx.add_pending(t, "answer_user", DECIDE, bound={"question_id": qid}, payload={"key": key, "title": title})
        tx.add_pending(t, "push", OUTBOX, bound={"question_id": qid}, payload={"key": key, "title": title,
                                                                                 "reason": "needs_user"})
    return q


def _decision_respond(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    if tx.op.expected_revision is None:
        raise ValidationError("decision.respond needs expected_revision")
    response = _req_str(p, "response")
    qid = _opt_str(p, "question_id")
    if qid and p.get("action_id") is None:  # the user may answer by question id (review H2)
        found = next((x for x in t.pending if x.kind == "answer_user" and x.bound.get("question_id") == qid
                      and x.status in OUTSTANDING), None)
        if found is None:
            raise Rejected(f"no open user question {qid} on {t.ticket}")
        action_id = found.action_id
    else:
        action_id = _req_str(p, "action_id")
    pend = t.pending_by_id(action_id)
    if pend is None or pend.mode != DECIDE:
        raise Rejected(f"no decision {action_id} on {t.ticket}")
    if pend.status not in OUTSTANDING:  # escalation only notifies; an escalated decision stays answerable
        raise Rejected(f"decision {action_id} is {pend.status}")
    if response not in pend.allowed_actions:
        raise Rejected(f"{response} is not allowed for {pend.kind}; allowed: {pend.allowed_actions}")
    if response not in DECIDE_RESPONSES:
        raise Rejected(f"{response} is a command: submit it directly; the decision closes when it is accepted")
    reason = str(p.get("reason") or p.get("text") or "")
    if response == "hold":
        if not reason:
            raise ValidationError("hold needs a reason")
        pend.status, pend.updated_at, pend.detail = CLAIMED_P, tx.op.at, reason
        tx.emit("pending.claimed", t.ticket, action_id=action_id, reason=reason)
        return
    if response == "escalate":
        if pend.status == ESCALATED:
            tx.noop("already escalated")
            return
        tx.close_pending(t, pend, ESCALATED, reason or "escalated by coordinator")
        return
    if response == "answer":
        if tx.op.actor != "user":
            raise Rejected("only the user answers a user question")
        if not reason:
            raise ValidationError("answer needs text")
        q = t.question_by_id(pend.bound.get("question_id", ""))
        if q is None or q.answer is not None:
            raise Rejected("the question is gone or already answered")
        q.answer = {"verdict": str(p.get("verdict") or "answered"), "text": reason,
                    "source_record": f"op:{tx.op.op_id}", "at": tx.op.at}
        if q.answer["verdict"] == "decided":
            _page_review_answer(tx, t, q, reason)
        for w in t.waits:
            if w.kind == "question" and w.ref == q.question_id and w.open:
                tx.satisfy(t, w, f"op:{tx.op.op_id}", {"verdict": q.answer["verdict"]})
    if response == "acknowledge":
        pend.detail = f"{ACKNOWLEDGED}: {reason}" if reason else ACKNOWLEDGED  # lifts a recovery pause
    tx.close_pending(t, pend, DONE_P, f"response {response}")


def _wait_add(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is closed")
    kind, ref = _req_str(p, "kind"), _req_str(p, "ref")
    if kind not in COORDINATOR_WAIT_KINDS:
        raise Rejected(f"wait.add accepts {sorted(COORDINATOR_WAIT_KINDS)}; {kind} waits come from "
                       "question.ask / pagewindow.enqueue / gate facts")
    wait_id = _opt_str(p, "wait_id")
    if wait_id and any(w.wait_id == wait_id for x in tx.tickets.values() for w in x.waits):
        raise Rejected(f"wait_id {wait_id} exists; use wait.update to change it")
    if any(w.open and w.kind == kind and w.ref == ref for w in t.waits):
        tx.noop(f"already waiting on {kind}:{ref}")
        return
    if kind == "ticket":
        target = tx.tickets.get(ref)
        if target is None:
            raise Rejected(f"wait ticket:{ref}: no such ticket")
        if ref == t.ticket:
            raise Rejected("a ticket cannot wait on itself")
        if accepted(target):
            tx.noop(f"{ref} is already accepted; nothing to wait for")
            return
        if target.closure is not None:
            raise Rejected(f"{ref} is closed {target.closure.kind}; it will never be accepted")
    elif kind == "batch":
        name, _, cond = ref.rpartition(":")
        if cond not in ("bound", "completed") or not name:
            raise ValidationError("batch wait ref is <batch>:bound or <batch>:completed")
        if _batch_condition(tx.tickets, name, cond):
            tx.noop(f"batch {ref} already holds")
            return
    elif kind == "time":
        if parse_at(ref) <= parse_at(tx.op.at):
            tx.noop(f"time {ref} is not in the future")
            return
    on_sat = p.get("on_satisfied")
    detail: JsonObj | None = None
    if p.get("detail") is not None:
        if kind != "external":
            raise ValidationError("wait.add detail is only for external waits (owner, attempts, recheck_at)")
        detail = external_detail(p.get("detail"))
    tx.add_wait(t, kind, ref, wait_id=wait_id, note=str(p.get("note") or ""),
                on_satisfied=Continuation.load(on_sat) if on_sat is not None else None, detail=detail)
    tx.complete_by_command(t)


def _wait_update(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    w = t.wait_by_id(_req_str(p, "wait_id"))
    if w is None or not w.open:
        raise Rejected("no such open wait")
    if w.kind not in ("time", "external"):
        raise Rejected("only time and external waits are rescheduled/reworded")
    reason, ref = _req_str(p, "reason"), _req_str(p, "ref")
    if w.kind == "time":
        parse_at(ref)
    if p.get("detail") is not None:
        if w.kind != "external":
            raise ValidationError("wait.update detail is only for external waits (owner, attempts, recheck_at)")
        w.detail = external_detail(p.get("detail"))
    if w.kind == "external":
        w.detail["revised_at"] = tx.op.at  # the new revision's episode is timed from here (HD-01 B)
    w.ref, w.revision = ref, w.revision + 1
    w.note = str(p.get("note") or w.note)
    tx.emit("wait.updated", t.ticket, wait_id=w.wait_id, ref=ref, reason=reason, revision=w.revision)


def _wait_waive(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    w = t.wait_by_id(_req_str(p, "wait_id"))
    reason = _req_str(p, "reason")
    if w is None:
        raise Rejected("no such wait")
    if not w.open:
        tx.noop("wait already closed")
        return
    if w.kind not in WAIVABLE:
        raise Rejected(f"{w.kind} waits close only by their fact (answer / fix / page acquire), not by waive")
    if w.kind == "external" and tx.op.actor != "user":
        raise Rejected("only the user waives an external wait")
    w.waived = {"by": tx.op.actor, "reason": reason, "at": tx.op.at}
    tx.emit("wait.waived", t.ticket, wait_id=w.wait_id, reason=reason)
    tx.complete_by_command(t)


def _pagewindow_enqueue(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is closed")
    spec = _opt_str(p, "spec")
    grant, checked = _flag(p, "grant"), _flag(p, "checked")
    if not (spec and grant and checked):
        missing = [n for n, ok in (("spec", bool(spec)), ("grant", grant), ("offline check", checked)) if not ok]
        tx.add_pending(t, "prepare_window", DECIDE, bound={"spec": spec or ""},
                       payload={"missing": list(missing)}, detail="missing " + ", ".join(missing))
        tx.detail = "not queued: missing " + ", ".join(missing)
        return
    if any(w.open and w.kind == "page-window" for w in t.waits):
        raise Rejected(f"{t.ticket} already has a queued page window; one at a time")
    entry = _opt_str(p, "entry_id") or tx.new_id(f"pw:{t.ticket}")
    on_rel = p.get("on_released")
    on_released = Continuation.load(on_rel) if on_rel is not None else Continuation()
    tx.add_wait(t, "page-window", entry, note=spec or "",
                detail={"spec": spec, "on_released": json.loads(json.dumps(asdict(on_released)))})
    tx.add_pending(t, "run_window", OUTBOX, bound={"entry_id": entry},
                   payload={"spec": spec, "spec_digest": p.get("spec_digest"), "priority": _opt_int(p, "priority") or 0,
                            "needs_guest": _flag(p, "needs_guest"), "est_seconds": _opt_int(p, "est_seconds"),
                            "on_released": json.loads(json.dumps(asdict(on_released)))})
    tx.complete_by_command(t)
    tx.detail = entry


def _pagewindow_policy(tx: _Tx) -> None:
    """§5 `pagewindow.policy`: ordering only. Changes the priority of this ticket's queued window (the
    controller takes the first eligible entry by priority, then age) and records why; it never names
    who runs next and never touches the library's registry."""
    t, p = tx.ticket(), tx.op.payload
    priority, reason = _opt_int(p, "priority"), _req_str(p, "reason")
    if priority is None:
        raise ValidationError("pagewindow.policy needs priority")
    entry = _opt_str(p, "entry_id")
    rows = [x for x in t.pending if x.kind == "run_window" and x.open
            and (entry is None or x.bound.get("entry_id") == entry)]
    if not rows:
        raise Rejected(f"{t.ticket} has no queued page window" + (f" {entry}" if entry else ""))
    if len(rows) > 1:
        raise Rejected(f"{t.ticket} has several queued windows; name entry_id")
    row = rows[0]
    before = _opt_int(row.payload, "priority") or 0
    if before == priority:
        tx.noop(f"priority already {priority}")
        return
    row.payload["priority"] = priority
    row.updated_at = tx.op.at
    tx.emit("pagewindow.policy", t.ticket, entry_id=row.bound.get("entry_id", ""), before=before, after=priority,
            reason=reason)
    tx.detail = row.bound.get("entry_id", "")


def _ticket_close(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    if t.phase == CLOSED:
        raise Rejected(f"{t.ticket} is already closed {t.closure.kind if t.closure else ''}")
    if p.get("legacy_reconcile") is not None:
        _legacy_reconcile_close(tx, t, p)
        return
    kind = _req_str(p, "kind", CLOSURE_KINDS)
    closure = Closure(kind, tx.op.at, reason=str(p.get("reason") or ""))
    if kind == "accepted":
        unmet = [d for d in t.deps if not accepted(tx.tickets.get(d))]
        if unmet:
            raise Rejected(f"{t.ticket} cannot be accepted before its prerequisites: {', '.join(unmet)}")
        if t.candidate is None:
            raise Rejected(f"{t.ticket} has no candidate")
        if t.kind in ("tool", "doc") and (not t.kind_proof or
                (t.kind_proof.get("status") != "verified" and not (t.kind_proof.get("method") == "release_coverage_missing"
                                                     and not _release_coverage_importable())) or
                t.kind_proof.get("kind") != t.kind or
                (t.kind_proof.get("ticket_source") or {}).get("candidate") != t.candidate.digest):
            raise Rejected("KIND_PROOF_STALE: reclassify the current candidate before acceptance")
        if t.candidate.from_execution.startswith("imported:") and not p.get("waive_reviews"):
            raise Rejected("imported candidate has no first-hand declaration; observe a fresh result or explicitly waive")
        digest = _req_str(p, "candidate_digest")
        if digest != t.candidate.digest:
            raise Rejected(f"candidate_digest {digest} is not the current candidate {t.candidate.digest}")
        if t.active("executor"):
            raise Rejected(f"{t.ticket} still has a running executor")
        if t.open_waits():
            raise Rejected(f"{t.ticket} still waits on " + ", ".join(f"{w.kind}:{w.ref}" for w in t.open_waits()))
        if t.window_running():
            raise Rejected(f"{t.ticket} has a page window acquired and not released; accept after it closes")
        waive = _opt_str(p, "waive_reviews")
        if not waive:
            bound = t.reviews_for_current()
            missing = [a for a in AXES if (bound.get(a) is None or bound[a].verdict != "PASS")]
            if missing:
                raise Rejected(f"reviews bound to candidate {digest} are not PASS on {missing} "
                               "(or give waive_reviews with a reason)")
        scope = _req_str(p, "scope", SCOPES)
        affects = _str_list(p, "affects_downstream")
        if scope != "FULL" and affects is None:
            raise ValidationError(f"scope {scope} needs affects_downstream (may be an explicit empty list)")
        closure.scope, closure.candidate_digest, closure.waive_reviews = scope, digest, waive
        closure.affects_downstream = affects
    elif kind == "superseded":
        by = _req_str(p, "by")
        if by not in tx.tickets or by == t.ticket:
            raise Rejected(f"superseded by {by}: no such other ticket")
        closure.superseded_by = by
    elif not closure.reason:
        raise ValidationError(f"close {kind} needs a reason")
    t.phase, t.closure = CLOSED, closure
    if kind == "accepted":
        try:
            from release_coverage import delivery_obligation
        except ImportError:
            _warn_missing_delivery("release_coverage")
        else:
            t.release_obligation = delivery_obligation(t.ticket, dict(
                candidate=t.candidate.digest, release_group=t.release_group,
                requires_for_release=t.requires_for_release), tx.op.at)
    tx.complete_by_command(t)
    for e in t.executions:
        if e.observed in ACTIVE or e.observed == "unknown":
            tx.add_pending(t, "cleanup", OUTBOX, bound={"execution_id": e.execution_id},
                           payload={"session": e.session, "role": e.role})
    for pend in t.pending:
        if pend.status in OUTSTANDING and pend.kind not in ("cleanup", "push"):
            tx.close_pending(t, pend, SUPERSEDED_P, f"ticket closed {kind}")
    if kind == "accepted":
        tx.add_pending(t, "push", OUTBOX, bound={"closure": "accepted"},
                       payload={"reason": "accepted", "scope": closure.scope})
    tx.emit("ticket.closed", t.ticket, kind=kind)


LEGACY_RECONCILE_WAIVER = "legacy run.json closure reconciled; the core holds no first-hand review verdicts"


def _legacy_reconcile_close(tx: _Tx, t: Ticket, p: JsonObj) -> None:
    """Backfill an acceptance that run.json (the authority in shadow mode) already recorded but whose
    mirror was lost or refused (core-mirror). Only the ``seed`` actor (`host-dag sm reconcile`)
    may submit it, only ``accepted`` is backfilled, and the closure says where it came from: the legacy
    state/status, the run.json digest and why. No push is queued (the legacy write already pushed), open
    waits are waived and outstanding actions superseded, as an accepted close would require."""
    if tx.op.actor != "seed":
        raise Rejected("legacy_reconcile closures come only from `host-dag sm reconcile` (actor seed)")
    if _req_str(p, "kind", CLOSURE_KINDS) != "accepted":
        raise Rejected("legacy_reconcile backfills accepted closures only")
    legacy = _obj(p.get("legacy_reconcile"), "legacy_reconcile")
    state, status = _opt_str(legacy, "state"), _opt_str(legacy, "status")
    if "ACCEPTED" not in (status, state) and state != "INTEGRATED":
        raise Rejected(f"run.json does not record {t.ticket} as accepted (state {state}, status {status})")
    source, reason = _req_str(legacy, "run_json_sha256"), _req_str(p, "reason")
    scope = _opt_str(legacy, "accept_scope", SCOPES) or "FULL"
    closure = Closure("accepted", tx.op.at, scope=scope, reason=f"reconciled from run.json ({source[:12]}): {reason}",
                      candidate_digest=t.candidate.digest if t.candidate is not None else None,
                      affects_downstream=None if scope == "FULL" else (_str_list(legacy, "affects_downstream") or []),
                      waive_reviews=LEGACY_RECONCILE_WAIVER)
    if state == "INTEGRATED":
        closure.integrated = {"tip": str(legacy.get("accepted_tip") or ""), "readback": "run.json state INTEGRATED",
                              "merge_base_evidence": f"legacy run.json {source}", "at": tx.op.at,
                              "integration_verification": "imported_unverified"}
    for w in t.open_waits():
        w.waived = {"by": tx.op.actor, "reason": "ticket closed by legacy reconcile", "at": tx.op.at}
    t.phase, t.closure = CLOSED, closure
    for e in t.executions:
        if e.observed in ACTIVE or e.observed == "unknown":
            tx.add_pending(t, "cleanup", OUTBOX, bound={"execution_id": e.execution_id},
                           payload={"session": e.session, "role": e.role})
    for pend in t.pending:
        if pend.status in OUTSTANDING and pend.kind not in ("cleanup", "push"):
            tx.close_pending(t, pend, SUPERSEDED_P, "ticket closed by legacy reconcile")
    tx.emit("ticket.closed", t.ticket, kind="accepted", legacy_reconcile={
        "state": state, "status": status, "run_json_sha256": source, "integrated": state == "INTEGRATED"})
    tx.detail = f"reconciled from run.json {state or status}"


def _ticket_classify(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    kind = _req_str(p, "kind").lower()
    if kind not in ("tool", "doc", "product", "ops"):
        raise ValidationError("kind must be tool|doc|product|ops")
    source = copy.deepcopy(_opt_obj(p, "ticket_source") or {})
    if t.candidate is not None and source.get("candidate") != t.candidate.digest:
        raise Rejected("kind classification must bind the current core candidate")
    if tx.kind_fact is not None:
        proof = tx.kind_fact
    elif kind in ("tool", "doc"):
        try:
            from release_coverage import verify_kind_paths
        except ImportError:
            _warn_missing_delivery("release_coverage")
            proof = dict(status="skipped", method="release_coverage_missing", ticket_source=source, sources=[])
        else:
            try:
                proof = verify_kind_paths(source, p.get("run_dir"), kind=kind, ticket_id=t.ticket)
            except (ValueError, OSError, TypeError) as exc:
                raise Rejected(str(exc)) from exc
    else:
        proof = dict(status="verified", method="non_tool", ticket_source=source, sources=[])
    proof = dict(proof, kind=kind)
    tx.op = replace(tx.op, kind_proof=copy.deepcopy(proof))
    t.kind, t.kind_proof = kind, copy.deepcopy(proof)
    tx.emit("ticket.classified", t.ticket, kind=kind)


def _ticket_integrate(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    digest = _req_str(p, "candidate_digest")
    if not accepted(t) or t.closure is None:
        raise Rejected(f"{t.ticket} is not accepted")
    if digest != t.closure.candidate_digest:
        raise Rejected(f"candidate_digest {digest} is not the accepted candidate")
    if tx.integration_fact is not None:
        proof = tx.integration_fact
    else:
        try:
            from release_coverage import verify_integration
        except ImportError:
            _warn_missing_delivery("release_coverage")
            proof = None
        else:
            # Tool head only for a path-verified classification bound to this very candidate.
            tool = (t.kind == "tool" and isinstance(t.kind_proof, dict) and t.kind_proof.get("status") == "verified"
                    and (t.kind_proof.get("ticket_source") or {}).get("candidate") == digest)
            try:
                proof = verify_integration(digest, p.get("run_dir"), p.get("integrate_heads"), tool=tool)
            except ValueError as exc:
                raise Rejected(str(exc)) from exc
    if proof is not None:
        tx.op = replace(tx.op, integration_proof=copy.deepcopy(proof))
        # Caller-supplied readback/tip/merge_base_evidence are never proof or targets.
        record = {"tip": ",".join(k + ":" + v["sha"] for k, v in proof["heads"].items()),
                  "integration_verification": "verified", "at": tx.op.at,
                  "git_evidence": json.dumps(proof, ensure_ascii=False, sort_keys=True)}
    else:
        record = {"tip": "", "integration_verification": "skipped_release_coverage_missing", "at": tx.op.at}
    t.closure.integrated = record


def _authority_switch(tx: _Tx) -> None:
    p = tx.op.payload
    epoch, mode = _opt_int(p, "epoch"), _req_str(p, "mode", MODES)
    if epoch is None:
        raise ValidationError("authority.switch needs epoch")
    if epoch < tx.meta.authority_epoch:
        raise Rejected(f"epoch {epoch} is older than {tx.meta.authority_epoch}")
    if mode == "core" and epoch <= tx.meta.authority_epoch:
        raise Rejected("switching to core needs a new epoch")
    if (epoch, mode) == (tx.meta.authority_epoch, tx.meta.mode):
        tx.noop("already there")
        return
    tx.meta.authority_epoch, tx.meta.mode = epoch, mode
    tx.emit("authority.switched", None, epoch=epoch, mode=mode)


def _resource_unpause(tx: _Tx) -> None:
    """Explicit unpause (review M4). Automatic unpause happens when the recovery decision is resolved
    or the drift question is answered (see _settle_paused); this is the manual path with a reason."""
    p = tx.op.payload
    resource, reason = _req_str(p, "resource"), _req_str(p, "reason")
    if resource not in tx.meta.paused:
        tx.noop(f"{resource} is not paused")
        return
    held = tx.meta.paused.pop(resource)
    tx.emit("resource.unpaused", None, resource=resource, held_by=held, reason=reason, by=tx.op.actor)


# ---- facts


def _execution_observed(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    eid, observed = _req_str(p, "execution_id"), _req_str(p, "observed", OBSERVED)
    session = _opt_str(p, "session")
    e = t.execution(eid)
    if e is None:
        tx.stale(f"execution {eid} is not recorded on {t.ticket}")
        return
    if session and e.session and session != e.session:
        tx.stale(f"session {session} is not the session of {eid}")
        return
    segment = _opt_int(p, "segment")
    if segment is not None and segment != e.segment:
        tx.stale(f"fact for segment {segment} of {eid}, which is at segment {e.segment} (resumed since)")
        return
    if segment is None and e.segment > 0:
        # Review R2: after a resume an untagged fact may be a late one from an earlier segment; only
        # facts tagged with the current segment move the execution (B5: the watcher tags segments).
        tx.stale(f"{eid} was resumed (segment {e.segment}); facts must carry segment={e.segment}")
        return
    if e.observed == "orphaned":
        cleanup = [x for x in t.pending if x.open and x.kind == "cleanup" and x.bound.get("execution_id") == eid]
        if observed == "finished" and cleanup:
            for pend in cleanup:  # the orphaned session ended by itself: nothing left to stop
                tx.close_pending(t, pend, DONE_P, "orphaned execution finished")
            tx.detail = "orphaned execution finished; cleanup done, result ignored"
            return
        tx.stale(f"{eid} is orphaned (its candidate left review); fact ignored")
        return
    current = t.current_executor() if e.role == "executor" else t.current_reviewer(e.axis or "")
    if t.phase != CLOSED and e is not current:
        tx.stale(f"{eid} is not the current {e.role} execution (late fact of an older attempt/candidate)")
        return
    if e.observed == "finished" and observed != "finished":
        tx.stale(f"{eid} already finished")
        return
    result = _opt_obj(p, "result")
    if result is not None and observed != "finished":
        raise ValidationError("result only with observed=finished")
    if observed == "orphaned":
        raise ValidationError("orphaned is set by the core, not observed")
    # Segment identity (above) separates resumed segments; the same result in two segments is valid
    # (review R1), so results are never compared with history.
    if observed == e.observed and observed != "finished" and (not session or session == e.session):
        tx.noop("heartbeat")  # review M7: no revision bump, no audit event
        tx.quiet = True
        return
    completing = False
    if observed == "finished" and e.observed == "finished":
        if result is None or result == e.result:
            tx.noop("same finish already recorded")
            return
        old = e.result or {}
        if any(k in result and result[k] != v for k, v in old.items() if v is not None) or \
                any(k not in result for k, v in old.items() if v is not None):
            raise Rejected(f"conflicting result for {eid}: recorded {canonical(old)}, got {canonical(result)}")
        completing = True
    if session and not e.session:
        e.session = session
    e.observed, e.observed_at = observed, tx.op.at
    if result is not None:
        e.result = result
    tx.emit("execution.observed", t.ticket, execution_id=eid, observed=observed, completing=completing)
    if observed != "unknown":  # back in sight: the executor_unknown decision for it is moot (B3)
        for pend in t.pending:
            if pend.kind == "executor_unknown" and pend.status in OUTSTANDING \
                    and pend.bound.get("execution_id") == eid:
                tx.close_pending(t, pend, SUPERSEDED_P, f"execution observed {observed}")
    if t.phase == CLOSED:
        if observed in ("finished",):
            for pend in t.pending:
                if pend.open and pend.kind == "cleanup" and pend.bound.get("execution_id") == eid:
                    tx.close_pending(t, pend, DONE_P, "execution finished")
        return
    if e.role != "executor" or observed != "finished":
        return
    candidate = str((e.result or {}).get("candidate") or "")
    if candidate:
        t.phase = REVIEW
        if t.kind in ("tool", "doc") and (not t.kind_proof or
                (t.kind_proof.get("ticket_source") or {}).get("candidate") != candidate):
            t.kind_proof = None
            tx.emit("ticket.classification_invalidated", t.ticket, candidate=candidate)
        t.candidate = Candidate(candidate, str((e.result or {}).get("evidence") or ""), eid)
        tx.supersede(t, ("triage_result", "dispatch", "executor_unknown"), "candidate arrived")
        tx.retire_reviews(t, candidate, "candidate replaced")
        _queue_reviews(tx, t)
    else:
        tx.add_pending(t, "triage_result", DECIDE, bound={"execution_id": eid},
                       payload={"result": e.result or {}})


def _queue_reviews(tx: _Tx, t: Ticket) -> None:
    """Two independent dispatch obligations (§6.2), only for axes without a verdict on this digest
    (a resume that reproduces the same digest keeps the verdicts it already has)."""
    if t.phase != REVIEW or t.candidate is None:
        return
    digest = t.candidate.digest
    bound = t.reviews_for_current()
    for axis in AXES:
        if bound.get(axis) is not None and bound[axis].verdict:
            continue
        running = t.current_reviewer(axis)
        if running is not None and running.observed in ACTIVE:
            continue  # B7 adopts existing reviews; a matching executor result must not dispatch them twice
        tx.add_pending(t, "request_reviews", OUTBOX, bound={"axis": axis, "candidate_digest": digest},
                       payload={"evidence": t.candidate.evidence})
    if all(bound.get(a) is not None and bound[a].verdict for a in AXES):
        tx.add_pending(t, "triage_reviews", DECIDE, bound={"candidate_digest": digest},
                       payload={a: f"{bound[a].verdict}/{bound[a].max_severity or '-'}" for a in AXES})


def _review_observed(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    rid, digest = _req_str(p, "review_request_id"), _req_str(p, "candidate_digest")
    axis, verdict = _req_str(p, "axis", AXES), _req_str(p, "verdict", VERDICTS)
    severity = _opt_str(p, "max_severity", SEVERITIES)
    if verdict == "FAIL" and severity is None:
        raise ValidationError("a FAIL needs max_severity")
    review = next((r for r in t.reviews if r.review_request_id == rid), None)
    if review is None or review.axis != axis or review.candidate_digest != digest:
        tx.stale(f"review {rid} ({axis}) on {digest} does not match a requested review")
        return
    reviewer = t.execution(review.execution_id)
    if reviewer is not None and reviewer.observed == "orphaned":
        tx.stale(f"review {rid} came from an orphaned reviewer (its round was reopened)")
        return
    if t.phase != REVIEW or t.candidate is None or t.candidate.digest != digest:
        tx.stale(f"review {rid} is bound to candidate {digest}, not the current one")
        return
    if review.verdict is not None:
        if (review.verdict, review.max_severity) == (verdict, severity):
            tx.noop("same verdict already recorded")
            return
        raise Rejected(f"conflicting verdict for {rid}: {review.verdict}/{review.max_severity} vs {verdict}/{severity}")
    review.verdict, review.max_severity = verdict, severity
    reviewer = t.execution(review.execution_id)
    if reviewer is not None and reviewer.observed != "finished":
        reviewer.observed, reviewer.observed_at = "finished", tx.op.at
    tx.emit("review.observed", t.ticket, review_request_id=rid, axis=axis, verdict=verdict, max_severity=severity)
    bound = t.reviews_for_current()
    if all(bound.get(a) is not None and bound[a].verdict for a in AXES):
        tx.add_pending(t, "triage_reviews", DECIDE, bound={"candidate_digest": digest},
                       payload={a: f"{bound[a].verdict}/{bound[a].max_severity or '-'}" for a in AXES})


PAGE_REVIEW_COOLDOWN_SECONDS = 5 * 60


def page_review_key(t: Ticket, entry: str, reason: str, at: str) -> str | None:
    """Closed-ticket findings ask once; unresolved identity reviews re-ask after cooldown."""
    base = f"pagewindow-review:{entry}:{reason}"
    questions = [q for q in t.questions if q.key == base or q.key.startswith(base + ":retry:")]
    if not questions:
        return base
    if reason == "closed_ticket_window_activity" or any(q.answer is None for q in questions):
        return None
    answered_at = max(parse_at(str(q.answer["at"])) for q in questions if q.answer is not None)
    if (parse_at(at) - answered_at).total_seconds() < PAGE_REVIEW_COOLDOWN_SECONDS:
        return None
    return f"{base}:retry:{len(questions)}"


def _page_review_answer(tx: _Tx, t: Ticket, q: Question, text: str) -> None:
    """Only an explicit JSON action disposes of an identity review. Prose/verdicts never do."""
    for w in t.waits:
        reason = w.detail.get("identity_review_reason") or "ambiguous_claims"
        base = f"pagewindow-review:{w.ref}:{reason}"
        if (w.kind != "page-window" or not w.detail.get("identity_review_required")
                or not (q.key == base or q.key.startswith(base + ":retry:"))):
            continue
        try:
            action = json.loads(text)
        except ValueError:
            return
        if not isinstance(action, dict):
            return
        if (action.get("action") == "bind_claim" and isinstance(action.get("claim_id"), str)
                and action["claim_id"]):
            # The resolver must still authenticate this exact claim's complete receipt and lease.
            w.detail["identity_review_request"] = {"claim_id": action["claim_id"], "question_id": q.question_id}
        elif action.get("action") == "abandon_window":
            w.detail["outcome"] = "ABANDONED"
            w.detail["identity_review_required"] = False
            w.detail.pop("identity_review_request", None)
            w.waived = {"by": tx.op.actor, "reason": "advisor abandoned window entry", "at": tx.op.at}
            w.continued = True
            for run in t.pending:
                if run.kind == "run_window" and run.bound.get("entry_id") == w.ref and run.open:
                    tx.close_pending(t, run, DONE_P, "advisor abandoned window entry")
            tx.emit("pagewindow.abandoned", t.ticket, entry_id=w.ref, question_id=q.question_id)


def _question_answered(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    qid, source = _req_str(p, "question_id"), _req_str(p, "source_record")
    revision, verdict = _opt_int(p, "revision"), _req_str(p, "verdict")
    q = t.question_by_id(qid)
    if q is None:
        tx.stale(f"question {qid} is not recorded on {t.ticket}")
        return
    if revision is not None and revision != q.revision:
        tx.stale(f"answer is for revision {revision}, question {qid} is revision {q.revision}")
        return
    if q.to != "advisor":
        raise Rejected("a user question is answered with decision.respond")
    if tx.already_imported():
        tx.noop(f"source record {source} already imported for {qid}")
        return
    if q.answer is not None:
        if q.answer.get("source_record") == source:
            tx.noop("same answer already recorded")
            return
        raise Rejected(f"question {qid} already answered from {q.answer.get('source_record')}")
    text = str(p.get("text") or "")
    q.answer = {"verdict": verdict, "text": text, "source_record": source, "at": tx.op.at}
    if verdict == "decided":
        _page_review_answer(tx, t, q, text)
    tx.mark_imported()
    for w in t.waits:
        if w.kind == "question" and w.ref == qid and w.open:
            tx.satisfy(t, w, source, {"verdict": verdict})
            if verdict == "needs_user" and t.phase != CLOSED:
                w.continued = True  # the advisor handed it to the user; the user's answer continues it
                keep = w.on_satisfied if w.on_satisfied.kind != "apply_decision" else None
                user_q = _ask(tx, t, tx.new_id(f"q:{t.ticket}:{q.key}:user"), "user", q.key,
                              q.title, q.question_file, keep)
                tx.emit("question.escalated", t.ticket, question_id=qid, to=user_q.question_id)
                for resource, holder in list(tx.meta.paused.items()):  # a drift pause follows the question
                    if holder == f"question:{qid}":
                        tx.meta.paused[resource] = f"question:{user_q.question_id}"


def _wait_satisfied(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    wid, source = _req_str(p, "wait_id"), _req_str(p, "source_record")
    w = t.wait_by_id(wid)
    if w is None:
        tx.stale(f"wait {wid} is not recorded on {t.ticket}")
        return
    if w.kind not in FACT_SATISFIABLE:
        raise Rejected(f"{w.kind} waits are closed by their own fact, not wait.satisfied")
    revision = _opt_int(p, "revision")
    if revision is not None and revision != w.revision:
        tx.stale(f"observation for wait revision {revision}, wait is revision {w.revision}")
        return
    if not w.open:
        tx.noop("wait already closed")
        return
    if tx.already_imported():
        tx.noop(f"source record {source} already imported for {wid}")
        return
    observed_at = _req_str(p, "observed_at")
    if w.kind == "time" and parse_at(observed_at) < parse_at(w.ref):
        raise Rejected(f"time wait {w.ref} is not due at {observed_at}")
    value = _opt_obj(p, "value") or {}
    tx.satisfy(t, w, source, {"observed_at": observed_at, **value})
    tx.mark_imported()


def _resource_observed(tx: _Tx) -> None:
    p = tx.op.payload
    resource, event = _req_str(p, "resource"), _req_str(p, "event")
    events = ("acquired", "released", "claim_failed", "blocked", "reading", "lost", "check_failed", "launch_failed",
              "needs_review")
    if event not in events:
        raise ValidationError(f"event must be one of {events}")
    if event == "reading":
        tx.emit("resource.reading", tx.op.ticket, resource=resource, value=_opt_obj(p, "value") or {})
        return
    t = tx.ticket()
    source = _opt_str(p, "source_record")
    if tx.already_imported():
        tx.noop(f"source record {source} already imported")
        return
    tx.mark_imported()  # kept only if this op is accepted (reduce drops it for noop/stale, review L1)
    if event == "blocked":
        gate = _obj(p.get("gate"), "gate")
        kind, ref = _req_str(gate, "kind"), _req_str(gate, "ref")
        if kind not in GATE_WAIT_KINDS:
            raise Rejected(f"gate kind must be one of {sorted(GATE_WAIT_KINDS)}")
        if t.phase == CLOSED:
            tx.stale("ticket closed")
            return
        if any(w.open and w.kind == kind and w.ref == ref for w in t.waits):
            tx.noop(f"already waiting on {kind}:{ref}")
            return
        on_sat = p.get("on_satisfied")
        tx.add_wait(t, kind, ref, detail=gate, note=str(gate.get("note") or ""),
                    on_satisfied=Continuation.load(on_sat) if on_sat is not None else None)
        return
    if resource != "page-window":
        raise ValidationError(f"{event} is a page-window event")
    entry = _req_str(p, "entry_id")
    if event == "needs_review":
        reason = _req_str(p, "reason", ("ambiguous_claims", "closed_ticket_window_activity", "claim_receipt_stalled"))
        history = t.imported.get("page_window")
        historical = (t.phase == CLOSED and isinstance(history, dict) and history.get("entry_id") == entry)
        operational = any(w.kind == "page-window" and w.ref == entry for w in t.waits)
        if not (historical if reason == "closed_ticket_window_activity" else operational):
            tx.stale(f"no matching page entry {entry} for {reason}")
            return
        key = page_review_key(t, entry, reason, tx.op.at)
        if key is None:
            tx.noop("page window finding already referred to advisor")
            return
        if reason in ("ambiguous_claims", "claim_receipt_stalled"):
            wait = next(w for w in t.waits if w.kind == "page-window" and w.ref == entry)
            wait.detail["identity_review_required"] = True
            wait.detail.setdefault("identity_review_reason", reason)
            ask_after = _opt_str(p, "ask_after")
            if ask_after is not None:
                wait.detail.setdefault("identity_review_after", ask_after)
                if parse_at(tx.op.at) < parse_at(wait.detail["identity_review_after"]):
                    tx.emit("pagewindow.needs_review", t.ticket, entry_id=entry, reason=reason)
                    return  # block immediately, but ask only after the receipt observation threshold
        # Findings on closed imports remain advisory. Operational ambiguity persists independently
        # of the question wait, including after an unstructured "decided" answer.
        _ask(tx, t, tx.new_id(f"q:{t.ticket}:{key}"), "advisor", key,
             _req_str(p, "title"), None, Continuation())
        tx.emit("pagewindow.needs_review", t.ticket, entry_id=entry, reason=reason)
        return
    wait = next((w for w in t.waits if w.kind == "page-window" and w.ref == entry), None)
    run = next((x for x in t.pending if x.kind == "run_window" and x.bound.get("entry_id") == entry), None)
    if wait is None or run is None:
        tx.stale(f"page entry {entry} is not queued on {t.ticket}")
        return
    outcome = _opt_str(p, "outcome")
    if event == "claim_failed":  # library said WAIT_RESOURCE: same action, same created_at, back off
        if not run.open:
            tx.stale("run_window no longer open")
            return
        run.payload["next_check_at"] = _opt_str(p, "next_check_at")
        run.payload["last_busy"] = _opt_obj(p, "busy") or {}
        run.updated_at = tx.op.at
        tx.emit("pagewindow.claim_failed", t.ticket, entry_id=entry, action_id=run.action_id)
        busy_gate = _opt_obj(p, "gate")
        if busy_gate is not None and _req_str(busy_gate, "kind") in GATE_WAIT_KINDS and t.phase != CLOSED:
            gkind, gref = _req_str(busy_gate, "kind"), _req_str(busy_gate, "ref")
            if not any(w.open and w.kind == gkind and w.ref == gref for w in t.waits):
                tx.add_wait(t, gkind, gref, detail=busy_gate)
        return
    if event == "acquired":
        request = wait.detail.get("identity_review_request")
        review_id = _opt_str(p, "review_question_id")
        if review_id is not None:
            if (not isinstance(request, dict) or request.get("question_id") != review_id
                    or request.get("claim_id") != p.get("claim_id") or not _opt_str(p, "run_dir")):
                raise Rejected("identity binding does not match the explicit review disposition")
        if not wait.open:
            satisfied = wait.satisfied_by or {}
            original = _opt_obj(satisfied, "value") or satisfied  # seed retains the legacy flat receipt
            if (wait.detail.get("outcome") is not None or wait.waived is not None
                    or p.get("holder") != original.get("holder") or p.get("lease") != original.get("lease")):
                tx.stale("identity completion does not match the acquired lease")
                return
            updates = {k: p[k] for k in ("claim_id", "run_dir") if _opt_str(p, k) is not None}
            if any(wait.detail.get(k) is not None and wait.detail[k] != v for k, v in updates.items()):
                raise Rejected("acquired claim identity is immutable")
            if not any(wait.detail.get(k) != v for k, v in updates.items()) and review_id is None:
                tx.noop("page entry already acquired")
                return
            wait.detail.update(updates)
            if review_id is not None:
                wait.detail["identity_review_required"] = False
                wait.detail["identity_review_claim"] = p["claim_id"]
                wait.detail.pop("identity_review_request", None)
            tx.emit("pagewindow.identity_bound", t.ticket, entry_id=entry, **updates)
            return
        tx.satisfy(t, wait, source or f"op:{tx.op.op_id}", {"holder": str(p.get("holder") or ""),
                                                             "lease": str(p.get("lease") or "")})
        for key in ("claim_id", "run_dir"):
            if _opt_str(p, key) is not None:
                wait.detail[key] = p[key]
        if review_id is not None:
            wait.detail["identity_review_required"] = False
            wait.detail["identity_review_claim"] = p["claim_id"]
            wait.detail.pop("identity_review_request", None)
        wait.continued = True  # acquiring starts the window; the release decides what follows
        if run.open:
            tx.close_pending(t, run, DONE_P, "page acquired")
        return
    # Resolver terminal facts carry the receipt identity too, including a result observed before
    # acquire. Never let a terminal fact replace a previously bound claim/run.
    for key in ("claim_id", "run_dir"):
        value = _opt_str(p, key)
        if value is not None:
            if wait.detail.get(key) is not None and wait.detail[key] != value:
                raise Rejected("terminal result differs from the entry's claim identity")
            wait.detail[key] = value
    result_info: JsonObj = {"outcome": outcome, "result_status": _opt_str(p, "result_status"),
                            "run_dir": _opt_str(p, "run_dir")}
    if event in ("lost", "check_failed", "launch_failed"):
        # Also after an acquire (B2 review H1): the entry ends here, so the window is no longer running.
        if wait.detail.get("outcome") is not None:
            tx.noop(f"page entry {entry} already ended {wait.detail.get('outcome')}")
            return
        wait.detail["outcome"] = outcome or event.upper()
        wait.detail["release_source"] = source or f"op:{tx.op.op_id}"
        if result_info["run_dir"] is not None:
            wait.detail["run_dir"] = result_info["run_dir"]
        if run.open:
            tx.close_pending(t, run, ESCALATED, event)
        if wait.open:
            wait.waived = {"by": "pagewindow", "reason": event, "at": tx.op.at}
            wait.continued = True
        if t.phase != CLOSED:
            tx.add_pending(t, f"window_{event}", DECIDE, bound={"entry_id": entry},
                           payload={**result_info, "outcome": wait.detail["outcome"]})
        return
    # released
    if not outcome:
        raise ValidationError("released needs an outcome")
    previous = wait.detail.get("outcome")
    if previous is not None:
        if previous == outcome:
            tx.noop("release already recorded")
            return
        if not (previous == "RECOVERY_REQUIRED" and _opt_str(p, "recovered_from") == "RECOVERY_REQUIRED"):
            raise Rejected(f"page entry {entry} already released with {previous}")
        # `page-window.py recover` re-closed a RECOVERY_REQUIRED window (B2 review L2): the recovery
        # decision is answered by the re-close, and the new outcome is handled like a first release.
        # Only while that decision is still open (B2 re-review M3): once the coordinator acted on it
        # (acknowledge, re-enqueue, close), a late re-close must not fire a continuation.
        recovery = [x for x in t.pending if x.kind == "recovery" and x.bound.get("entry_id") == entry
                    and x.status in OUTSTANDING]
        if not recovery:
            tx.stale(f"recover re-close of {entry} after its recovery decision was already answered")
            return
        history = wait.detail.get("outcomes")
        wait.detail["outcomes"] = [*(history if isinstance(history, list) else []), previous]
        for pend in recovery:
            pend.detail = f"{ACKNOWLEDGED}: re-closed by page-window recover ({outcome})"
            tx.close_pending(t, pend, DONE_P, "re-closed by page-window recover")
    wait.detail["outcome"] = outcome
    wait.detail["release_source"] = source or f"op:{tx.op.op_id}"
    if result_info["run_dir"] is not None:
        wait.detail["run_dir"] = result_info["run_dir"]
    if wait.open:  # released without a recorded acquire: record both
        tx.satisfy(t, wait, source or f"op:{tx.op.op_id}", {"holder": str(p.get("holder") or "")})
        wait.continued = True
    if run.open:
        tx.close_pending(t, run, DONE_P, "window ran")
    tx.emit("pagewindow.released", t.ticket, entry_id=entry, outcome=outcome)
    if t.phase == CLOSED:
        return
    if outcome == "CLEAN":
        on_rel = Continuation.load(wait.detail.get("on_released") or {"kind": "none"})
        if on_rel.kind == "none":
            tx.add_pending(t, "window_result", DECIDE, bound={"entry_id": entry}, payload=result_info)
        else:
            _emit_continuation(tx, t, on_rel, wait)
    elif outcome == "HOLD":
        # B2 review M3: a HOLD after STEP_WAIT / PARTIAL / RUN_ERROR must show the steps did not finish.
        tx.add_pending(t, "clear_hold", DECIDE, bound={"entry_id": entry}, payload=result_info)
    elif outcome == "DRIFT_REVIEW":
        drift = _ask(tx, t, tx.new_id(f"q:{t.ticket}:drift"), "advisor", f"pagewindow-drift:{entry}",
                     f"page window {entry} drift review", None, None)
        tx.meta.paused["page-window"] = f"question:{drift.question_id}"
    elif outcome == "RECOVERY_REQUIRED":
        pend = tx.add_pending(t, "recovery", DECIDE, bound={"entry_id": entry}, payload=result_info)
        tx.meta.paused["page-window"] = f"action:{pend.action_id}"
    else:
        tx.add_pending(t, "window_result", DECIDE, bound={"entry_id": entry}, payload=result_info)


def _clock_observed(tx: _Tx) -> None:
    now = parse_at(tx.op.at)
    hits = 0
    for t in tx.tickets.values():
        for w in t.waits:
            if w.kind == "time" and w.open and parse_at(w.ref) <= now:
                tx.satisfy(t, w, f"clock:{tx.op.at}", {"observed_at": tx.op.at})
                hits += 1
    grace = _opt_int(tx.op.payload, "executor_unknown_after")
    if grace is not None:
        if grace < 0:
            raise ValidationError("executor_unknown_after: must be >= 0 seconds")
        for name, e in executor_unknown_due(World(tx.tickets, tx.meta), tx.op.at, grace):
            t = tx.tickets[name]
            tx.add_pending(t, "executor_unknown", DECIDE, bound={"execution_id": e.execution_id, "since": e.observed_at},
                           payload={"session": e.session, "attempt": e.attempt, "unknown_since": e.observed_at,
                                    "grace_seconds": grace}, once=True)
            hits += 1
    if not hits:
        tx.noop("no time wait or executor_unknown due")


def executor_unknown_due(world: World, at: str, grace_seconds: int) -> list[tuple[str, Execution]]:
    """Design §3.4 health: the current executor of an open ticket observed ``unknown`` for at least
    ``grace_seconds`` at ``at``, with no executor_unknown decision for that unknown episode yet (the
    binding carries the episode start, so unknown -> running -> unknown asks again). Pure; the B3
    controller submits clock.observed(executor_unknown_after) only when this is non-empty."""
    now = parse_at(at)
    out: list[tuple[str, Execution]] = []
    for name in sorted(world.tickets):
        t = world.tickets[name]
        e = t.current_executor()
        if t.phase == CLOSED or e is None or e.observed != "unknown":
            continue
        if parse_at(e.observed_at) + timedelta(seconds=grace_seconds) > now:
            continue
        bound = {"execution_id": e.execution_id, "since": e.observed_at}
        if any(p.kind == "executor_unknown" and p.bound == bound for p in t.pending):
            continue
        out.append((name, e))
    return out


# ---------------------------------------------------------------- ready-idle (shadow health, HD-01 A)

READY_IDLE_SECONDS = 20 * 60  # a ready ticket nobody started for this long is flagged (same as the nudge cadence)
REVIEWER_UNKNOWN_GRACE_SECONDS = 10 * 60  # same grace as executor_unknown; after it a lost reviewer stops hiding


@dataclass(frozen=True)
class ReadyIdle:
    """An open ticket that could move and nothing moves it: every dependency (and the defer condition
    of a deferred ticket) is accepted, nothing runs, waits, or sits in front of the coordinator, and
    that has been true for at least ``READY_IDLE_SECONDS``. ``need`` is what would start it:
    ``authorize`` (deferred), ``dispatch`` (WORK) or ``triage`` (REVIEW). ``since`` starts the
    readiness episode (the ticket's last change or the latest acceptance it waited for).
    ``board_only`` lists prerequisites accepted on the legacy board (run.json) but still open in the
    core: until stage C the board is the live truth, so those count, and the record says so."""
    ticket: str
    need: str
    since: str
    reasons: tuple[str, ...]
    board_only: tuple[str, ...] = ()

    @property
    def basis(self) -> str:
        return "board" if self.board_only else "core"


def ready_idle(world: World, at: str, idle_seconds: int = READY_IDLE_SECONDS,
               board_accepted: Mapping[str, str | None] | None = None) -> list[ReadyIdle]:
    """Pure query (design §3.4 health, HD-01 A). ``board_accepted``: ticket -> legacy accepted_at (or
    None when unknown) of tickets accepted on run.json. A pending decision (dispatch, next_step, ...)
    excludes the ticket: decisions already have their own delivery/nudge cadence. An executor or
    current reviewer in ``unknown`` excludes it too (the executor_unknown grace owns that)."""
    now = parse_at(at)
    board = board_accepted or {}
    out: list[ReadyIdle] = []

    def acceptance(name: str) -> tuple[bool, str | None, bool]:
        """(accepted, when, on the board only)."""
        target = world.tickets.get(name)
        if accepted(target):
            assert target is not None and target.closure is not None
            return True, target.closure.at, False
        if name in board and (target is None or target.closure is None):
            return True, board[name], True
        return False, None, False

    for name in sorted(world.tickets):
        t = world.tickets[name]
        if t.phase == CLOSED or t.open_waits() or t.active() or t.window_running():
            continue
        executor = t.current_executor()
        if executor is not None and executor.observed == "unknown":
            continue  # the executor_unknown grace and decision own it
        # Reviewers count only for a current candidate (without one, current_reviewer() would match
        # historical reviews with candidate_digest=None), and an unknown one only within its grace:
        # nothing else ever resolves a lost reviewer, so it must not hide the ticket for good.
        reviewers = [t.current_reviewer(axis) for axis in AXES] if t.candidate is not None else []
        if any(e is not None and e.observed == "unknown"
               and parse_at(e.observed_at) + timedelta(seconds=REVIEWER_UNKNOWN_GRACE_SECONDS) > now
               for e in reviewers):
            continue
        if t.outstanding(DECIDE) or any(p.mode == OUTBOX and p.open for p in t.pending):
            continue
        prereqs = list(t.deps)
        defer_note = ""
        if t.scheduling == DEFERRED:
            kind, _, rest = (t.defer or "").partition(":")
            if kind == "ticket" and rest:
                prereqs.append(rest)
                defer_note = f"defer {t.defer}"
            elif kind == "batch":
                batch, _, cond = rest.rpartition(":")
                if cond == "completed":
                    members = sorted(n for n, x in world.tickets.items() if x.batch == batch)
                    if not members:
                        continue
                    prereqs.extend(members)  # each member's acceptance (core or board) dates readiness
                elif not _batch_condition(world.tickets, batch, cond):
                    continue
                defer_note = f"defer {t.defer} holds"
            elif t.defer:
                continue  # an unknown predicate is never guessed satisfied
            else:
                defer_note = "deferred without a defer condition (legacy CREATED)"
        stamps = [t.updated_at]
        board_only: list[str] = []
        ok = True
        for dep in dict.fromkeys(prereqs):
            met, when, on_board = acceptance(dep)
            if not met:
                ok = False
                break
            if when:
                stamps.append(when)
            if on_board:
                board_only.append(dep)
        if not ok:
            continue
        since = max(stamps, key=lambda s: parse_at(s))
        if parse_at(since) + timedelta(seconds=idle_seconds) > now:
            continue
        need = "authorize" if t.scheduling == DEFERRED else ("triage" if t.phase == REVIEW else "dispatch")
        reasons = [f"ready since {since}: no execution, open wait, question or decision"]
        if prereqs:
            reasons.append("accepted: " + ", ".join(
                f"{d} (board only; core still open)" if d in board_only else d for d in dict.fromkeys(prereqs)))
        if defer_note:
            reasons.append(defer_note)
        out.append(ReadyIdle(name, need, since, tuple(reasons), tuple(board_only)))
    return out


# ---------------------------------------------------------------- structured external blocks (HD-01 B)

EXTERNAL_FIELDS = ("owner", "attempts", "recheck_at")


def external_detail(value: Json) -> JsonObj:
    """Validate the structure of an external wait (``wait.add``/``wait.update`` ``detail``): ``owner``
    (who or what outside us), ``attempts`` (what we already tried), ``recheck_at`` (ISO time to
    re-judge). Every field is optional here so legacy free-text blocks still enter the core; a missing
    one is flagged by the shadow controller (``external_gaps``), not refused."""
    obj = _obj(value, "detail")
    out: JsonObj = {}
    owner = obj.get("owner")
    if owner is not None:
        if not isinstance(owner, str) or not owner.strip():
            raise ValidationError("detail.owner: must be a non-empty string")
        out["owner"] = owner.strip()
    attempts = obj.get("attempts")
    if attempts is not None:
        if not isinstance(attempts, list) or not all(isinstance(a, str) and a.strip() for a in attempts):
            raise ValidationError("detail.attempts: must be a list of non-empty strings")
        out["attempts"] = [a.strip() for a in attempts if isinstance(a, str)]
    recheck = obj.get("recheck_at")
    if recheck is not None:
        if not isinstance(recheck, str):
            raise ValidationError("detail.recheck_at: must be an ISO-8601 time")
        parse_at(recheck)
        out["recheck_at"] = recheck
    for key, extra in obj.items():  # provenance markers (legacy_import, ...) ride along unvalidated
        if key not in EXTERNAL_FIELDS:
            out[key] = extra
    return out


def external_gaps(w: Wait) -> list[str]:
    """The structured fields an external wait is missing (empty list: fully structured)."""
    d = w.detail
    gaps = []
    if not (isinstance(d.get("owner"), str) and d["owner"]):
        gaps.append("owner")
    if not (isinstance(d.get("attempts"), list) and d["attempts"]):
        gaps.append("attempts")
    if not isinstance(d.get("recheck_at"), str):
        gaps.append("recheck_at")
    return gaps


def _pending_progress(tx: _Tx) -> None:
    t, p = tx.ticket(), tx.op.payload
    pend = t.pending_by_id(_req_str(p, "action_id"))
    status = _req_str(p, "status", ("delivered", "claimed", "done", "nudged", "escalated", "retry"))
    if pend is None:
        tx.stale("unknown action")
        return
    if not pend.open:
        tx.stale(f"action already {pend.status}")
        return
    detail = str(p.get("detail") or "")
    if status == "nudged":
        pend.nudges.append(tx.op.at)
        pend.updated_at = tx.op.at
        tx.emit("pending.nudged", t.ticket, action_id=pend.action_id, count=len(pend.nudges))
        if len(pend.nudges) >= NUDGE_LIMIT:
            tx.close_pending(t, pend, ESCALATED, f"{NUDGE_LIMIT} nudges without a response")
        return
    if status == "retry":  # mechanical wait (quota WAIT, page busy): same action, keep created_at
        pend.payload["next_check_at"] = _opt_str(p, "next_check_at")
        pend.updated_at, pend.detail = tx.op.at, detail
        return
    if status == "done":
        if pend.mode != OUTBOX:
            raise Rejected("a decision closes only by the command that answers it")
        tx.close_pending(t, pend, DONE_P, detail or "receipt")
        return
    if status == "escalated":
        tx.close_pending(t, pend, ESCALATED, detail or "escalated")
        return
    order = {QUEUED: 0, DELIVERED: 1, CLAIMED_P: 2}
    if order[status] <= order[pend.status]:
        tx.noop(f"action already {pend.status}")
        return
    pend.status, pend.updated_at, pend.detail = status, tx.op.at, detail
    tx.emit(f"pending.{status}", t.ticket, action_id=pend.action_id)


def _migration_seed(tx: _Tx) -> None:
    """One atomic, replayable legacy snapshot, never a route for later state amendments.

    Imported closures retain the old board's authority; reviews never gain a PASS from migration.
    Adopted executions and windows have no launch/register outbox. B5 observes fresh turns later.
    """
    if tx.tickets or tx.meta.seed or tx.meta.mode != "shadow":
        raise Rejected("migration.seed needs an empty shadow core; already seeded cores are never overwritten")
    records = tx.op.payload.get("tickets")
    if not isinstance(records, list):
        raise ValidationError("migration.seed needs tickets[]")
    meta = _obj(tx.op.payload.get("seed"), "seed")
    _req_str(meta, "source_digest")
    _obj(meta.get("batches"), "seed.batches")
    for raw in records:
        t = Ticket.load(_obj(raw, "seed ticket"))
        if t.ticket in tx.tickets:
            raise ValidationError(f"duplicate seed ticket {t.ticket}")
        if not t.imported.get("limit") or any(
                (p.kind != "run_window" or not p.payload.get("migration_adopt"))
                and not (p.kind == "answer_user" and p.mode == DECIDE) for p in t.pending):
            raise ValidationError("seed records need imported.limit and only adopted window actions")
        if any(e.mode != "adopt" for e in t.executions) or any(r.verdict for r in t.reviews):
            raise ValidationError("migration only adopts executions; imported reviews are not verdicts")
        tx.tickets[t.ticket] = t
    tx.meta.seed = copy.deepcopy(meta)
    tx.emit("migration.seeded", None, tickets=len(tx.tickets), source_digest=meta["source_digest"])


HANDLERS: dict[str, Handler] = {
    "migration.seed": _migration_seed,
    "ticket.classify": _ticket_classify, "ticket.create": _ticket_create, "ticket.amend": _ticket_amend, "ticket.authorize": _ticket_authorize,
    "execution.request": _execution_request, "question.ask": _question_ask,
    "decision.respond": _decision_respond, "wait.add": _wait_add, "wait.update": _wait_update,
    "wait.waive": _wait_waive, "pagewindow.enqueue": _pagewindow_enqueue, "ticket.close": _ticket_close,
    "ticket.integrate": _ticket_integrate, "authority.switch": _authority_switch,
    "resource.unpause": _resource_unpause, "pagewindow.policy": _pagewindow_policy, "execution.observed": _execution_observed, "review.observed": _review_observed,
    "question.answered": _question_answered, "wait.satisfied": _wait_satisfied,
    "resource.observed": _resource_observed, "clock.observed": _clock_observed,
    "pending.progress": _pending_progress,
}


# ---- settle: the conversions the program owns (§0), run after every op inside the same transaction


def _batch_condition(tickets: Mapping[str, Ticket], batch: str, cond: str) -> bool:
    members = [t for t in tickets.values() if t.batch == batch]
    if cond == "bound":
        return any(t.scheduling == AUTHORIZED for t in members)
    return bool(members) and all(accepted(t) for t in members)


def _emit_continuation(tx: _Tx, t: Ticket, c: Continuation, w: Wait) -> None:
    if c.kind == "none":
        return
    if c.kind == "resume":
        # Re-checked when it fires (review M1): the record may have moved on since the wait was made.
        eid = c.bound["execution_id"]
        current = t.current_executor()
        why = None
        if t.phase != WORK:
            why = f"ticket is in {t.phase}, not WORK"
        elif current is None or current.execution_id != eid:
            why = f"{eid} is no longer the current executor execution"
        elif current.observed in ACTIVE:
            why = f"{eid} is already {current.observed}"
        if why is None:
            tx.add_pending(t, "resume", OUTBOX, bound={"execution_id": eid},
                           payload={"because": f"{w.kind}:{w.ref}", "satisfied_by": w.satisfied_by})
        else:
            tx.add_pending(t, "next_step", DECIDE, bound={"wait_id": w.wait_id},
                           payload={"reason": f"approved resume of {eid} not sent: {why}"}, detail=why)
    elif c.kind == "request_reviews":
        _queue_reviews(tx, t)
    elif c.kind == "run_window":
        tx.add_pending(t, "run_window", OUTBOX, bound={"entry_id": c.bound["entry_id"]}, payload=dict(c.bound))
    elif c.kind == "apply_decision":
        qid = c.bound.get("question_id", w.ref)
        q = next((x for x in t.questions if x.question_id == qid), None)
        answer = q.answer if q is not None and q.answer is not None else {}
        tx.add_pending(t, "apply_decision", DECIDE, bound={"question_id": qid},
                       payload={"verdict": answer.get("verdict"), "text": answer.get("text"),
                                "source_record": answer.get("source_record")})
    else:  # decide
        tx.add_pending(t, c.bound["kind"], DECIDE, bound={"wait_id": w.wait_id})


def _settle(tx: _Tx) -> None:
    for t in tx.tickets.values():
        if t.phase == CLOSED:
            continue
        for w in t.waits:  # internal waits resolve from the authoritative records, no resolver
            if not w.open:
                continue
            if w.kind == "ticket":
                target = tx.tickets.get(w.ref)
                if accepted(target):
                    tx.satisfy(t, w, f"ticket:{w.ref}:accepted", {"closure": "accepted"})
                elif target is not None and target.closure is not None:
                    tx.add_pending(t, "unresolvable_wait", DECIDE, bound={"wait_id": w.wait_id},
                                   payload={"target": w.ref, "closure": target.closure.kind}, once=True)
            elif w.kind == "batch":
                name, _, cond = w.ref.rpartition(":")
                if _batch_condition(tx.tickets, name, cond):
                    tx.satisfy(t, w, f"batch:{w.ref}", {"condition": cond})
        for dep in t.deps:
            target = tx.tickets.get(dep)
            if target is not None and target.closure is not None and target.closure.kind != "accepted":
                tx.add_pending(t, "unresolvable_wait", DECIDE, bound={"dep": dep},
                               payload={"target": dep, "closure": target.closure.kind}, once=True)
        if not t.open_waits() and not t.active():
            # Order-independent: whichever wait closed last, every closed wait's continuation fires once.
            for w in t.waits:
                if not w.open and not w.continued:
                    w.continued = True
                    _emit_continuation(tx, t, w.on_satisfied, w)
        if (t.scheduling == AUTHORIZED and t.phase == WORK and not t.executions and not t.open_waits()
                and all(accepted(tx.tickets.get(d)) for d in t.deps)
                and not any(p.kind == "dispatch" for p in t.pending)):
            tx.add_pending(t, "dispatch", DECIDE, bound={}, once=True)
        # Failure #7 / review M3, R3: an open, authorized ticket that waits on nothing, runs nothing and
        # has nothing queued for the program gets a fallback decision; it is withdrawn as soon as the
        # ticket is no longer stuck (something to wait on, run, or decide appeared).
        fallback = [p for p in t.pending if p.kind == "next_step" and not p.bound and p.status in OUTSTANDING]
        if _stuck(tx, t):
            if not fallback:
                tx.add_pending(t, "next_step", DECIDE, bound={},
                               payload={"reason": "nothing is waiting, running or queued for this ticket"})
        else:
            for p in fallback:
                tx.close_pending(t, p, SUPERSEDED_P, "ticket no longer stuck")
    _settle_paused(tx)


PROGRESS_OUTBOX = frozenset({"resume", "request_reviews", "run_window"})


def run_window_eligibility(world: World, ticket: str, entry_id: str, at: str) -> list[str]:
    """Why a queued page-window entry may not be launched at ``at`` (empty list = eligible).
    Pure, for the B3 controller (review M6): deps accepted, no open question, no open wait other than
    this entry's own page-window wait, no running executor, page resource not paused, not backing off.
    The page library still makes the final atomic claim (§5)."""
    t = world.tickets.get(ticket)
    if t is None:
        return [f"unknown ticket {ticket}"]
    reasons: list[str] = []
    run = next((p for p in t.pending if p.kind == "run_window" and p.open and p.bound.get("entry_id") == entry_id),
               None)
    if run is None:
        reasons.append(f"no queued run_window for {entry_id}")
    elif run.payload.get("migration_adopt"):
        reasons.append("imported window: reconcile existing claim/result; never launch again")
    if t.phase == CLOSED:
        reasons.append("ticket closed")
    if t.scheduling != AUTHORIZED:
        reasons.append("ticket deferred")
    if "page-window" in world.meta.paused:
        reasons.append(f"page-window paused by {world.meta.paused['page-window']}")
    unmet = [d for d in t.deps if not accepted(world.tickets.get(d))]
    if unmet:
        reasons.append("prerequisites not accepted: " + ", ".join(unmet))
    for w in t.open_waits():
        if w.kind == "page-window" and w.ref == entry_id:
            continue
        reasons.append(("open question " if w.kind == "question" else "waits on ") + f"{w.kind}:{w.ref}")
    if t.active("executor"):
        reasons.append("executor running")
    if any(w.kind == "page-window" and w.ref == entry_id and w.detail.get("identity_review_required")
           for w in t.waits):
        reasons.append("page-window claim identity awaiting explicit disposition")
    if run is not None:
        until = run.payload.get("next_check_at")
        if isinstance(until, str) and until and parse_at(until) > parse_at(at):
            reasons.append(f"backing off until {until}")
    return reasons


def page_queue(world: World, at: str) -> list[tuple[str, Pending, list[str]]]:
    """Every queued run_window row in launch order (priority desc, then age), with its blockers."""
    rows = [(name, p) for name, t in world.tickets.items() for p in t.pending if p.kind == "run_window" and p.open]

    def priority(p: Pending) -> int:
        value = p.payload.get("priority")
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    rows.sort(key=lambda r: (-priority(r[1]), r[1].created_at, r[1].action_id))
    return [(name, p, run_window_eligibility(world, name, p.bound.get("entry_id", ""), at)) for name, p in rows]


def _stuck(tx: _Tx, t: Ticket) -> bool:
    """Nothing waits, runs, is queued for the program or is in front of the coordinator, apart from
    the fallback next_step itself. An executor in ``unknown`` is not stuck yet: the 10-minute
    executor_unknown grace is the B3 controller's (review R3)."""
    return (t.phase != CLOSED and t.scheduling == AUTHORIZED and not t.open_waits() and not t.active()
            and not any(e.observed == "unknown" for e in t.executions)
            and not t.window_running() and all(accepted(tx.tickets.get(d)) for d in t.deps)
            and not any(p for p in t.outstanding(DECIDE) if not (p.kind == "next_step" and not p.bound))
            and not any(p.open and p.kind in PROGRESS_OUTBOX for p in t.pending))


ACKNOWLEDGED = "acknowledged"


def _settle_paused(tx: _Tx) -> None:
    """Review M4/R5: a paused resource resumes only on a final resolution — the recovery decision
    acknowledged (not closed, failed or re-enqueued), or the drift question finally answered (an
    advisor verdict other than needs_user, or the user's answer). resource.unpause is the manual path."""
    for resource, holder in sorted(tx.meta.paused.items()):
        kind, _, ref = holder.partition(":")
        resolved = False
        for t in tx.tickets.values():
            if kind == "action":
                pend = t.pending_by_id(ref)
                resolved = resolved or (pend is not None and pend.status == DONE_P
                                        and pend.detail.startswith(ACKNOWLEDGED))
            elif kind == "question":
                q = t.question_by_id(ref)
                resolved = resolved or (q is not None and q.answer is not None
                                        and (q.to == "user" or q.answer.get("verdict") != "needs_user"))
        if resolved:
            del tx.meta.paused[resource]
            tx.emit("resource.unpaused", None, resource=resource, held_by=holder, reason="resolved")


# ---- structural invariants (§3.4), checked before every commit


def _graph(tickets: Mapping[str, Ticket]) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = {name: set(t.deps) for name, t in tickets.items()}
    for name, t in tickets.items():
        for w in t.waits:
            if not w.open:
                continue
            if w.kind == "ticket":
                edges[name].add(w.ref)
            elif w.kind == "batch" and w.ref.endswith(":completed"):
                batch = w.ref.rpartition(":")[0]
                edges[name].update(x.ticket for x in tickets.values() if x.batch == batch)
    return edges


def find_cycle(tickets: Mapping[str, Ticket]) -> list[str] | None:
    """A cycle in deps ∪ open ticket/batch waits (the joint graph), or None."""
    edges = _graph(tickets)
    color: dict[str, int] = {}
    stack: list[str] = []

    def visit(node: str) -> list[str] | None:
        color[node] = 1
        stack.append(node)
        for nxt in sorted(edges.get(node, ())):
            if color.get(nxt) == 1:
                return stack[stack.index(nxt):] + [nxt]
            if color.get(nxt, 0) == 0 and nxt in edges:
                found = visit(nxt)
                if found:
                    return found
        stack.pop()
        color[node] = 2
        return None

    for node in sorted(edges):
        if color.get(node, 0) == 0:
            found = visit(node)
            if found:
                return found
    return None


def check_invariants(before: Mapping[str, Ticket], after: Mapping[str, Ticket]) -> None:
    seen: dict[str, str] = {}

    def unique(kind: str, value: str, owner: str) -> None:
        key = f"{kind}:{value}"
        if key in seen:
            raise InvariantError(f"duplicate {kind} {value} ({seen[key]} and {owner})")
        seen[key] = owner

    for name, t in after.items():
        if name != t.ticket:
            raise InvariantError(f"record key {name} != ticket {t.ticket}")
        for e in t.executions:
            unique("execution_id", e.execution_id, name)
            if e.role == "executor" and e.attempt > t.attempt:
                raise InvariantError(f"{name}: execution attempt {e.attempt} above ticket attempt {t.attempt}")
        for w in t.waits:
            unique("wait_id", w.wait_id, name)
            if w.kind == "ticket" and w.ref not in after:
                raise InvariantError(f"{name}: wait on unknown ticket {w.ref}")
        for q in t.questions:
            unique("question_id", q.question_id, name)
        for p in t.pending:
            unique("action_id", p.action_id, name)
            if t.phase == CLOSED and p.open and p.kind not in ("cleanup", "push"):
                raise InvariantError(f"{name}: closed ticket has open {p.kind}")
        for r in t.reviews:
            unique("review_request_id", r.review_request_id, name)
            rx = t.execution(r.execution_id)
            if rx is None or rx.role != "reviewer" or rx.axis != r.axis or rx.candidate_digest != r.candidate_digest:
                raise InvariantError(f"{name}: review {r.review_request_id} not bound to its reviewer execution")
        for dep in t.deps:
            if dep == name or dep not in after:
                raise InvariantError(f"{name}: bad dependency {dep}")
        if (t.phase == CLOSED) != (t.closure is not None):
            raise InvariantError(f"{name}: phase {t.phase} with closure {t.closure}")
        if t.phase == REVIEW and t.candidate is None:
            raise InvariantError(f"{name}: REVIEW without a candidate")
        if t.scheduling == DEFERRED and t.executions:
            raise InvariantError(f"{name}: deferred ticket has executions")
        old = before.get(name)
        if old is not None:
            if t.attempt < old.attempt:
                raise InvariantError(f"{name}: attempt went back {old.attempt} -> {t.attempt}")
            if old.phase == CLOSED and len(t.executions) > len(old.executions):
                raise InvariantError(f"{name}: closed ticket got a new execution")
            if old.phase == CLOSED and t.phase != CLOSED:
                raise InvariantError(f"{name}: closed ticket reopened")
    cycle = find_cycle(after)
    if cycle:
        raise InvariantError("dependency/wait cycle: " + " -> ".join(cycle))


def reduce(world: World, op: Op) -> Transition:
    """Live write boundary. Caller proof is never trusted; integration runs Git itself."""
    return _reduce(world, op)


def _reduce(world: World, op: Op, *, integration_fact: JsonObj | None = None,
            kind_fact: JsonObj | None = None) -> Transition:
    """Internal reducer; only offline from_audit supplies archived tool facts."""
    op.validate()
    if op.epoch is not None and op.epoch != world.meta.authority_epoch:
        raise Rejected(f"op epoch {op.epoch} is not the current authority epoch {world.meta.authority_epoch}")
    if world.meta.mode in ("freeze", "legacy") and op.kind != "authority.switch":
        if world.meta.mode == "legacy" or op.kind in COMMANDS:
            raise Rejected(f"authority mode {world.meta.mode}: {op.kind} not accepted")
    if op.expected_revision is not None:
        current = world.tickets.get(op.ticket or "")
        have = current.revision if current is not None else 0
        if have != op.expected_revision:
            raise Rejected(f"{op.ticket} is at revision {have}, expected {op.expected_revision} (stale decision)")
    tx = _Tx(world, op)
    tx.kind_fact = kind_fact
    tx.integration_fact = integration_fact
    if op.kind in FACTS and op.ticket and op.ticket not in world.tickets:
        tx.stale(f"fact about unknown ticket {op.ticket}")
    else:
        HANDLERS[op.kind](tx)
    if tx.status == "accepted":
        _settle(tx)
    check_invariants(tx.before, tx.tickets)
    changed: dict[str, Ticket] = {}
    for name, t in tx.tickets.items():
        old = tx.before.get(name)
        if old is None or old.to_json() != t.to_json():
            label_before = label(old, tx.before) if old is not None else None
            t.revision = (old.revision if old is not None else 0) + 1
            t.updated_at = op.at
            changed[name] = t
            label_after = label(t, tx.tickets)
            if label_after != label_before:
                tx.emit("label.changed", name, before=label_before, after=label_after)
    if tx.status != "accepted" and changed:
        # A noop/stale result never changes records (§3.3 facts: late facts do not move state).
        raise InvariantError(f"{op.kind} returned {tx.status} but changed {sorted(changed)}")
    main = Event(op.kind, op.ticket, {"op": tx.op.to_json(), "status": tx.status, "detail": tx.detail}, None)
    revision = changed[op.ticket].revision if op.ticket in changed else (
        world.tickets[op.ticket].revision if op.ticket in world.tickets else None)
    receipt = Receipt(op.op_id, tx.status, op.ticket, revision, None, tx.detail, tx.actions)
    imported = tx.imported if tx.status == "accepted" else []  # a stale/noop fact imports nothing (L1)
    events = [] if tx.quiet else [main] + tx.events
    return Transition(receipt, changed, events, imported, tx.meta, tx.quiet)


# ---------------------------------------------------------------- in-memory store (tests, replay)


class MemoryStore:
    """Same semantics as SqliteStore without a file: ops receipts, events, idempotency. Replaying an
    audit export into a fresh MemoryStore gives the same records (events carry their values)."""

    def __init__(self, run_id: str = "memory") -> None:
        self.tickets: dict[str, Ticket] = {}
        self.meta = Meta(run_id)
        self.ops: dict[str, tuple[str, Receipt]] = {}
        self.events: list[tuple[int, Event]] = []
        self.imported: set[str] = set()

    def submit(self, op: Op) -> Receipt:
        prior = self.ops.get(op.op_id)
        if prior is not None:
            if prior[0] != op.digest():
                raise OpConflict(f"op {op.op_id} was accepted with a different request")
            return Receipt.load(prior[1].to_json(), replayed=True)
        key = import_key(op)
        known = frozenset({key}) if key is not None and key in self.imported else frozenset()
        tr = reduce(World(self.tickets, self.meta, known), op)
        return self._record(op, tr)

    def _record(self, op: Op, tr: Transition) -> Receipt:
        for name, t in tr.changed.items():
            self.tickets[name] = t
        seq = len(self.events)
        for event in tr.events:
            seq += 1
            self.events.append((seq, event))
        tr.receipt.seq = seq if tr.events else None
        self.imported.update(tr.imported)
        self.meta = tr.meta
        self.ops[op.op_id] = (op.digest(), tr.receipt)
        return tr.receipt

    def submit_batch(self, plan: Plan) -> list[Receipt]:
        """Same contract as ``SqliteStore.submit_batch``: all ops or none."""
        ops = list(plan(World(copy.deepcopy(self.tickets), copy.deepcopy(self.meta))) if callable(plan) else plan)
        replayed = _batch_replay(ops, {o.op_id: (self.ops[o.op_id][0], self.ops[o.op_id][1].to_json())
                                       for o in ops if o.op_id in self.ops})
        if replayed is not None:
            return replayed
        saved = (copy.deepcopy(self.tickets), copy.deepcopy(self.meta), dict(self.ops), list(self.events),
                 set(self.imported))
        try:
            return [self.submit(op) for op in ops]
        except BaseException:
            self.tickets, self.meta, self.ops, self.events, self.imported = saved
            raise

    def ops_log(self) -> list[Op]:
        return [Op.load(_obj(e.payload["op"], "op")) for _, e in self.events if "op" in e.payload]

    @classmethod
    def from_audit(cls, ops: Sequence[Op], run_id: str = "memory") -> MemoryStore:
        """Offline reconstruction from a TRUSTED store audit export, never a live write API.

        Audit files/DB rows have the same trust boundary as migration.seed: archive
        provenance/integrity is the operator's responsibility. This method does not
        authenticate arbitrary JSON and cannot write SqliteStore or run.json.
        Live submit/reduce ignore embedded proofs and always run Git again.
        """
        store = cls(run_id)
        for op in ops:
            if op.op_id in store.ops:
                raise ValidationError(f"audit repeats op_id {op.op_id}")
            proof = None
            if op.kind == "ticket.integrate":
                proof = op.integration_proof
                if (not isinstance(proof, dict) or proof.get("status") != "verified" or
                        proof.get("candidate") != op.payload.get("candidate_digest") or
                        not proof.get("heads") or not proof.get("sources")):
                    raise Rejected("audit integration lacks a bound tool proof; cannot invent historical landing")
            kind_proof = None
            if op.kind == "ticket.classify":
                kind_proof = op.kind_proof
                if (not isinstance(kind_proof, dict) or kind_proof.get("status") != "verified" or
                        kind_proof.get("kind") != str(op.payload.get("kind") or "").lower() or
                        kind_proof.get("ticket_source") != (op.payload.get("ticket_source") or {})):
                    raise Rejected("audit classification lacks a bound tool proof")
                if kind_proof["kind"] in ("tool", "doc"):
                    method = kind_proof.get("method")
                    if not ((method == "git_paths" and kind_proof.get("sources")) or
                            (method == "fable_not_applicable" and kind_proof.get("decision_ref"))):
                        raise Rejected("audit classification lacks archived paths or decision")
            key = import_key(op)
            known = frozenset({key}) if key is not None and key in store.imported else frozenset()
            tr = _reduce(World(store.tickets, store.meta, known), op, integration_fact=proof, kind_fact=kind_proof)
            store._record(op, tr)
        return store


# A compound command (stage B4): a fixed list of ops, or a function of the World read inside the
# transaction (a translation that must see the records it is guarded against).
Plan = Union[Sequence[Op], Callable[[World], Sequence[Op]]]


def _batch_replay(ops: Sequence[Op], prior: Mapping[str, tuple[str, JsonObj]]) -> list[Receipt] | None:
    """None: every op is new. All ops already accepted with the same requests: their receipts
    (replayed). Anything in between is refused: a compound is accepted whole or not at all."""
    ids = [o.op_id for o in ops]
    if len(set(ids)) != len(ids):
        raise ValidationError("a batch repeats an op_id")
    for o in ops:
        o.validate()
    if not prior:
        return None
    if len(prior) != len(ops):
        raise OpConflict(f"batch partly accepted before ({sorted(prior)}); a compound is accepted whole")
    for o in ops:
        if prior[o.op_id][0] != o.digest():
            raise OpConflict(f"op {o.op_id} was accepted with a different request")
    return [Receipt.load(prior[o.op_id][1], replayed=True) for o in ops]


# ---------------------------------------------------------------- SQLite store


DDL = (
    "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE tickets (ticket TEXT PRIMARY KEY, revision INTEGER NOT NULL, record_json TEXT NOT NULL)",
    "CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, kind TEXT NOT NULL,"
    " ticket TEXT, actor TEXT NOT NULL, op_id TEXT NOT NULL, payload_json TEXT NOT NULL, cause TEXT)",
    "CREATE TABLE ops (op_id TEXT PRIMARY KEY, request_digest TEXT NOT NULL, result_json TEXT NOT NULL,"
    " at TEXT NOT NULL)",
    "CREATE TABLE pending (action_id TEXT PRIMARY KEY, ticket TEXT NOT NULL, kind TEXT NOT NULL,"
    " mode TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,"
    " record_json TEXT NOT NULL)",
    "CREATE INDEX pending_open ON pending (mode, status)",
    # import_key = (source_record, ticket, fact kind, target, event): one source may release many waits
    "CREATE TABLE facts_imported (import_key TEXT PRIMARY KEY, source_record TEXT NOT NULL, ticket TEXT,"
    " op_id TEXT NOT NULL, at TEXT NOT NULL)",
    "CREATE INDEX facts_imported_source ON facts_imported (source_record)",
)


def check_sqlite_version(version: tuple[int, int, int] | None = None) -> None:
    have = version or sqlite3.sqlite_version_info
    if tuple(have) < MIN_SQLITE:
        raise SqliteVersionError(
            f"SQLite {'.'.join(map(str, have))} is older than {'.'.join(map(str, MIN_SQLITE))} (WAL-reset fix, "
            "design §11.4); run the core with an interpreter linked against a newer SQLite")


def _is_busy(exc: sqlite3.Error) -> bool:
    name = str(getattr(exc, "sqlite_errorname", ""))
    return name.startswith("SQLITE_BUSY") or name.startswith("SQLITE_LOCKED") or "database is locked" in str(exc)


def _configure(conn: sqlite3.Connection, busy_timeout_ms: int) -> None:
    conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
    mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
    if str(mode).lower() != "wal":
        raise StorageError(f"journal_mode is {mode}, need wal")
    conn.execute("PRAGMA synchronous = FULL")
    if conn.execute("PRAGMA synchronous").fetchone()[0] != 2:
        raise StorageError("synchronous is not FULL")
    conn.execute("PRAGMA foreign_keys = ON")


def init_db(path: Path, run_id: str, *, at: str) -> None:
    """The only place a database file is created. Refuses an existing file and a missing run dir
    (an unmounted USB disk must not get a fresh empty database under its mount point)."""
    check_sqlite_version()
    if not run_id:
        raise StorageError("run_id is required")
    path = Path(path)
    if path.exists():
        raise StorageError(f"{path} already exists")
    if not path.parent.parent.is_dir():
        raise StorageError(f"run directory {path.parent.parent} does not exist")
    path.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(f"file:{path}?mode=rwc", uri=True, isolation_level=None)
    try:
        _configure(conn, BUSY_TIMEOUT_MS)
        conn.execute("BEGIN IMMEDIATE")
        for stmt in DDL:
            conn.execute(stmt)
        meta = {"run_id": run_id, "schema_version": str(SCHEMA_VERSION), "authority_epoch": "0",
                "mode": "shadow", "paused": "{}", "created_at": at}
        conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", sorted(meta.items()))
        conn.execute("COMMIT")
    finally:
        conn.close()


def connect(path: Path, run_id: str, *, busy_timeout_ms: int = BUSY_TIMEOUT_MS,
            allow_quiesced: bool = False) -> sqlite3.Connection:
    """Connection factory: SQLite version check, ``mode=rw`` (never creates a file), WAL +
    synchronous=FULL verified, explicit transactions only, run_id and schema_version checked.
    Refuses a quiesced database (``QUIESCE_MARKER`` beside it) unless ``allow_quiesced`` (only the
    controller's quiesce/resume use that)."""
    check_sqlite_version()
    path = Path(path)
    if not allow_quiesced and (path.parent / QUIESCE_MARKER).exists():
        raise QuiescedError(f"{path} is quiesced ({path.parent / QUIESCE_MARKER}); run `resume` first")
    try:
        conn = sqlite3.connect(f"file:{path}?mode=rw", uri=True, isolation_level=None,
                               timeout=busy_timeout_ms / 1000)
    except sqlite3.OperationalError as exc:
        raise StorageError(f"cannot open {path} (it must exist; create it with init_db): {exc}") from exc
    try:
        _configure(conn, busy_timeout_ms)
        rows = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    except (sqlite3.Error, StorageError) as exc:
        conn.close()
        raise StorageError(f"{path} is not a DAG state database: {exc}") from exc
    if rows.get("run_id") != run_id:
        conn.close()
        raise StorageError(f"{path} belongs to run {rows.get('run_id')!r}, not {run_id!r}")
    if rows.get("schema_version") != str(SCHEMA_VERSION):
        conn.close()
        raise StorageError(f"{path} has schema_version {rows.get('schema_version')}, core is {SCHEMA_VERSION}")
    return conn


class SqliteStore:
    """Every submitter (CLI, controller, watcher) uses this one ``submit()``: no in-process cache;
    each op reads the current records inside its own BEGIN IMMEDIATE transaction."""

    def __init__(self, conn: sqlite3.Connection, *, max_wait_seconds: float = 30.0,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.conn = conn
        self.max_wait_seconds = max_wait_seconds
        self.sleep = sleep

    @classmethod
    def open(cls, path: Path, run_id: str) -> SqliteStore:
        return cls(connect(path, run_id))

    def close(self) -> None:
        self.conn.close()

    def _rollback(self) -> None:
        if self.conn.in_transaction:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass

    def submit(self, op: Op) -> Receipt:
        """Accept one op. SQLITE_BUSY is a scheduling result, retried here with the same op_id;
        a COMMIT failure is rolled back before the retry. Guard rejections and conflicts raise."""
        op.validate()
        deadline = time.monotonic() + self.max_wait_seconds
        delay = 0.01
        while True:
            try:
                return self._submit_once(op)
            except sqlite3.OperationalError as exc:
                self._rollback()
                if not _is_busy(exc):
                    raise StorageError(f"storage error, not accepting ops: {exc}") from exc
                if time.monotonic() >= deadline:
                    raise BusyError(f"database busy for {self.max_wait_seconds}s; retry op {op.op_id}") from exc
                self.sleep(delay)
                delay = min(delay * 2, 0.5)
            except BaseException:
                self._rollback()
                raise

    def _read_state(self) -> tuple[Meta, dict[str, Ticket]]:
        c = self.conn
        meta_rows = dict(c.execute("SELECT key, value FROM meta").fetchall())
        meta = Meta(meta_rows["run_id"], int(meta_rows["schema_version"]), int(meta_rows["authority_epoch"]),
                    meta_rows["mode"], json.loads(meta_rows.get("paused") or "{}"),
                    json.loads(meta_rows.get("seed") or "{}"))
        tickets = {name: Ticket.load(_obj(json.loads(raw), "record"))
                   for name, raw in c.execute("SELECT ticket, record_json FROM tickets")}
        return meta, tickets

    def _imported(self, op: Op, also: Iterable[str] = ()) -> frozenset[str]:
        key = import_key(op)
        if key is None:
            return frozenset()
        if key in set(also) or self.conn.execute("SELECT 1 FROM facts_imported WHERE import_key = ?",
                                                 (key,)).fetchone():
            return frozenset({key})
        return frozenset()

    def _submit_once(self, op: Op) -> Receipt:
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT request_digest, result_json FROM ops WHERE op_id = ?", (op.op_id,)).fetchone()
        if row is not None:
            c.execute("ROLLBACK")
            if row[0] != op.digest():
                raise OpConflict(f"op {op.op_id} was accepted with a different request")
            return Receipt.load(_obj(json.loads(row[1]), "result"), replayed=True)
        meta, tickets = self._read_state()
        tr = reduce(World(tickets, meta, self._imported(op)), op)
        self._write(op, tr)
        self._write_meta(meta, tr.meta)
        c.execute("COMMIT")
        return tr.receipt

    def submit_batch(self, plan: Plan) -> list[Receipt]:
        """Stage B4 compound command (§8 旧 CLI 翻译：先整组校验，再一次事务接受): every op in ONE
        BEGIN IMMEDIATE transaction, reduced in order over the evolving records; any rejection rolls
        all of them back. A callable plan runs inside the transaction on the World it is guarded
        against. BUSY is retried like ``submit`` (the plan is re-run on the fresh World)."""
        deadline = time.monotonic() + self.max_wait_seconds
        delay = 0.01
        while True:
            try:
                return self._submit_batch_once(plan)
            except sqlite3.OperationalError as exc:
                self._rollback()
                if not _is_busy(exc):
                    raise StorageError(f"storage error, not accepting ops: {exc}") from exc
                if time.monotonic() >= deadline:
                    raise BusyError(f"database busy for {self.max_wait_seconds}s; batch not accepted") from exc
                self.sleep(delay)
                delay = min(delay * 2, 0.5)
            except BaseException:
                self._rollback()
                raise

    def _submit_batch_once(self, plan: Plan) -> list[Receipt]:
        c = self.conn
        c.execute("BEGIN IMMEDIATE")
        meta, tickets = self._read_state()
        ops = list(plan(World(copy.deepcopy(tickets), copy.deepcopy(meta))) if callable(plan) else plan)
        prior: dict[str, tuple[str, JsonObj]] = {}
        for o in ops:
            row = c.execute("SELECT request_digest, result_json FROM ops WHERE op_id = ?", (o.op_id,)).fetchone()
            if row is not None:
                prior[o.op_id] = (str(row[0]), _obj(json.loads(row[1]), "result"))
        replayed = _batch_replay(ops, prior)
        if replayed is not None or not ops:
            c.execute("ROLLBACK")
            return replayed or []
        done: list[tuple[Op, Transition]] = []
        current, cur_meta = dict(tickets), meta
        new_keys: set[str] = set()
        for op in ops:
            tr = reduce(World(current, cur_meta, self._imported(op, new_keys)), op)
            current.update(tr.changed)
            cur_meta = tr.meta
            new_keys.update(tr.imported)
            done.append((op, tr))
        for op, tr in done:
            self._write(op, tr)
        self._write_meta(meta, cur_meta)
        c.execute("COMMIT")
        return [tr.receipt for _, tr in done]

    def _write_meta(self, before: Meta, after: Meta) -> None:
        if (after.authority_epoch, after.mode, after.paused, after.seed) != (before.authority_epoch, before.mode, before.paused, before.seed):
            for key, value in (("authority_epoch", str(after.authority_epoch)), ("mode", after.mode),
                               ("paused", canonical(dict(after.paused))), ("seed", canonical(after.seed))):
                self.conn.execute("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET "
                                  "value = excluded.value", (key, value))

    def _write(self, op: Op, tr: Transition) -> None:
        """Ticket rows, pending rows, audit events, import keys and the op receipt of one reduced op."""
        c = self.conn
        for name, t in tr.changed.items():
            c.execute("INSERT INTO tickets (ticket, revision, record_json) VALUES (?, ?, ?) "
                      "ON CONFLICT(ticket) DO UPDATE SET revision = excluded.revision, "
                      "record_json = excluded.record_json", (name, t.revision, canonical(t.to_json())))
            for p in t.pending:
                c.execute("INSERT INTO pending (action_id, ticket, kind, mode, status, created_at, updated_at, "
                          "record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(action_id) DO UPDATE SET "
                          "status = excluded.status, updated_at = excluded.updated_at, "
                          "record_json = excluded.record_json",
                          (p.action_id, name, p.kind, p.mode, p.status, p.created_at, p.updated_at,
                           canonical(json.loads(json.dumps(asdict(p))))))
        seq = 0
        for event in tr.events:
            cur = c.execute("INSERT INTO events (at, kind, ticket, actor, op_id, payload_json, cause) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)", (op.at, event.kind, event.ticket, op.actor, op.op_id,
                                                             canonical(event.payload), event.cause))
            seq = int(cur.lastrowid or 0)
        for record in tr.imported:
            c.execute("INSERT INTO facts_imported (import_key, source_record, ticket, op_id, at) "
                      "VALUES (?, ?, ?, ?, ?)", (record, str(op.payload.get("source_record") or ""), op.ticket,
                                                 op.op_id, op.at))
        tr.receipt.seq = seq or None
        c.execute("INSERT INTO ops (op_id, request_digest, result_json, at) VALUES (?, ?, ?, ?)",
                  (op.op_id, op.digest(), canonical(tr.receipt.to_json()), op.at))

    # -- reads (short, no long-lived read transaction)

    def ticket(self, name: str) -> Ticket | None:
        row = self.conn.execute("SELECT record_json FROM tickets WHERE ticket = ?", (name,)).fetchone()
        return None if row is None else Ticket.load(_obj(json.loads(row[0]), "record"))

    def tickets(self) -> dict[str, Ticket]:
        return {n: Ticket.load(_obj(json.loads(r), "record"))
                for n, r in self.conn.execute("SELECT ticket, record_json FROM tickets")}

    def outbox(self, statuses: Iterable[str] = (QUEUED,)) -> list[Pending]:
        """Outbox rows awaiting the controller (stage B3). B1 never executes them."""
        wanted = tuple(statuses)
        marks = ",".join("?" for _ in wanted)
        rows = self.conn.execute(f"SELECT record_json FROM pending WHERE mode = 'outbox' AND status IN ({marks}) "
                                 "ORDER BY created_at, action_id", wanted).fetchall()
        return [Pending.load(_obj(json.loads(r[0]), "pending")) for r in rows]

    def meta(self) -> dict[str, str]:
        return dict(self.conn.execute("SELECT key, value FROM meta").fetchall())

    def world(self) -> World:
        """A read snapshot for pure helpers such as page_queue (no import keys)."""
        rows = self.meta()
        return World(self.tickets(), Meta(rows["run_id"], int(rows["schema_version"]), int(rows["authority_epoch"]),
                                          rows["mode"], json.loads(rows.get("paused") or "{}"),
                                          json.loads(rows.get("seed") or "{}")))

    def events(self, after_seq: int = 0) -> list[tuple[int, str, str | None, JsonObj]]:
        return [(int(s), str(k), t, _obj(json.loads(p), "event"))
                for s, k, t, p in self.conn.execute(
                    "SELECT seq, kind, ticket, payload_json FROM events WHERE seq > ? ORDER BY seq", (after_seq,))]

    def backup(self, target: Path) -> None:
        """Consistent copy through the Backup API (never copy .db/-wal/-shm by hand)."""
        dest = sqlite3.connect(str(target))
        try:
            self.conn.backup(dest)
        finally:
            dest.close()

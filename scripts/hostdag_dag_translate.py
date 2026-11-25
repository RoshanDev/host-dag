"""Host DAG stage B4: translate a legacy ``host-dag ticket set`` / ``batch set`` into core ops.

Design: docs/local-dev/dag-state-machine-design-20261019.md §8 "旧 CLI 翻译" (the table this module
implements, row by row), §3.3 (commands and their guards), §15 (B4 status).

Classification/integration reducers perform the explicitly authorized live Git checks.
The planner itself does not read files, clocks or databases. ``plan_ticket_set`` is handed the core's World as
read inside the mirror's transaction and returns the ops of one compound command; each op is reduced
on a private copy of that World as it is planned (``hostdag_dag_core.reduce`` is pure), so a later op
is chosen against the state the earlier ones leave, exactly as ``submit_batch`` will reduce them. A
core guard that refuses an op raises out of the plan and the whole compound is refused (atomic).

Every flag the invocation gave is *consumed* by one row of ``TABLE``: translated to an op, a no-op
(already true in the core), a skip (no core equivalent, with the reason), or a refusal with a hint.
``Translation.unconsumed`` lists flags no row looked at; the coverage test requires it to be empty for
every argument shape the coordinator used in the last seven days.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from typing import Mapping, Sequence

import hostdag_dag_core as c
from hostdag_dag_core import Json, JsonObj

# ---------------------------------------------------------------- the translation table (§8)


@dataclass(frozen=True)
class Row:
    row: str
    legacy: str
    core: str


TABLE: tuple[Row, ...] = (
    Row("T01", "ticket set on a ticket the core does not know", "ticket.create (authorized; batch, title, deps)"),
    Row("T02", "--state CREATED on a new ticket", "ticket.create(scheduling=deferred, defer=--after | ticket:<first dep>); "
        "neither -> refused, hint --after"),
    Row("T03", "--state CREATED on a known ticket", "no-op when deferred; skip when authorized (the core never "
        "de-authorizes)"),
    Row("T04", "--title / --deps / free-text --blocker", "ticket.amend(title / deps / notes), only fields that differ"),
    Row("T05", "any non-CREATED state on a deferred ticket", "ticket.authorize"),
    Row("T06", "--state RUNNING | --status CLAIMED | --executor T [--round N]",
        "same session already recorded -> no-op; session registered with the watcher as <ticket> -> "
        "execution.request(adopt), session bookkeeping only (registration is filled from run.json, it proves "
        "nothing); otherwise refused, hint `host-dag exec request` (never guess a dispatch from round)"),
    Row("T07", "--review AXIS=T", "execution.request(role=reviewer, mode=adopt, session=T) when the watcher has T "
        "registered as <ticket>-review-<axis>, bookkeeping only; otherwise skip; recorded -> no-op"),
    Row("T08", "--review AXIS=T:PASS|FAIL", "always skip: facts come from the watcher (B5); no CLI path can submit "
        "review.observed"),
    Row("T09", "--outcome READY_FOR_ACCEPTANCE | --state REVIEWING with --candidate [--evidence]",
        "always skip: facts come from the watcher (B5); no CLI path can submit execution.observed"),
    Row("T10", "--approve-window SPEC [--window-priority N]", "pagewindow.enqueue(spec, spec_digest, grant from the "
        "spec, check at launch); empty SPEC -> skip (a queued window closes by its fact)"),
    Row("T11", "--state WAITING_RESOURCE --wait-on page-window without a spec",
        "already queued / running -> no-op; otherwise pagewindow.enqueue without spec -> prepare_window decision"),
    Row("T12", "--wait-on ticket:X (or --wait-on ticket --wait-holder X)", "wait.add ticket:X (accepted X -> core no-op)"),
    Row("T13", "--wait-on decision:K", "question.ask(to=advisor, key=K), idempotent by key (= question link)"),
    Row("T14", "--wait-on capacity|quota|host|heavy-slot|devcluster-shared",
        "skip: gate waits come only from gate facts (resource.observed)"),
    Row("T15", "--wait-on '' / a state that drops the legacy wait", "wait.waive of the coordinator wait the legacy "
        "record dropped (ticket / time); other kinds close by their fact"),
    Row("T16", "--blocker 'WAIT-UNTIL:<ISO> …'", "wait.add time:<ISO>, or wait.update of the open time wait"),
    Row("T17", "--state NEEDS_USER | --outcome NEEDS_DECISION", "question.ask(to=user, key=<ticket>:needs-user)"),
    Row("T18", "--state EXTERNALLY_BLOCKED | --outcome EXTERNALLY_BLOCKED --blocker B",
        "wait.add external(B), or wait.update when the text changed; structure (owner, attempts, recheck_at) "
        "from the markers 'EXT-OWNER: x | EXT-TRIED: a; b | EXT-RECHECK: <ISO>' in B (missing ones are "
        "flagged by the shadow controller, never refused)"),
    Row("T19", "--state ACCEPTED|INTEGRATED | --status ACCEPTED [--accept-scope S]",
        "ticket.close(accepted, scope, candidate_digest = current candidate; non-FULL needs "
        "--affects-downstream); INTEGRATED evidence is not on ticket set -> accepted only. The core accepts only "
        "with a candidate and PASS verdicts already in it, which come only from watcher facts (B5): until B5 "
        "lands a shadow acceptance is refused and shows up as a diff (expected)"),
    Row("T20", "--state CANCELED | --status SUPERSEDED", "ticket.close(superseded{by=--by}) or close(canceled, reason)"),
    Row("T21", "--state FAILED", "ticket.close(failed, reason=--blocker)"),
    Row("T22", "--window-priority N on a queued window", "pagewindow.policy(priority, reason)"),
    Row("T23", "--status none | --outcome none | --clear-reviews",
        "skip: the core's phase follows commands and facts, review records follow the candidate"),
    Row("T27", "--kind", "ticket.classify with live path verification and archived proof"),
    Row("T24", "--scope --owner --evidence --base --branch --worktree --model --effort --round --attempt "
        "--accepted-at --wait-holder", "skip: board metadata, not core state (model/effort ride on an adopt)"),
    Row("T25", "--state WAITING_DEPENDENCY|WAITING_RESOURCE|REVIEWING|RUNNING with nothing left to translate",
        "no-op: the label is derived by the core (deps, waits, phase, executions)"),
    Row("T26", "any write on a ticket closed in the core", "no-op (a closed record only takes title/notes)"),
    Row("B01", "batch set", "skip: batch metadata (dag, title, tips, remote) is not core state"),
    Row("R01", "peek | board | runs [--add|--scan]", "read-only (runs --add/--scan only edit the run registry)"),
    Row("X01", "an invocation the legacy path refuses (argparse error, ViewError)", "nothing is mirrored"),
)
ROWS = {r.row: r for r in TABLE}

OP, NOOP, SKIP, REFUSE = "op", "noop", "skip", "refuse"

LEGACY_STATES = ("CREATED", "WAITING_DEPENDENCY", "WAITING_RESOURCE", "RUNNING", "REVIEWING", "NEEDS_USER",
                 "EXTERNALLY_BLOCKED", "ACCEPTED", "INTEGRATED", "FAILED", "CANCELED")
WATCH_REVIEW_MARK = "-review-"  # watch.json tracked key of a reviewer: <ticket>-review-<axis>
GATE_WAIT_ON = frozenset({"capacity", "quota", "host", "heavy-slot", "devcluster-shared"})
WAIT_UNTIL = re.compile(r"WAIT-UNTIL:\s*(\S+)")
# A value stays on its marker's own line ([ \t]* never crosses a newline); an empty value is missing,
# and a value that is itself the next marker (``EXT-OWNER: EXT-TRIED: x``) is missing too.
EXT_OWNER = re.compile(r"EXT-OWNER:[ \t]*([^|\n]*)")
EXT_TRIED = re.compile(r"EXT-TRIED:[ \t]*([^|\n]*)")
EXT_RECHECK = re.compile(r"EXT-RECHECK:[ \t]*([^\s|]*)")
EXT_ANY = re.compile(r"EXT-(?:OWNER|TRIED|RECHECK):")
# The `ticket set` fields (cmd_ticket_set) that are board metadata only.
METADATA_FIELDS = ( "root_ticket", "chain_id", "review_token", "review_from", "freeze_exempt", "freeze_reason", "critical_path", "degrade_plan", "degrade_decision", "candidate_decision_ref", "release_disposition", "scope", "owner", "evidence", "base", "branch", "worktree", "model", "effort", "round",
                   "attempt", "accepted_at", "wait_holder", "accept_scope", "window_priority", "candidate")


class TranslationRefused(c.Rejected):
    """The translation itself refuses (a hint for the coordinator), before any core guard."""


@dataclass
class Step:
    row: str
    outcome: str  # op | noop | skip | refuse
    detail: str
    flags: tuple[str, ...] = ()
    kind: str | None = None

    def to_json(self) -> JsonObj:
        return {"row": self.row, "outcome": self.outcome, "detail": self.detail, "flags": list(self.flags),
                "kind": self.kind}


@dataclass
class Translation:
    ops: list[c.Op] = field(default_factory=list)
    steps: list[Step] = field(default_factory=list)
    consumed: set[str] = field(default_factory=set)
    given: set[str] = field(default_factory=set)

    @property
    def unconsumed(self) -> list[str]:
        return sorted(self.given - self.consumed)

    def to_json(self) -> JsonObj:
        return {"ops": [o.kind for o in self.ops], "steps": [s.to_json() for s in self.steps],
                "unconsumed": list(self.unconsumed)}


# ---------------------------------------------------------------- legacy input (runtime-validated)


def _s(value: object) -> str | None:
    return value if isinstance(value, str) and value != "" else None


def _strs(value: object) -> list[str]:
    return [x for x in value if isinstance(x, str)] if isinstance(value, list) else []


@dataclass(frozen=True)
class LegacyTicket:
    """The run.json ticket fields the translation reads (dict[str, Any] never enters the core)."""
    title: str | None = None
    deps: tuple[str, ...] = ()
    state: str | None = None
    status: str | None = None
    outcome: str | None = None
    blocker: str | None = None
    wait_on: str | None = None
    wait_holder: str | None = None
    executor: str | None = None
    candidate: str | None = None
    evidence: str | None = None
    model: str | None = None
    effort: str | None = None
    page_spec: str | None = None
    page_status: str | None = None
    classification_source: Mapping[str, object] = field(default_factory=dict)

    @staticmethod
    def load(raw: object) -> LegacyTicket:
        if not isinstance(raw, dict):
            return LegacyTicket()
        page = raw.get("page_request")
        page = page if isinstance(page, dict) else {}
        state = _s(raw.get("state"))
        return LegacyTicket(_s(raw.get("title")), tuple(_strs(raw.get("deps"))),
                            state.upper() if state else None, _s(raw.get("status")), _s(raw.get("outcome")),
                            _s(raw.get("blocker")), _s(raw.get("wait_on")), _s(raw.get("wait_holder")),
                            _s(raw.get("executor")), _s(raw.get("candidate")), _s(raw.get("evidence")),
                            _s(raw.get("model")), _s(raw.get("effort")), _s(page.get("spec")), _s(page.get("status")),
                            {k: raw[k] for k in ("candidate", "commits", "worktree", "release_disposition") if k in raw})


@dataclass(frozen=True)
class TicketSet:
    """One `host-dag ticket set` as the CLI parsed it. ``fields`` is cmd_ticket_set's dict (None = not
    given; "status"/"outcome" present only when given). ``after``/``affects_downstream``/``by`` are the
    B4 flags the legacy path ignores."""
    batch: str
    ticket: str
    fields: Mapping[str, object]
    after: str | None = None
    affects_downstream: tuple[str, ...] | None = None
    by: str | None = None
    run_dir: str | None = None
    spec_digest: str | None = None  # sha256 of --approve-window's spec, read by the CLI (not a flag)

    def given(self) -> set[str]:
        out = {k for k, v in self.fields.items() if v is not None and v is not False and k != "reviews"}
        out |= {k for k in ("status", "outcome") if k in self.fields}
        if self.fields.get("reviews"):
            out.add("reviews")
        for k, v in (("after", self.after), ("affects_downstream", self.affects_downstream), ("by", self.by)):
            if v is not None:
                out.add(k)
        return out

    def text(self, key: str) -> str | None:
        value = self.fields.get(key)
        return value if isinstance(value, str) else None

    def has(self, key: str) -> bool:
        return key in self.given()


@dataclass(frozen=True)
class WatchSession:
    key: str  # watch.json tracked key: <ticket> or <ticket>-review-<axis>
    thread: str
    terminal: bool


Tracked = Mapping[tuple[str, str], WatchSession]  # (watch.json tracked key, thread) -> entry


def tracked_sessions(watch: object) -> dict[tuple[str, str], WatchSession]:
    """(tracked key, thread) -> tracked entry, from watch.json (runtime-validated). Keyed by both, so
    one thread registered under two keys never shadows the other. Registration is bookkeeping: the
    watcher fills ``tracked`` from the coordinator's own run.json fields, so it proves nothing."""
    tracked = watch.get("tracked") if isinstance(watch, dict) else None
    out: dict[tuple[str, str], WatchSession] = {}
    if isinstance(tracked, dict):
        for key, rec in tracked.items():
            if isinstance(rec, dict) and isinstance(rec.get("thread"), str):
                out[(str(key), rec["thread"])] = WatchSession(str(key), rec["thread"], bool(rec.get("terminal")))
    return out


# ---------------------------------------------------------------- planner


class _Planner:
    def __init__(self, world: c.World, ticket: str, at: str, base: str) -> None:
        self.tickets = dict(world.tickets)
        self.meta = world.meta
        self.name = ticket
        self.at = at
        self.base = base
        self.out = Translation()

    @property
    def t(self) -> c.Ticket | None:
        return self.tickets.get(self.name)

    def submit(self, row: str, kind: str, payload: JsonObj, *, flags: Sequence[str] = (), actor: str = "coordinator",
               detail: str = "", expected_revision: int | None = None) -> c.Receipt:
        op = c.Op(f"{self.base}-{len(self.out.ops):02d}", kind, actor, self.at, self.name, expected_revision, None,
                  payload)
        tr = c.reduce(c.World(self.tickets, self.meta), op)  # raises: the whole compound is refused
        self.tickets.update(tr.changed)
        self.meta = tr.meta
        self.out.ops.append(op)
        self.step(row, OP, detail or tr.receipt.detail or tr.receipt.status, flags, kind)
        return tr.receipt

    def step(self, row: str, outcome: str, detail: str, flags: Sequence[str] = (), kind: str | None = None) -> None:
        self.out.steps.append(Step(row, outcome, detail, tuple(flags), kind))
        self.out.consumed.update(flags)

    def refuse(self, row: str, hint: str, flags: Sequence[str] = ()) -> None:
        self.step(row, REFUSE, hint, flags)
        raise TranslationRefused(f"{row}: {hint}")


def _intent(req: TicketSet) -> str | None:
    """The legacy state this invocation asks for: --state, else what --status/--outcome imply."""
    state = req.text("state")
    if state:
        return state.upper()
    status, outcome = req.text("status"), req.text("outcome")
    if status == "ACCEPTED":
        return "ACCEPTED"
    if status == "SUPERSEDED":
        return "CANCELED"
    if outcome == "NEEDS_DECISION":
        return "NEEDS_USER"
    if outcome == "EXTERNALLY_BLOCKED":
        return "EXTERNALLY_BLOCKED"
    if outcome == "READY_FOR_ACCEPTANCE":
        return "REVIEWING"
    return None


def _wait_until(blocker: str | None) -> str | None:
    m = WAIT_UNTIL.search(blocker or "")
    if not m:
        return None
    try:
        c.parse_at(m.group(1))
    except c.CoreError:
        return None
    return m.group(1)


def new_base() -> str:
    return "hv-" + uuid.uuid4().hex[:16]


def plan_ticket_set(req: TicketSet, before: LegacyTicket, after: LegacyTicket, world: c.World,
                    tracked: Tracked, at: str, base: str) -> Translation:
    """The compound command for one successful legacy `ticket set`. Raises ``c.CoreError`` (refused)."""
    p = _Planner(world, req.ticket, at, base)
    p.out.given = req.given()
    intent = _intent(req)
    state_flags = [f for f in ("state", "status", "outcome") if req.has(f)]
    if after.state in ('FROZEN_GATE','DEGRADING','READY_DEGRADE_A','CANDIDATE_BLOCKED'):
        # L1265 scheduling fence is board-owned; never authorize work in the
        # frozen Stage B mirror merely because a frozen row was created.
        p.step('T24', SKIP, 'freeze-gate scheduling metadata; no core authorization', sorted(p.out.given))
        return p.out
    if intent is not None and intent not in LEGACY_STATES:
        p.refuse("X01", f"unknown state {intent}", state_flags)

    # T01/T02: the ticket itself
    if p.t is None:
        deps = list(after.deps)
        title = after.title or req.ticket
        if intent == "CREATED":
            defer = (f"ticket:{req.after}" if req.after and ":" not in req.after else req.after) or \
                (f"ticket:{deps[0]}" if deps else None)
            if defer is None:
                p.refuse("T02", "a new CREATED ticket needs --after <ticket> or batch:<name>:bound|completed",
                         ["state", "after"])
            p.submit("T02", "ticket.create", {"batch": req.batch, "title": title, "deps": list(deps),
                                              "scheduling": c.DEFERRED, "defer": defer},
                     flags=["state", "title", "deps", "after"])
        else:
            p.submit("T01", "ticket.create", {"batch": req.batch, "title": title, "deps": list(deps),
                                              "scheduling": c.AUTHORIZED}, flags=["title", "deps"])
    t = p.t
    assert t is not None

    # T04: amendable fields that differ
    amend: JsonObj = {}
    if req.has("title") and after.title and after.title != t.title:
        amend["title"] = after.title
    if req.has("deps") and list(after.deps) != t.deps and t.phase != c.CLOSED:
        amend["deps"] = list(after.deps)
    blocker = req.text("blocker")
    if blocker is not None and blocker != t.notes:
        amend["notes"] = blocker
    for key in ("release_group", "requires_for_release", "defect_class", "regression", "production_write_fence", "coverage_migration"):
        if req.has(key):
            amend[key] = req.fields[key]
    if amend:
        p.submit("T04", "ticket.amend", amend, flags=[{"notes": "blocker"}.get(k, k) for k in amend])
    for f in ("title", "deps", "blocker"):
        if req.has(f) and f not in p.out.consumed:
            p.step("T04", NOOP, f"{f} already as recorded (or closed)", [f])

    t = p.t
    assert t is not None
    closed = t.phase == c.CLOSED

    # T03/T05: scheduling
    if intent == "CREATED":
        if t.scheduling == c.DEFERRED:
            p.step("T03", NOOP, "deferred", state_flags + (["after"] if req.after else []))
        else:
            p.step("T03", SKIP, "authorized in the core; the core never de-authorizes",
                   state_flags + (["after"] if req.after else []))
    elif t.scheduling == c.DEFERRED and not closed and (intent is not None or req.has("executor")):
        p.submit("T05", "ticket.authorize", {}, flags=[])

    if req.has("kind"):
        p.submit("T27", "ticket.classify", {"kind": "ops" if req.text("kind") == "runtime-only" else req.text("kind"),
                 "ticket_source": dict(after.classification_source), "run_dir": req.run_dir}, flags=["kind"])

    # T24 metadata (consumed up front; adopt below may still read model/effort)
    meta_flags = [f for f in METADATA_FIELDS if req.has(f) and f not in ("accept_scope", "window_priority",
                                                                            "candidate", "wait_holder")]
    if meta_flags:
        p.step("T24", SKIP, "board metadata, not core state", meta_flags)

    # T23 resets
    resets = [f for f in ("status", "outcome") if f in req.fields and req.fields[f] is None]
    if resets or req.has("clear_reviews"):
        p.step("T23", SKIP, "legacy reset; core phase follows commands and facts",
               resets + (["clear_reviews"] if req.has("clear_reviews") else []))

    if closed:
        rest = sorted(p.out.given - p.out.consumed)
        if rest:
            p.step("T26", NOOP, f"{req.ticket} is closed ({t.closure.kind if t.closure else ''}) in the core", rest)
        return p.out

    # T06 executor
    _executor(p, req, after, intent, tracked, state_flags)
    # T09 candidate, before the reviews (review fix P2-2): a call that brings the candidate and its
    # reviewers in one go must do the same thing the first and the second time
    _candidate(p, req, after, intent, state_flags)
    # T07/T08 reviews
    _reviews(p, req, tracked)
    # waits (T10-T18, T22)
    _waits(p, req, before, after, intent, state_flags)
    # T19-T21 closures
    _closure(p, req, after, intent, state_flags)

    # whatever --state/--status/--outcome was not translated by a row above is a state the core
    # derives (WAITING_DEPENDENCY from deps, REVIEWING from the phase, ...)
    rest = [f for f in state_flags if f not in p.out.consumed]
    if rest:
        p.step("T25", NOOP, f"{intent} is derived by the core (label {c.label(p.t, p.tickets) if p.t else '?'})", rest)
    return p.out


def _executor(p: _Planner, req: TicketSet, after: LegacyTicket, intent: str | None,
              tracked: Tracked, state_flags: list[str]) -> None:
    claims = intent == "RUNNING" or req.text("status") == "CLAIMED" or req.has("executor")
    if not claims:
        return
    flags = [f for f in ("executor", "round") if req.has(f)]
    if intent == "RUNNING" or req.text("status") == "CLAIMED":
        flags += [f for f in state_flags if f in ("state", "status")]
    t = p.t
    assert t is not None
    session = req.text("executor") or after.executor
    current = t.current_executor()
    if session is None:
        if t.active():  # the executor, or in REVIEW the reviewers (the core's RUNNING·审查)
            p.step("T06", NOOP, f"{t.active()[0].role} {t.active()[0].execution_id} is running", flags)
            return
        if t.phase == c.REVIEW:  # re-review N4: the board's RUNNING over a candidate in review, nothing to claim
            p.step("T06", NOOP, "in review in the core; no executor session given, nothing to claim", flags)
            return
        p.refuse("T06", "no executor session to adopt; use `host-dag exec request` (new_attempt / resume / adopt)",
                 flags)
        return
    same = next((e for e in t.executions if e.role == "executor" and e.session == session), None)
    if same is not None:
        if same is current and same.observed in ("running", "starting", "unknown"):
            p.step("T06", NOOP, f"session already tracked as {same.execution_id}", flags)
        else:
            p.step("T06", NOOP, f"session {session} recorded as {same.execution_id} ({same.observed}); "
                   "a resume is `host-dag exec request --mode resume`", flags)
        return
    seen = tracked.get((req.ticket, session))  # bookkeeping: names the session, proves nothing
    if seen is None:
        p.refuse("T06", f"executor {session} is not tracked by the watcher for {req.ticket}; use "
                 "`host-dag exec request --mode new_attempt|resume|adopt` to say which", flags)
        return
    if t.active("executor"):
        p.refuse("T06", f"{req.ticket} already runs {t.active('executor')[0].execution_id}; a different session "
                 "is `host-dag exec request`", flags)
        return
    payload: JsonObj = {"role": "executor", "mode": "adopt", "session": session,
                        "observed": "finished" if seen.terminal else "running"}
    for key, value in (("model", req.text("model") or after.model), ("effort", req.text("effort") or after.effort)):
        if value:
            payload[key] = value
    p.submit("T06", "execution.request", payload, flags=flags)


FACTS_FROM_WATCHER = "facts come from the watcher (B5)"


def _reviews(p: _Planner, req: TicketSet, tracked: Tracked) -> None:
    """T07 reviewer adoption is session bookkeeping only (B4 redesign): it lets the core name the
    review's execution, it proves nothing. T08 never forwards a verdict: watch.json ``tracked`` is filled
    from the coordinator's own run.json writes (hostdag_watch_registration.reconcile), so nothing the
    CLI sees is evidence of a review result. Verdicts reach the core only as watcher facts (B5)."""
    reviews = req.fields.get("reviews")
    if not isinstance(reviews, dict) or not reviews:
        return
    for axis, body in sorted(reviews.items()):
        body = body if isinstance(body, dict) else {}
        thread, verdict = _s(body.get("thread")), _s(body.get("verdict"))
        t = p.t
        assert t is not None
        if axis not in c.AXES:
            p.refuse("T07", f"unknown review axis {axis}", ["reviews"])
        key = f"{req.ticket}{WATCH_REVIEW_MARK}{axis}"
        if verdict:
            p.step("T08", SKIP, f"{axis} {verdict}: {FACTS_FROM_WATCHER}", ["reviews"])
        if not thread:
            continue
        if t.phase != c.REVIEW or t.candidate is None:
            p.step("T07", SKIP, f"{axis}: no candidate under review in the core", ["reviews"])
            continue
        current = t.current_reviewer(axis)
        if current is not None and current.session == thread:
            p.step("T07", NOOP, f"{axis} reviewer {thread} already recorded", ["reviews"])
        elif current is not None and current.observed in c.ACTIVE:
            p.step("T07", SKIP, f"{axis} review already running as {current.execution_id}", ["reviews"])
        elif axis in t.reviews_for_current() and t.reviews_for_current()[axis].verdict:
            p.step("T07", NOOP, f"{axis} already has a verdict on the current candidate", ["reviews"])
        elif (key, thread) not in tracked:
            p.step("T07", SKIP, f"{axis} reviewer {thread} is not registered with the watcher as {key}; "
                   "adopted once it is (bookkeeping only)", ["reviews"])
        else:
            p.submit("T07", "execution.request", {"role": "reviewer", "axis": axis, "mode": "adopt", "session": thread},
                     flags=["reviews"], detail=f"{axis} reviewer adopted (bookkeeping, not evidence)")


def _candidate(p: _Planner, req: TicketSet, after: LegacyTicket, intent: str | None, state_flags: list[str]) -> None:
    """T09 never forwards a result candidate (B4 redesign): the candidate reaches the core only as the
    watcher's execution.observed fact (B5). Until then a ticket the board shows as REVIEWING stays in WORK
    in the core, and that is an expected shadow diff."""
    if intent != "REVIEWING" and not (req.has("candidate") and req.text("outcome") == "READY_FOR_ACCEPTANCE"):
        if req.has("candidate"):
            p.step("T24", SKIP, "candidate without READY_FOR_ACCEPTANCE/REVIEWING is board metadata", ["candidate"])
        return
    flags = [f for f in state_flags if f in ("state", "outcome")] + (["candidate"] if req.has("candidate") else [])
    p.step("T09", SKIP, f"candidate {req.text('candidate') or after.candidate or '-'}: {FACTS_FROM_WATCHER}", flags)


def _waits(p: _Planner, req: TicketSet, before: LegacyTicket, after: LegacyTicket, intent: str | None,
           state_flags: list[str]) -> None:
    t = p.t
    assert t is not None
    wait_on = req.text("wait_on")
    kind, _, detail = (wait_on or "").partition(":")
    wflags = ["wait_on"] + (["wait_holder"] if req.has("wait_holder") else [])
    sflags = [f for f in state_flags if f == "state"] if intent in ("WAITING_RESOURCE", "WAITING_DEPENDENCY") else []

    # T10 / T22: page-window approval and priority
    spec = req.text("window_spec")
    spec_flags = ["window_spec", "window_priority"] + sflags + (wflags if kind == "page-window" else [])
    if spec is not None:
        if spec == "":
            p.step("T10", SKIP, "withdrawn in the legacy queue; a queued core window closes by its fact", spec_flags)
        elif any(w.open and w.kind == "page-window" for w in t.waits):
            p.step("T10", NOOP, "a page window is already queued in the core", spec_flags)
        else:
            payload: JsonObj = {"spec": after.page_spec or spec, "grant": True, "checked": True,
                                "check_at_launch": True}
            if req.spec_digest:
                payload["spec_digest"] = req.spec_digest
            prio = req.fields.get("window_priority")
            if isinstance(prio, int):
                payload["priority"] = prio
            p.submit("T10", "pagewindow.enqueue", payload, flags=spec_flags)
    elif req.has("window_priority"):
        prio = req.fields.get("window_priority")
        queued = [x for x in t.pending if x.kind == "run_window" and x.open]
        if queued and isinstance(prio, int):
            have = queued[0].payload.get("priority")
            if prio == (have if isinstance(have, int) else 0):
                p.step("T22", NOOP, f"priority already {prio}", ["window_priority"])
            else:
                p.submit("T22", "pagewindow.policy", {"priority": prio, "reason": "host-dag ticket set --window-priority"},
                         flags=["window_priority"])
        else:
            p.step("T22", SKIP, "no queued page window in the core", ["window_priority"])

    t = p.t
    assert t is not None
    # T11-T14: --wait-on
    if wait_on:
        if kind == "page-window":
            if "wait_on" in p.out.consumed:
                pass
            elif any(w.kind == "page-window" and (w.open or (w.satisfied_at and w.detail.get("outcome") is None))
                     for w in t.waits):
                p.step("T11", NOOP, "page window already queued or running in the core", wflags + sflags)
            elif any(x.kind == "prepare_window" and x.status in c.OUTSTANDING for x in t.pending):
                p.step("T11", NOOP, "prepare_window already open", wflags + sflags)
            elif after.page_spec and after.page_status in ("pending", "claimed"):
                p.submit("T11", "pagewindow.enqueue", {"spec": after.page_spec, "grant": True, "checked": True,
                                                       "check_at_launch": True}, flags=wflags + sflags)
            else:
                p.submit("T11", "pagewindow.enqueue", {}, flags=wflags + sflags,
                         detail="no approved spec: prepare_window decision")
        elif kind == "ticket":
            target = detail or req.text("wait_holder") or after.wait_holder
            if not target:
                p.refuse("T12", "--wait-on ticket needs the ticket (ticket:X or --wait-holder X)", wflags)
                return
            if any(w.open and w.kind == "ticket" and w.ref == target for w in t.waits):
                p.step("T12", NOOP, f"already waiting on ticket:{target}", wflags + sflags)
            else:
                p.submit("T12", "wait.add", {"kind": "ticket", "ref": target}, flags=wflags + sflags)
        elif kind == "decision":
            key = detail or req.text("wait_holder") or ""
            if not key:
                p.refuse("T13", "--wait-on decision needs a question key (decision:K)", wflags)
                return
            title = req.text("blocker") or after.blocker or key
            p.submit("T13", "question.ask", {"to": "advisor", "key": key, "title": title[:500]}, flags=wflags + sflags)
        elif kind in GATE_WAIT_ON:
            p.step("T14", SKIP, f"{kind} waits come only from gate facts (resource.observed)", wflags + sflags)
        else:
            p.step("T14", SKIP, f"{kind}: no core wait kind", wflags + sflags)
    elif req.has("wait_holder") and "wait_holder" not in p.out.consumed:
        p.step("T24", SKIP, "wait holder is board metadata", ["wait_holder"])

    # T15: waits the legacy record dropped
    t = p.t
    assert t is not None
    dropped: list[c.Wait] = []
    b_kind, _, b_detail = (before.wait_on or "").partition(":")
    a_kind, _, a_detail = (after.wait_on or "").partition(":")
    if b_kind == "ticket" and not (a_kind == "ticket" and (a_detail or after.wait_holder) == (b_detail or before.wait_holder)):
        target = b_detail or before.wait_holder
        dropped += [w for w in t.waits if w.open and w.kind == "ticket" and w.ref == target]
    if _wait_until(before.blocker) and not _wait_until(after.blocker):
        dropped += [w for w in t.waits if w.open and w.kind == "time"]
    for w in dropped:
        p.submit("T15", "wait.waive", {"wait_id": w.wait_id, "reason": "the legacy record dropped this wait"},
                 flags=[f for f in ("wait_on", "blocker", "state") if req.has(f) and f not in p.out.consumed])
    if wait_on == "" and "wait_on" not in p.out.consumed:
        p.step("T15", NOOP if not dropped else OP, "wait cleared; other kinds close by their fact", ["wait_on"])

    # T16: WAIT-UNTIL
    until = _wait_until(req.text("blocker"))
    if until:
        t = p.t
        assert t is not None
        open_time = [w for w in t.waits if w.open and w.kind == "time"]
        if open_time:
            if open_time[0].ref == until:
                p.step("T16", NOOP, f"time wait {until} already open", ["blocker"])
            else:
                p.submit("T16", "wait.update", {"wait_id": open_time[0].wait_id, "ref": until,
                                                "reason": "WAIT-UNTIL rescheduled"}, flags=["blocker"])
        else:
            p.submit("T16", "wait.add", {"kind": "time", "ref": until}, flags=["blocker"])

    # T17: needs user
    if intent == "NEEDS_USER":
        title = req.text("blocker") or after.blocker or "needs user decision"
        p.submit("T17", "question.ask", {"to": "user", "key": f"{req.ticket}:needs-user", "title": title[:500]},
                 flags=[f for f in state_flags if f in ("state", "outcome", "status")])
    # T18: externally blocked
    if intent == "EXTERNALLY_BLOCKED":
        text = req.text("blocker") or after.blocker or "externally blocked"
        t = p.t
        assert t is not None
        ext = [w for w in t.waits if w.open and w.kind == "external"]
        flags = [f for f in state_flags if f in ("state", "outcome", "status")]
        ext_detail = external_markers(text)
        if ext:
            if ext[0].ref == text:
                p.step("T18", NOOP, "external wait already recorded", flags)
            else:
                upd: JsonObj = {"wait_id": ext[0].wait_id, "ref": text, "reason": "reworded by ticket set"}
                # No EXT-* marker at all: the pre-HD-01 payload byte for byte, the stored detail kept.
                # Any marker, even with only empty/invalid values: the parsed structure replaces the
                # stored one, so bad values read as missing and the shadow asks again.
                if EXT_ANY.search(text):
                    upd["detail"] = ext_detail
                p.submit("T18", "wait.update", upd, flags=flags)
        else:
            ext_payload: JsonObj = {"kind": "external", "ref": text}
            if EXT_ANY.search(text):
                ext_payload["detail"] = ext_detail
            p.submit("T18", "wait.add", ext_payload, flags=flags)


def external_markers(text: str) -> JsonObj:
    """T18 structure from the blocker text: ``EXT-OWNER: x``, ``EXT-TRIED: a; b`` and
    ``EXT-RECHECK: <ISO>`` (fields separated by ``|`` or newlines). A marker that does not parse is
    left out, so the shadow controller flags it as missing instead of the compound being refused."""
    out: JsonObj = {}

    def value(pattern: re.Pattern[str]) -> str:
        m = pattern.search(text)
        found = m.group(1).strip() if m else ""
        return "" if found.startswith("EXT-") else found

    owner = value(EXT_OWNER)
    if owner:
        out["owner"] = owner
    tried = value(EXT_TRIED)
    if tried:
        attempts: list[Json] = [a.strip() for a in re.split(r"[;；]", tried) if a.strip()]
        if attempts:
            out["attempts"] = attempts
    recheck = EXT_RECHECK.search(text) if value(EXT_RECHECK) else None
    if recheck:
        try:
            c.parse_at(recheck.group(1))
            out["recheck_at"] = recheck.group(1)
        except c.CoreError:
            pass
    return out


def _closure(p: _Planner, req: TicketSet, after: LegacyTicket, intent: str | None, state_flags: list[str]) -> None:
    flags = [f for f in state_flags if f in ("state", "status")]
    t = p.t
    assert t is not None
    if intent in ("ACCEPTED", "INTEGRATED"):
        flags += [f for f in ("accept_scope", "affects_downstream", "candidate") if req.has(f)]
        scope = (req.text("accept_scope") or "FULL").upper()
        digest = t.candidate.digest if t.candidate is not None else (req.text("candidate") or after.candidate)
        if digest is None:
            p.refuse("T19", f"{req.ticket} has no candidate in the core to accept", flags)
            return
        payload: JsonObj = {"kind": "accepted", "scope": scope, "candidate_digest": digest}
        if scope != "FULL":
            if req.affects_downstream is None:
                p.refuse("T19", f"scope {scope} needs --affects-downstream (may be empty: '')", flags)
                return
            payload["affects_downstream"] = list(req.affects_downstream)
        p.submit("T19", "ticket.close", payload, flags=flags,
                 detail="accepted" + ("; integration evidence is recorded by batch set, not mirrored"
                                      if intent == "INTEGRATED" else ""))
    elif intent == "CANCELED":
        if req.by:
            p.submit("T20", "ticket.close", {"kind": "superseded", "by": req.by}, flags=flags + ["by"])
        else:
            p.submit("T20", "ticket.close", {"kind": "canceled",
                                             "reason": req.text("blocker") or after.blocker or "canceled by ticket set"},
                     flags=flags)
    elif intent == "FAILED":
        p.submit("T21", "ticket.close", {"kind": "failed",
                                         "reason": req.text("blocker") or after.blocker or "failed by ticket set"},
                 flags=flags)
    if req.has("accept_scope") and "accept_scope" not in p.out.consumed:
        p.step("T24", SKIP, "accept scope without an acceptance is board metadata", ["accept_scope"])
    if req.has("affects_downstream") and "affects_downstream" not in p.out.consumed:
        p.step("T19", SKIP, "--affects-downstream only applies to a SCOPED/REDUCED acceptance", ["affects_downstream"])
    if req.has("by") and "by" not in p.out.consumed:
        p.step("T20", SKIP, "--by only applies to --status SUPERSEDED / --state CANCELED", ["by"])
    if req.has("after") and "after" not in p.out.consumed:
        p.step("T02", SKIP, "--after only applies to a new CREATED ticket", ["after"])


def plan_batch_set(given: Sequence[str]) -> Translation:
    out = Translation(given=set(given))
    out.steps.append(Step("B01", SKIP, ROWS["B01"].core, tuple(given)))
    out.consumed.update(given)
    return out


# ---------------------------------------------------------------- argument shapes (coverage, §10 B4)


READ_ONLY = frozenset({"peek", "board", "runs"})
ENUM_FLAGS = {"--state": "upper", "--status": "upper", "--outcome": "upper", "--accept-scope": "upper"}


def _shape_value(flag: str, value: str) -> str:
    if flag in ENUM_FLAGS:
        return value.upper() if re.fullmatch(r"[A-Za-z_]+", value) else "<other>"
    if flag == "--wait-on":
        if value == "":
            return "<empty>"
        kind = value.partition(":")[0]
        return (kind if re.fullmatch(r"[a-z-]+", kind) else "<other>") + (":<detail>" if ":" in value else "")
    if flag == "--review":
        axis, _, rest = value.partition("=")
        _, _, verdict = rest.partition(":")
        return f"{axis if axis in c.AXES else '<other>'}=<thread>" + (f":{verdict}" if verdict in ("PASS", "FAIL") else
                                                                      (":<other>" if verdict else ""))
    if flag == "--attempt":
        return "+1" if value == "+1" else "<n>"
    if flag == "--blocker":
        if value == "":
            return "<empty>"
        if value.startswith("WAIT-UNTIL:"):
            return "WAIT-UNTIL:<time>"
        return "<text>"
    if flag in ("--approve-window", "--wait-holder", "--deps"):
        return "<empty>" if value == "" else "<value>"
    return "<value>"


def shape_of(argv: Sequence[str]) -> tuple[str, ...]:
    """A sanitized argument shape: subcommand words, then each flag with a value class (enumerated
    values kept, free text/ids/paths replaced). Same flag order-insensitive; repeated flags kept."""
    words: list[str] = []
    flags: list[str] = []
    i = 0
    args = list(argv)
    while i < len(args):
        a = args[i]
        if a.startswith("--"):
            name, eq, inline = a.partition("=")
            value: str | None = inline if eq else None
            takes = name not in ("--clear-reviews", "--follow", "--tools", "--help", "-h", "-f")
            if value is None and takes and i + 1 < len(args) and not (args[i + 1].startswith("--")
                                                                     and args[i + 1] != "--"):
                value = args[i + 1]
                i += 1
            if name == "--run":
                flags.append("--run=<run>")
            else:
                flags.append(name if value is None else f"{name}={_shape_value(name, value)}")
        elif a in ("-h", "-f"):
            flags.append(a)
        elif a.startswith("-") and len(a) == 2:
            name = a
            value = args[i + 1] if i + 1 < len(args) else ""
            i += 1
            flags.append(f"{name}=<value>")
        else:
            words.append(a if not words or (len(words) < 2 and re.fullmatch(r"[a-z-]+", a)) else "<arg>")
        i += 1
    return tuple(words[:2]) + (("<arg>",) if len(words) > 2 else ()) + tuple(sorted(flags))


def classify_shape(shape: Sequence[str]) -> str:
    """mutating | read-only | registry | invalid (the legacy parser refuses it: nothing is written)."""
    words = [w for w in shape if not w.startswith("-")]
    if not words:
        return "invalid"
    if words[0] in READ_ONLY:
        if words[0] == "runs" and any(f.startswith(("--add", "--scan")) for f in shape):
            return "registry"
        return "read-only"
    if words[:2] in (["ticket", "set"], ["batch", "set"]):
        if any(f in ("--help", "-h") for f in shape):
            return "help"
        return "invalid" if len(words) > 2 else "mutating"  # a positional after `set`: argparse refuses
    if words[0] in ("ticket", "batch") and any(f in ("--help", "-h") for f in shape):
        return "help"
    return "invalid"

"""Host DAG resolvers, stage B2: read the outside world, return core fact ops.

Design: docs/local-dev/dag-state-machine-design-20261019.md §4 (typed waits and resolvers), §4.2 (one
resolver per wait kind), §11.7 (advisor key alias table), §11.8 (capacity probe), §13 (runtime).

A resolver is a read-only reconciliation function: it reads the current core records (a ``World``)
and external facts (advisor log, page-window library files, capacity samples, the clock reading it
is given) and returns ``hostdag_dag_core.Op`` facts plus ``Note`` findings it must not turn into
facts. Nothing here writes a file, starts a process or opens state.db; the B3 controller submits
the ops (``apply``) on its own per-cycle connection.

Idempotence, in three layers:
1. Every op is content-addressed: ``op_id`` is a digest of the op's request (kind, actor, ticket,
   payload; never ``at``), so the same inputs give the same op and a resubmission is a replay.
2. Every fact carries a ``source_record`` naming the external record it came from; the core's
   per-wait import key (source_record, ticket, kind, target, event) turns a second import of the
   same record into a noop even if another submitter (the B6 advisor script) built the op.
3. Resolvers only emit for waits that are still open in the World they were given, so a steady
   state produces no ops at all.

Ticket and batch waits are not resolved here: the core settles them inside every accepted
transaction from its own records (§4.2 "内部，不读文件"), and ``wait.satisfied`` refuses them. The
ticket resolver therefore only reports (``ticket_findings``).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Protocol, Sequence

from hostdag_advisor_log import is_provisional_answer
import hostdag_dag_core as c
from hostdag_dag_core import Json, JsonObj

DATA_DIR = Path(__file__).resolve().parent / "dag-sm-data"
ALIAS_TABLE = DATA_DIR / "advisor-key-aliases.json"
ALIAS_SCHEMA = "host-dag-dag-sm/advisor-key-aliases/1"
SAMPLE_SCHEMA = "host-dag-dag-sm/capacity-sample/1"

ADVISOR_LOG_REL = "advisor/log.jsonl"
ADVISOR_PREFIX = "advisor:"
DRIFT_PREFIX = "pagewindow-drift:"
ADVISOR_SOURCE = "advisor-log:sha256:"

REGISTRY_REL = "control/api-page-validation-pause.json"
CLAIMS_REL = "control/pagewindow-claims.ndjson"
PAGE_RETRY_SECONDS = 5 * 60  # same back-off as the stage A watcher (host-dag-host-watch.PAGE_RETRY_SECONDS)
# Missing identity receipts require review after this observation threshold.
CLAIM_RECEIPT_STALL_SECONDS = 30 * 60

CAPACITY_REL = "control"
# A sample older than this is stale: it neither releases a wait nor counts as "still short" (noted).
# The host probe samples every 2 minutes and the node probe every 5; one missed node sample is tolerated.
CAPACITY_FRESH_SECONDS = 10 * 60
CAPACITY_BACKOFF_SECONDS = 10 * 60  # §4.2: a pool releases a ticket's capacity wait at most once per 10 minutes
NODE_POOL = "node-memory"
HOST_MEMORY_POOL = "host-memory"
HOST_IO_POOL = "host-io"  # value = 100 - the busiest block device's busy% (io_ticks), never /proc/pressure/io
POOL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")

DISK_MIN_FREE_BYTES = 20 * 1024**3  # §13.3: below 20 GiB or 2 % free nothing more is written to the run disk
DISK_MIN_FREE_RATIO = 0.02

FACT_ACTOR = {"question": "advisor", "page-window": "pagewindow", "capacity": "probe", "time": "controller"}


# ---------------------------------------------------------------- results


@dataclass(frozen=True)
class Note:
    """Something a resolver saw and deliberately did not turn into a fact (ambiguity, stale answer,
    unreadable file, a finding the core resolves itself). The controller surfaces these."""
    resolver: str
    kind: str
    ticket: str | None
    ref: str
    detail: str

    def to_json(self) -> JsonObj:
        return {"resolver": self.resolver, "kind": self.kind, "ticket": self.ticket, "ref": self.ref,
                "detail": self.detail}


@dataclass
class Resolution:
    ops: list[c.Op] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)

    def extend(self, other: Resolution) -> Resolution:
        self.ops.extend(other.ops)
        self.notes.extend(other.notes)
        return self

    def note(self, resolver: str, kind: str, ticket: str | None, ref: str, detail: str) -> None:
        self.notes.append(Note(resolver, kind, ticket, ref, detail))


def fact_op(kind: str, actor: str, ticket: str | None, payload: JsonObj, at: str, *,
            clock_fields: Iterable[str] = ()) -> c.Op:
    """A content-addressed fact: same request -> same op_id, whatever ``at`` says. ``clock_fields``
    are payload fields that only carry the reconciliation clock (a time wait's observed_at); they are
    left out of the id too (B2 review L1), so a later cycle builds the same op_id."""
    body = json.loads(json.dumps(payload))
    skip = set(clock_fields)
    probe = c.Op("-", kind, actor, at, ticket, None, None, {k: v for k, v in body.items() if k not in skip})
    digest = hashlib.sha256(c.canonical(probe.request()).encode()).hexdigest()[:24]
    op = c.Op(f"rsv-{kind.split('.')[0]}-{digest}", kind, actor, at, ticket, None, None, body)
    op.validate()
    return op


# ---------------------------------------------------------------- small helpers


def _num(value: Json) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _epoch(iso: str) -> float:
    return c.parse_at(iso).timestamp()


def iso_utc(epoch: float) -> str:
    """Deterministic ISO form of an epoch reading (UTC, microseconds)."""
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="microseconds")


def _read_json(path: Path) -> JsonObj | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _sorted_tickets(world: c.World) -> list[c.Ticket]:
    return [world.tickets[name] for name in sorted(world.tickets)]


def _str(value: Json) -> str | None:
    return value if isinstance(value, str) and value else None


# ---------------------------------------------------------------- advisor log + alias table


@dataclass(frozen=True)
class AdvisorRecord:
    line_no: int
    source_record: str
    key: str
    at: float
    verdict: str
    decision: str
    question_file: str | None
    offset: int = 0


def advisor_source_record(raw_line: str, offset: int = 0) -> str:
    """The identity of one advisor log line (design §4.2: 日志行 sha). The B6 advisor script must use
    this same function so its own question.answered and the controller's import dedup."""
    return ADVISOR_SOURCE + hashlib.sha256(raw_line.rstrip("\r\n").encode("utf-8")).hexdigest() + f":offset:{offset}"


def parse_advisor_log(text: str) -> tuple[list[AdvisorRecord], list[Note]]:
    """Answered records in log order. A line that is not JSON, has no key, no verdict (a question
    still open) or no time is skipped; unparsable lines are reported."""
    records: list[AdvisorRecord] = []
    pending_after: dict[str, int] = {}
    notes: list[Note] = []
    offset = 0
    for n, line in enumerate(text.splitlines(keepends=True), 1):
        raw = line.rstrip("\r\n")
        line_offset, offset = offset, offset + len(line.encode("utf-8"))
        if not raw.strip():
            continue
        try:
            rec = json.loads(raw)
        except ValueError:
            notes.append(Note("question", "unreadable_log_line", None, f"line {n}", "advisor log line is not JSON"))
            continue
        if not isinstance(rec, dict):
            continue
        # A provisional answer is coordinator-only guidance, not a final answer binding.
        # Ignore pre-fix samples too; their later Fable verdict must remain consumable.
        if is_provisional_answer(rec):
            key = _str(rec.get("key"))
            if key is not None:
                pending_after[key] = n
            continue
        key, verdict, at = _str(rec.get("key")), _str(rec.get("verdict")), _num(rec.get("at"))
        if key is None or verdict is None:
            continue
        if at is None:
            notes.append(Note("question", "unreadable_log_line", None, f"line {n}", f"{key}: answer has no time"))
            continue
        records.append(AdvisorRecord(n, advisor_source_record(raw, line_offset), key, at, verdict,
                                     str(rec.get("decision") or ""), _str(rec.get("question_file")), line_offset))
    # Retain legacy ordering, but a new pending review supersedes earlier answers of that key.
    return [rec for rec in records if rec.line_no > pending_after.get(rec.key, 0)], notes


def build_alias_table(text: str) -> JsonObj:
    """§11.7 explicit alias table from the historical advisor log (keys only, no bodies).

    Rules (the only ones stage A's ``endswith`` matching relied on in practice): a key written with
    the ``advisor:`` prefix is also reachable without it and the other way round. An alias that names
    a real key is not an alias. An alias reaching two keys is ambiguous and never auto-resolves.
    ``suffix_collisions`` documents key pairs the old ``endswith`` match would have conflated."""
    keys: list[str] = []
    seen: set[str] = set()
    raw_records = 0
    ats: list[float] = []
    for raw in text.splitlines():
        try:
            rec = json.loads(raw) if raw.strip() else None
        except ValueError:
            continue
        if not isinstance(rec, dict):
            continue
        key = _str(rec.get("key"))
        if key is None:
            continue
        raw_records += 1
        at = _num(rec.get("at"))
        if at is not None:
            ats.append(at)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    targets: dict[str, set[str]] = {}
    for key in keys:
        alias = key[len(ADVISOR_PREFIX):] if key.startswith(ADVISOR_PREFIX) else ADVISOR_PREFIX + key
        if alias and alias not in seen:
            targets.setdefault(alias, set()).add(key)
    aliases: JsonObj = {a: sorted(t)[0] for a, t in sorted(targets.items()) if len(t) == 1}
    ambiguous: JsonObj = {a: list(sorted(t)) for a, t in sorted(targets.items()) if len(t) > 1}
    collisions: list[Json] = [[a, b] for a in sorted(seen) for b in sorted(seen) if a != b and b.endswith(a)]
    return {"schema": ALIAS_SCHEMA,
            "source": {"log": ADVISOR_LOG_REL, "records": raw_records, "keys": len(keys),
                       "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                       "first_at": iso_utc(min(ats)) if ats else None, "last_at": iso_utc(max(ats)) if ats else None},
            "rules": ["exact key", "advisor: prefix added or removed"],
            "keys": list(sorted(seen)), "aliases": aliases, "ambiguous": ambiguous,
            "suffix_collisions": collisions}


@dataclass(frozen=True)
class AliasTable:
    aliases: Mapping[str, str]
    ambiguous: Mapping[str, tuple[str, ...]]

    @staticmethod
    def from_json(o: JsonObj) -> AliasTable:
        if o.get("schema") != ALIAS_SCHEMA:
            raise c.ValidationError(f"alias table schema is {o.get('schema')!r}, need {ALIAS_SCHEMA}")
        raw_aliases, raw_ambiguous = o.get("aliases") or {}, o.get("ambiguous") or {}
        if not isinstance(raw_aliases, dict) or not isinstance(raw_ambiguous, dict):
            raise c.ValidationError("alias table aliases/ambiguous must be objects")
        aliases = {str(k): str(v) for k, v in raw_aliases.items() if isinstance(v, str)}
        ambiguous = {str(k): tuple(str(x) for x in v) for k, v in raw_ambiguous.items() if isinstance(v, list)}
        overlap = sorted(set(aliases) & set(ambiguous))
        if overlap:
            raise c.ValidationError(f"alias table lists {overlap} as both unique and ambiguous")
        return AliasTable(aliases, ambiguous)

    @staticmethod
    def load(path: Path = ALIAS_TABLE) -> AliasTable:
        data = _read_json(path)
        if data is None:
            raise c.ValidationError(f"cannot read alias table {path}")
        return AliasTable.from_json(data)

    @staticmethod
    def empty() -> AliasTable:
        return AliasTable({}, {})


def resolve_key(question_key: str, log_keys: Iterable[str], table: AliasTable) -> tuple[list[str], str]:
    """Advisor log key(s) that answer ``question_key``: exact first, then the explicit table. Never a
    suffix match. Returns (keys, how) with how in exact | alias | ambiguous | none."""
    known = set(log_keys)
    if question_key in known:
        return [question_key], "exact"
    if question_key in table.ambiguous:
        return list(table.ambiguous[question_key]), "ambiguous"
    target = table.aliases.get(question_key)
    if target is not None:
        return [target], "alias"
    return [], "none"


ASK_SLACK_SECONDS = 30 * 60  # an advisor started up to 30 min before the question was registered still counts


def question_ops(world: c.World, records: Sequence[AdvisorRecord], table: AliasTable, at: str, *,
                 drift_owners: Mapping[str, str] | None = None) -> Resolution:
    """question.answered for every open advisor question whose answer is in the log.

    A log record's ``at`` is when the advisor was *started* (host-dag-advisor.py writes the line after
    the answer with the start time), so binding is by log order and identity, not by answer time:
    - records already bound to any recorded answer are consumed and never bind again;
    - a revision > 1 question only takes records after the line that answered its previous
      revision (review "旧回答不解除新问题");
    - the advisor run must have started no earlier than 30 minutes before the question was asked
      (the legacy flow starts the advisor first and registers the wait after);
    - when the question names its question_file, only a record of that same file binds; a time-
      eligible record for another file is reported (question_file_mismatch), not bound;
    - when two open questions (on any tickets) would bind the same record, neither binds: each gets an
      ambiguous_answer note (B2 review M2) until question_file or a re-ask separates them.
    A page-window drift question is keyed by the core entry id while the library asks the advisor
    under the window owner; ``drift_owners`` maps entry -> owner, and any run started after the entry
    was queued counts (the library asks before the core sees the release)."""
    out = Resolution()
    owners = drift_owners or {}
    by_key: dict[str, list[AdvisorRecord]] = {}
    line_of: dict[str, int] = {}
    for rec in records:
        by_key.setdefault(rec.key, []).append(rec)
        line_of[rec.source_record] = rec.line_no
    consumed = {str(q.answer.get("source_record")) for t in world.tickets.values() for q in t.questions
                if q.answer is not None}
    proposals: list[tuple[c.Ticket, c.Question, AdvisorRecord, str]] = []
    for t in _sorted_tickets(world):
        open_refs = {w.ref for w in t.waits if w.kind == "question" and w.open}
        for q in t.questions:
            if q.answer is not None or q.question_id not in open_refs:
                continue
            floor = _epoch(q.asked_at) - ASK_SLACK_SECONDS
            if q.key.startswith(DRIFT_PREFIX) and q.key[len(DRIFT_PREFIX):] in owners:
                entry = q.key[len(DRIFT_PREFIX):]
                keys, how = [DRIFT_PREFIX + owners[entry]], "drift-owner"
                queued = [w.added_at for w in t.waits if w.kind == "page-window" and w.ref == entry]
                if queued:
                    floor = min(floor, _epoch(queued[0]))
            elif q.key.startswith(DRIFT_PREFIX) and any(
                    w.kind == "page-window" and w.ref == q.key[len(DRIFT_PREFIX):] for w in t.waits):
                out.note("question", "drift_owner_unknown", t.ticket, q.question_id,
                         f"{q.key}: the entry's spec is unreadable, so the library's advisor key is unknown")
                continue
            else:
                keys, how = resolve_key(q.key, by_key, table)
            if how == "none":
                continue
            if how == "ambiguous":
                out.note("question", "ambiguous_answer", t.ticket, q.question_id,
                         f"key {q.key} maps to {keys}; not resolved automatically")
                continue
            after_line = 0
            for prev in t.questions:
                if prev.key == q.key and prev.revision < q.revision and prev.answer is not None:
                    after_line = max(after_line, line_of.get(str(prev.answer.get("source_record")), 0))
                    if str(prev.answer.get("source_record")) not in line_of:  # answered outside the log
                        floor = max(floor, _epoch(str(prev.answer.get("at"))))
            # B7 may know this is a re-ask but lack the earlier revisions' consumed source rows.
            # Persist the conservative floor so controller catch-up AND the direct B6 writer obey it.
            limits = t.imported.get("question_history_limits")
            limit = limits.get(q.question_id) if isinstance(limits, dict) else None
            if isinstance(limit, str):
                floor = max(floor, _epoch(limit))
            candidates = [r for r in by_key.get(keys[0], []) if r.source_record not in consumed
                          and r.line_no > after_line and r.at >= floor]
            if q.to == "user":
                if candidates:
                    out.note("question", "advisor_answer_for_user_question", t.ticket, q.question_id,
                             f"{keys[0]} has an advisor record after the hand-off; the user answers with decision.respond")
                continue
            if not candidates:
                if any(r.source_record not in consumed for r in by_key.get(keys[0], [])):
                    out.note("question", "answer_predates_question", t.ticket, q.question_id,
                             f"{keys[0]} has only records from before {q.question_id} (revision {q.revision}) was asked")
                continue
            if q.question_file:
                same = [r for r in candidates if r.question_file == q.question_file]
                if not same:
                    out.note("question", "question_file_mismatch", t.ticket, q.question_id,
                             f"{keys[0]} line {candidates[0].line_no} answers {candidates[0].question_file}, "
                             f"question is {q.question_file}")
                    continue
                candidates = same
            proposals.append((t, q, candidates[0], how))
    claims: dict[str, list[str]] = {}
    for t, q, rec, _ in proposals:
        claims.setdefault(rec.source_record, []).append(f"{t.ticket}/{q.question_id}")
    for t, q, rec, how in proposals:
        rivals = claims[rec.source_record]
        if len(rivals) > 1:
            out.note("question", "ambiguous_answer", t.ticket, q.question_id,
                     f"{rec.key} line {rec.line_no} would answer {', '.join(rivals)}; none bound")
            continue
        payload: JsonObj = {"question_id": q.question_id, "revision": q.revision, "verdict": rec.verdict,
                            "text": rec.decision, "source_record": rec.source_record, "log_key": rec.key,
                            "advisor_started_at": iso_utc(rec.at), "matched_by": how,
                            "log_offset": rec.offset}
        out.ops.append(fact_op("question.answered", FACT_ACTOR["question"], t.ticket, payload, at))
    return out


# ---------------------------------------------------------------- ticket waits (report only)


LEGACY_CLOSURE = {"ACCEPTED": "accepted", "INTEGRATED": "accepted", "DONE": "accepted", "CLOSED": "accepted",
                  "SUPERSEDED": "superseded", "CANCELED": "canceled", "CANCELLED": "canceled", "FAILED": "failed"}


def legacy_closures(run: JsonObj) -> dict[str, str]:
    """ticket -> closure kind as run.json (the live truth until stage C) records it. ``status`` is read
    before ``state`` so SUPERSEDED stays distinct (stage A folds it into CANCELED; neither is done)."""
    out: dict[str, str] = {}
    batches = run.get("batches")
    if not isinstance(batches, dict):
        return out
    for body in batches.values():
        tickets = body.get("tickets") if isinstance(body, dict) else None
        if not isinstance(tickets, dict):
            continue
        for name, rec in tickets.items():
            if not isinstance(rec, dict):
                continue
            for field_name in ("status", "state"):
                kind = LEGACY_CLOSURE.get(str(rec.get(field_name) or "").strip().upper())
                if kind is not None:
                    out[str(name)] = kind
                    break
    return out


def ticket_findings(world: c.World, legacy_run: JsonObj | None = None) -> Resolution:
    """Ticket waits and hard deps against their targets. Emits no ops (see module doc): a wait whose
    target is accepted is ``satisfiable`` (the next accepted transaction's settle closes it), one whose
    target was superseded/canceled/failed is ``unresolvable`` (settle creates unresolvable_wait), and a
    target whose run.json closure differs from the core record is ``legacy_drift`` (shadow diff, B3)."""
    out = Resolution()
    legacy = legacy_closures(legacy_run) if legacy_run is not None else None
    for t in _sorted_tickets(world):
        if t.phase == c.CLOSED:
            continue
        targets = [(w.ref, f"wait {w.wait_id}") for w in t.waits if w.kind == "ticket" and w.open]
        targets += [(d, "dep") for d in t.deps]
        for ref, what in targets:
            target = world.tickets.get(ref)
            core_kind = target.closure.kind if target is not None and target.closure is not None else None
            if what != "dep" and core_kind == "accepted":
                out.note("ticket", "satisfiable", t.ticket, ref, f"{what}: {ref} is accepted; settle closes it")
            elif core_kind is not None and core_kind != "accepted":
                out.note("ticket", "unresolvable", t.ticket, ref, f"{what}: {ref} closed {core_kind}")
            if legacy is not None:
                was = legacy.get(ref)
                if was != core_kind and (was is not None or core_kind is not None):
                    out.note("ticket", "legacy_drift", t.ticket, ref,
                             f"{what}: run.json closure {was or 'open'}, core {core_kind or 'open'}")
    return out


# ---------------------------------------------------------------- page window (library files, read only)


@dataclass(frozen=True)
class SpecView:
    spec: str
    owner: str | None
    result: JsonObj | None
    current_run: JsonObj | None
    error: str | None = None
    ticket: str | None = None


@dataclass(frozen=True)
class PageFiles:
    registry: JsonObj
    specs: Mapping[str, SpecView]
    claims: tuple[JsonObj, ...] = ()


@dataclass(frozen=True)
class PageEntry:
    ticket: str
    wait: c.Wait
    run: c.Pending | None
    spec: str | None


def page_entries(world: c.World) -> list[PageEntry]:
    """Every page-window entry the library may still report on: queued, acquired, or ended
    RECOVERY_REQUIRED with its recovery decision still open (only a ``page-window.py recover``
    re-close can still arrive for it). Any other ended entry is finished and no longer scanned."""
    out: list[PageEntry] = []
    for t in _sorted_tickets(world):
        for w in t.waits:
            if w.kind != "page-window" or (w.waived is not None and w.satisfied_at is None):
                continue
            recorded = w.detail.get("outcome")
            if recorded is not None and not (recorded == "RECOVERY_REQUIRED" and _recovery_open(t, w.ref)):
                continue
            runs = [p for p in t.pending if p.kind == "run_window" and p.bound.get("entry_id") == w.ref]
            spec = _str(w.detail.get("spec")) or (w.note or None)
            out.append(PageEntry(t.ticket, w, runs[-1] if runs else None, spec))
    return out


def read_spec_view(spec_path: str) -> SpecView:
    path = Path(spec_path)
    spec = _read_json(path)
    if spec is None:
        return SpecView(spec_path, None, None, None, "spec.json unreadable")
    evidence = _str(spec.get("evidence"))
    root = Path(evidence) if evidence and Path(evidence).is_absolute() else path.parent / (evidence or "")
    current = _read_json(root / "current-run.json")
    result = _read_json(root / "result.json")
    run_dir = _str(current.get("dir")) if current is not None else None
    if result is None and run_dir:
        result = _read_json(Path(run_dir) / "result.json")
    return SpecView(spec_path, _str(spec.get("owner")), result, current, ticket=_str(spec.get("ticket")))


def load_page_files(run_dir: Path, world: c.World) -> PageFiles:
    registry = _read_json(Path(run_dir) / REGISTRY_REL) or {}
    specs = {e.spec: read_spec_view(e.spec) for e in page_entries(world) if e.spec}
    for t in _sorted_tickets(world):
        history = t.imported.get("page_window")
        spec = _str(history.get("spec")) if isinstance(history, dict) else None
        if t.phase == c.CLOSED and spec and spec not in specs:
            specs[spec] = read_spec_view(spec)
    try:
        claims = parse_page_claims((Path(run_dir) / CLAIMS_REL).read_text(encoding="utf-8"))
    except (OSError, UnicodeError):
        claims = ()
    return PageFiles(registry, specs, claims)


def parse_page_claims(text: str) -> tuple[JsonObj, ...]:
    rows: list[JsonObj] = []
    for line in text.splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue  # Partial appends cannot authenticate a result; retry next cycle.
        if isinstance(row, dict):
            rows.append(row)
    return tuple(rows)


def matching_page_claims(ticket: str, spec: str, owner: str,
                         claims: Sequence[JsonObj], since: float) -> list[JsonObj]:
    """Candidates come from the append-only ledger, never from a mutable run pointer."""
    matches = []
    for row in claims:
        spec_record = row.get("spec")
        claim_at = _num(row.get("at"))
        if (row.get("ticket") == ticket and row.get("owner") == owner
                and isinstance(spec_record, dict)
                and spec_record.get("path") == spec
                and claim_at is not None and (claim_at >= since or f"{claim_at:.6f}" == f"{since:.6f}")):
            matches.append(row)
    return matches


def page_claim_binding(ticket: str, spec: str, owner: str, claims: Sequence[JsonObj],
                       since: float, *, lease_at: float | None = None, claim_id: str | None = None,
                       read_open: Callable[[str], JsonObj | None] | None = None
                       ) -> tuple[JsonObj, int]:
    # Count *all* matching ledger rows before checking publication completeness. Otherwise an
    # unfinished first open disappears and a later, complete retry impersonates the first run.
    matches = matching_page_claims(ticket, spec, owner, claims, since)
    # registryAt is the registry JSON value copied by this exact claim. Ledger append time
    # cannot associate a receipt with a lease, even when a retry follows immediately.
    candidates = [row for row in matches
                  if lease_at is not None and _num(row.get("registryAt")) is not None
                  and f"{_num(row.get('registryAt')):.6f}" == f"{lease_at:.6f}"
                  and (claim_id is None or row.get("claimId") == claim_id)]
    if len(candidates) != 1:
        return {}, len(matches)
    for row in candidates:
        if not _str(row.get("claimId")) or not _str(row.get("runDir")):
            return {}, len(matches)
        path = str(Path(str(row["runDir"])) / "open.json")
        opened = (read_open(path) if read_open else _read_json(Path(path))) or {}
        files = opened.get("files")
        receipt = row["spec"]
        assert isinstance(receipt, dict)
        digest = _digest(receipt.get("sha256"))
        if (opened.get("claimId") == row["claimId"] and isinstance(files, dict)
                and digest is not None and re.fullmatch(r"[0-9a-f]{64}", digest)
                and digest == _digest(files.get("spec"))):
            return {"claim_id": row["claimId"], "run_dir": row["runDir"]}, len(matches)
    return {}, len(matches)


def _page_review(out: Resolution, world: c.World, ticket: str, entry: str,
                 reason: str, detail: str, at: str, *, ask_after: str | None = None) -> None:
    out.note("page-window", reason, ticket, entry, detail)
    deferred = ask_after is not None and _epoch(at) < _epoch(ask_after)
    if deferred and any(w.ref == entry and w.detail.get("identity_review_required")
                        for w in world.tickets[ticket].waits):
        return
    key = c.page_review_key(world.tickets[ticket], entry, reason, at)
    if key is not None:
        out.ops.append(fact_op("resource.observed", "pagewindow", ticket, {
            "resource": "page-window", "event": "needs_review", "entry_id": entry,
            "reason": reason, "title": detail, "review_key": key,
            **({"ask_after": ask_after} if ask_after is not None else {}),
            "source_record": key + (":deferred" if deferred else "")}, at))


def closed_page_window_findings(world: c.World, files: PageFiles, at: str) -> Resolution:
    """Historical imports stay outside the operational queue, but never outside observation."""
    out = Resolution()
    for t in _sorted_tickets(world):
        history = t.imported.get("page_window")
        if t.phase != c.CLOSED or not isinstance(history, dict):
            continue
        record = history.get("record")
        spec, entry = _str(history.get("spec")), _str(history.get("entry_id"))
        if not isinstance(record, dict) or not spec or not entry:
            continue
        owner = _str(record.get("owner"))
        queued = _str(history.get("queued_at")) or _str(record.get("approved_at"))
        if not owner or not queued:
            continue
        since = _epoch(queued)
        claims = matching_page_claims(t.ticket, spec, owner, files.claims, since)
        view = files.specs.get(spec)
        result = view.result if view else None
        result_at = _num(result.get("at")) if result else None
        # A partial/missing open receipt must not hide activity; this path authenticates no identity.
        active_result = (result is not None and result.get("owner") == owner
                         and result.get("ticket") == t.ticket and result_at is not None
                         and result_at >= since)
        if claims or active_result:
            _page_review(out, world, t.ticket, entry, "closed_ticket_window_activity",
                         f"Closed ticket {t.ticket}, historical window {entry}: matching claim/result "
                         "after approval; advisor must determine the running window's disposition", at)
    return out


def _bound_page_result(entry: PageEntry, view: SpecView, files: PageFiles, binding: JsonObj | None = None) -> str | None:
    """A result needs one library claim, its open claimId and the exact ticket/owner/run/spec.

    No guesses from result timestamps or current-run when runDir is absent. Old libraries without
    this receipt remain observable as withheld; they cannot close an entry.
    """
    result = view.result or {}
    run_dir = _str(result.get("runDir"))
    if not run_dir:
        return "result has no runDir"
    opened = _read_json(Path(run_dir) / "open.json") or {}
    claim_id = _str(opened.get("claimId"))
    matches = [row for row in files.claims if claim_id and row.get("claimId") == claim_id]
    if len(matches) != 1:
        return "missing or duplicate library claimId"
    row = matches[0]
    claim_at = _num(row.get("at"))
    floor = _claim_stamp(entry.wait, view.owner or "")
    if floor is None:
        floor = _epoch(entry.run.created_at) if entry.run else 0.0
    if claim_at is None or (claim_at < floor and f"{claim_at:.6f}" != f"{floor:.6f}"):
        return "library claim predates this entry's approval/acquire"
    if result.get("claimId") not in (None, claim_id):
        return "result names a different claimId"
    spec = row.get("spec")
    open_files = opened.get("files")
    if (view.ticket != entry.ticket or result.get("ticket") != entry.ticket
            or result.get("owner") != view.owner or row.get("ticket") != entry.ticket
            or row.get("owner") != view.owner or row.get("runDir") != run_dir
            or not isinstance(spec, dict) or spec.get("path") != entry.spec
            or re.fullmatch(r"[0-9a-f]{64}", _digest(spec.get("sha256")) or "") is None
            or not isinstance(open_files, dict)
            or _digest(spec.get("sha256")) != _digest(open_files.get("spec"))):
        return "claim ticket/owner/runDir/spec differs from entry, result or open.json"
    identity = {**entry.wait.detail, **(binding or {})}
    own_run = _str(identity.get("run_dir"))
    own_claim = _str(identity.get("claim_id"))
    if not own_run or not own_claim:
        return "entry has no unique bound library claim identity"
    if (own_run and own_run != run_dir) or (own_claim and own_claim != claim_id):
        return "result differs from the entry's recorded claim"
    return None


def drift_owners(world: c.World, files: PageFiles) -> dict[str, str]:
    """entry_id -> library owner, for every page-window wait whose spec names one (including released
    entries, whose drift question is still open)."""
    out: dict[str, str] = {}
    for t in _sorted_tickets(world):
        for w in t.waits:
            if w.kind != "page-window":
                continue
            spec = _str(w.detail.get("spec")) or (w.note or None)
            view = files.specs.get(spec) if spec else None
            if view is None and spec:
                view = read_spec_view(spec)
            if view is not None and view.owner:
                out[w.ref] = view.owner
    return out


def capacity_evidence(run_dir: str | None) -> JsonObj | None:
    """The shortage a WAIT_RESOURCE window recorded: the latest ``memory-<stage>-*.json`` with ok=false
    in the window's run evidence dir (written by pagewindow.capacity.require_headroom / gate; the
    library is not changed). Returns the stage, its candidate nodes and per-node stage requests +
    margin, or None when no such file exists (B2 review M1)."""
    if not run_dir:
        return None
    best: tuple[float, str, JsonObj] | None = None
    try:
        files = sorted(Path(run_dir).glob("memory-*.json"))
    except OSError:
        return None
    for path in files:
        data = _read_json(path)
        if data is None or data.get("ok") is not False:
            continue
        at = _num(data.get("at")) or 0.0
        if best is None or (at, path.name) > (best[0], best[1]):
            best = (at, path.name, data)
    if best is None:
        return None
    _, name, data = best
    rows = data.get("nodes")
    candidates = data.get("candidates")
    if not isinstance(rows, list) or not isinstance(candidates, list):
        return None
    need: JsonObj = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("node") not in candidates:
            continue
        stage, margin = _num(row.get("stageRequests")), _num(row.get("margin"))
        if stage is None or margin is None:
            return None
        need[str(row["node"])] = stage + margin
    return {"stage": data.get("stage"), "candidates": [str(x) for x in candidates], "need_by_node": need,
            "evidence": name, "evidence_at": data.get("at")}


def _busy_gate(busy: Json, run_dir: str | None) -> JsonObj | None:
    """The first capacity shortage in a WAIT_RESOURCE result, as a core gate. Only capacity: this
    module resolves capacity waits; a lock/slot/decision busy reason stays a plain back-off (a gate
    wait nothing resolves would park the entry for good). The gate carries the window's own capacity
    evidence when it can be found; without it the wait is kept and never released by a bare sample."""
    if not isinstance(busy, list):
        return None
    for item in busy:
        if not isinstance(item, dict):
            continue
        resource = _str(item.get("resource")) or ""
        pool = resource[len("capacity:"):] if resource.startswith("capacity:") else (
            NODE_POOL if resource == NODE_POOL else None)
        if pool and POOL_NAME.fullmatch(pool):
            gate: JsonObj = {"kind": "capacity", "ref": pool, "note": str(item.get("reason") or "")[:300]}
            if _str(item.get("holder")):
                gate["holder"] = item.get("holder")
            evidence = capacity_evidence(run_dir)
            if evidence is not None:
                gate.update(evidence)
            return gate
    return None


def _digest(value: Json) -> str | None:
    text = _str(value)
    return text.lower().removeprefix("sha256:") if text else None


def page_window_ops(world: c.World, files: PageFiles, at: str) -> Resolution:
    """Library facts -> resource.observed. Terminal results require a matching library claim receipt.
    Correlation also checks owner and time: a registry claim or a
    result.json counts for an entry only if it is not older than the entry's queued run_window. An
    entry is decided by whether it is resolved (an outcome recorded), never by whether its run_window
    or wait is still open: a failure after an observed acquire still ends the entry (B2 review H1).

    result.json status/outcome mapping (library: scripts/local-dev/pagewindow/window.py):
    - not opened, WAIT_RESOURCE, run_window still queued -> claim_failed (same action, next check in
      5 minutes; a capacity shortage also becomes a capacity gate);
    - not opened, WAIT_RESOURCE after an acquire (an open check found a busy resource and gave the
      page back) -> launch_failed with outcome WAIT_RESOURCE (the core will not re-queue a closed
      run_window; the decision offers pagewindow.enqueue);
    - not opened, PREFLIGHT_FAILED / PROTECTED_DRIFT_AT_OPEN -> check_failed;
    - not opened, anything else (OPEN_FAILED, INTERRUPTED_BEFORE_OPEN, ...) -> launch_failed;
    - opened, LOST_CLAIM -> lost;
    - opened, any other outcome -> released. CLEAN counts as CLEAN only with status STEPS_DONE and a
      spec unchanged since approval (open.json ``files.spec`` = run_window ``spec_digest``, B2 review
      M4; a missing digest on either side is ``CLEAN:SPEC_UNVERIFIED``); otherwise it is
      ``CLEAN:<status>`` / ``CLEAN:SPEC_CHANGED`` and the core asks for a
      window_result decision instead of continuing.
    Attribution (B2 re-review H1/H2): a result counts for an unresolved entry only if its run claimed
    or started after the entry was queued, or after the entry's recorded claim once acquired (run
    start from open.json ``at`` / runs/<stamp>, not a mutable pointer). recoveredFrom
    is accepted for an unresolved entry only with its bound claim/run identity. A RECOVERY_REQUIRED entry is scanned only
    while its recovery decision is open, and a result newer than its recorded close from the same
    run that says recoveredFrom RECOVERY_REQUIRED is a ``recover`` re-close (sent as a release the
    core accepts); anything else is noted (L2)."""
    out = closed_page_window_findings(world, files, at)
    registry = files.registry
    actor = FACT_ACTOR["page-window"]
    now = _epoch(at)
    for entry in page_entries(world):
        w, run, name = entry.wait, entry.run, entry.ticket
        recorded = _str(w.detail.get("outcome"))
        if run is None:
            if recorded is None:
                out.note("page-window", "no_run_window", name, w.ref, "page-window wait without a run_window action")
            continue
        if entry.spec is None:
            out.note("page-window", "no_spec", name, w.ref, "page-window wait names no spec")
            continue
        view = files.specs.get(entry.spec) or read_spec_view(entry.spec)
        if view.error or not view.owner:
            out.note("page-window", "spec_unreadable", name, w.ref, f"{entry.spec}: {view.error or 'no owner'}")
            continue
        owner = view.owner
        queued = _epoch(run.created_at)
        base: JsonObj = {"resource": "page-window", "entry_id": w.ref, "holder": owner, "spec": entry.spec}
        result = view.result
        result_at = _num(result.get("at")) if result is not None else None
        recovered = result is not None and result.get("recoveredFrom") == "RECOVERY_REQUIRED"
        reg_at = _num(registry.get("at"))
        claimed = registry.get("owner") == owner and reg_at is not None and reg_at >= queued
        claim_at = _claim_stamp(w, owner)
        lease_at = reg_at if w.open and claimed else claim_at
        binding_since = lease_at if lease_at is not None else queued
        request = w.detail.get("identity_review_request")
        request = request if isinstance(request, dict) else {}
        selected = _str(w.detail.get("identity_review_claim")) or _str(request.get("claim_id"))
        # An explicit disposition may select a receipt before any acquire was observed.
        # Its registryAt supplies the lease without requiring current registry ownership.
        if lease_at is None and selected and request:
            selected_rows = [row for row in matching_page_claims(
                name, entry.spec, owner, files.claims, binding_since) if row.get("claimId") == selected]
            if len(selected_rows) == 1:
                lease_at = _num(selected_rows[0].get("registryAt"))
        missed_claims = (matching_page_claims(name, entry.spec, owner, files.claims, binding_since)
                         if lease_at is None else [])
        if len(missed_claims) == 1:
            receipt_lease = _num(missed_claims[0].get("registryAt"))
            if receipt_lease is not None and (receipt_lease >= binding_since
                    or f"{receipt_lease:.6f}" == f"{binding_since:.6f}"):
                lease_at = receipt_lease
        identity, count = page_claim_binding(name, entry.spec, owner, files.claims, binding_since,
                                             lease_at=lease_at, claim_id=selected)
        # A window may finish entirely between observations. Only one complete ledger/open/result
        # chain can reconstruct its missed acquire; every other observed claim must block a relaunch.
        if missed_claims and (not identity or result is None
                or result.get("claimId") != identity.get("claim_id")
                or _bound_page_result(entry, view, files, identity) is not None):
            identity, lease_at = {}, None
        # Keep the old identity immutable, even when a requested selection names another run.
        if any(w.detail.get(k) is not None and w.detail[k] != v for k, v in identity.items()):
            identity = {}
        resolving = bool(identity and request and selected == identity.get("claim_id"))
        disposed = bool(w.detail.get("identity_review_claim"))
        missed_unbound = bool(missed_claims and not identity)
        review_reason = (_str(w.detail.get("identity_review_reason"))
                         or ("claim_receipt_stalled" if missed_unbound and count == 1 else "ambiguous_claims"))
        ask_after = _str(w.detail.get("identity_review_after"))
        if missed_unbound and ask_after is None:
            ask_after = iso_utc(min(float(row["at"]) for row in missed_claims) + CLAIM_RECEIPT_STALL_SECONDS)
        if not resolving and (w.detail.get("identity_review_required") or count > 1 and not disposed
                              or missed_unbound):
            _page_review(out, world, name, w.ref, review_reason,
                         f"Window {w.ref}: claim identity still requires disposition ({count} matching ledger rows); "
                         'use {"action":"bind_claim","claim_id":"X"} or {"action":"abandon_window"}',
                         at, ask_after=ask_after)
        acquire_update = (not w.open and recorded is None and
                          (resolving or any(w.detail.get(k) != v for k, v in identity.items())))
        acquire_fact = None
        observe_acquire = w.open and (claimed or bool(missed_claims and identity))
        if (observe_acquire or acquire_update or resolving) and lease_at is not None:
            stamp = f"{lease_at:.6f}"
            review = {"review_question_id": request["question_id"]} if resolving else {}
            suffix = ":identity:" + c.canonical({**identity, **review}) if not w.open or resolving else ""
            acquire_fact = fact_op("resource.observed", actor, name, {
                **base, **identity, **review, "event": "acquired", "lease": f"claim@{stamp}",
                "source_record": f"pagewindow:{owner}:claim:{stamp}" + suffix}, at)
        if resolving and acquire_fact is not None:
            out.ops.append(acquire_fact)
            continue  # let the core authenticate the disposition before applying any terminal result
        if (count > 1 and not disposed) or w.detail.get("identity_review_required") or missed_unbound:
            if w.open and claimed and acquire_fact is not None:
                out.ops.append(acquire_fact)  # retain the observed original lease without guessing identity
            if result is not None:
                out.note("page-window", "result_unbound", name, w.ref, "claim identity awaiting explicit disposition")
            continue
        if (recorded is None and claim_at is not None and not w.detail.get("claim_id") and not identity
                and now - claim_at >= CLAIM_RECEIPT_STALL_SECONDS):
            _page_review(out, world, name, w.ref, "claim_receipt_stalled",
                         f"Window {w.ref}: acquired more than {CLAIM_RECEIPT_STALL_SECONDS} seconds ago "
                         "without a bindable claim receipt; explicit identity disposition required", at)
            continue
        if observe_acquire and acquire_fact is not None:
            out.ops.append(acquire_fact)
            continue  # record acquire before a terminal fact
        stale_at: float | None = None  # set when the spec's result is from an earlier run
        if result is not None and result_at is not None and recorded is None:
            # Attribute a result to this entry only if its run started after the entry was queued, or
            # after this entry's claim once an acquire is recorded (B2 re-review H1/H2): a stale
            # pre-claim WAIT_RESOURCE, or a recover re-close of an older entry's run, is not ours.
            claim_at = _claim_stamp(w, owner)
            floor = claim_at if claim_at is not None else queued
            started = _run_started(result, view)
            if recovered and _bound_page_result(entry, view, files, identity) is not None:
                out.note("page-window", "recovered_result_not_ours", name, w.ref,
                         f"recover re-close at {iso_utc(result_at)} belongs to an earlier entry's run")
                result_at = None
            elif (started if started is not None else result_at) < floor - RUN_START_SLACK_SECONDS:
                stale_at, result_at = result_at, None  # an earlier run of the same spec
        if recorded is None and claimed and reg_at is not None and (result_at is None or reg_at > result_at):
            # The initial acquire and later identity completion share a lease, but have distinct
            # source records. Otherwise source-record dedup would discard the completion.
            if acquire_fact is not None:
                out.ops.append(acquire_fact)
            continue
        if stale_at is not None and run.open and now - queued > STALE_RESULT_SECONDS:
            # B2 follow-up: the spec's only result is from an earlier run and nothing newer appeared for
            # a long time (the window was never launched, or its result.json is not where the spec says).
            out.note("page-window", "stale_results_only", name, w.ref,
                     f"queued {run.created_at}; {owner}'s only result ({iso_utc(stale_at)}) "
                     "predates the entry and no claim or newer result appeared")
        if result is None or result_at is None:
            if acquire_update and acquire_fact is not None:
                out.ops.append(acquire_fact)
            continue
        # WAIT_RESOURCE before acquire only backs off; it cannot end a window. All terminal paths,
        # including recover/lost/check_failed/launch_failed, share the receipt check.
        backoff_only = (not result.get("opened") and result.get("outcome", result.get("status")) == "WAIT_RESOURCE"
                        and run.open and recorded is None)
        problem = None if backoff_only else _bound_page_result(entry, view, files, identity)
        if problem:
            out.note("page-window", "result_unbound", name, w.ref, problem)
            if acquire_update and acquire_fact is not None:
                out.ops.append(acquire_fact)
            continue
        stamp = f"{result_at:.6f}"
        status = str(result.get("status") or "")
        outcome = str(result.get("outcome") or status)
        source = f"pagewindow:{owner}:result:{stamp}"
        evidence = _str(result.get("runDir"))
        # Even a first-seen terminal result uses only the unique ledger identity.
        bound_identity = {**w.detail, **identity} if not backoff_only else {}
        info: JsonObj = {**base, "source_record": source, "result_status": status,
                         "result_at": iso_utc(result_at)}
        info.update({k: bound_identity[k] for k in ("claim_id", "run_dir") if k in bound_identity})
        opened = bool(result.get("opened"))
        if recorded is not None:
            # Only RECOVERY_REQUIRED entries with an open recovery decision get here (page_entries).
            if w.detail.get("release_source") == source:
                continue  # the result the recorded outcome came from
            released_at = _release_stamp(w)
            if released_at is not None and result_at <= released_at:
                continue  # not newer than the recorded close
            own_run = _str(w.detail.get("run_dir"))
            if own_run is not None and evidence is not None and own_run != evidence:
                out.note("page-window", "result_after_outcome", name, w.ref,
                         f"{outcome} from run {evidence}, not this entry's run {own_run}; not applied")
                continue
            if recovered and opened and outcome not in ("LOST_CLAIM", "RECOVERY_REQUIRED"):
                core_outcome = _clean_outcome(out, name, w.ref, run, evidence, outcome, status)
                out.ops.append(fact_op("resource.observed", actor, name, {
                    **info, "event": "released", "outcome": core_outcome, "recovered_from": "RECOVERY_REQUIRED"}, at))
            else:
                out.note("page-window", "result_after_outcome", name, w.ref,
                         f"{outcome} ({status}) at {iso_utc(result_at)} after the entry already ended {recorded}; "
                         "not applied")
            continue
        if not opened:
            if outcome == "WAIT_RESOURCE" and run.open:
                busy = result.get("busy")
                gate = _busy_gate(busy, evidence)
                if gate is None and now - queued > BUSY_STALL_SECONDS:
                    out.note("page-window", "busy_stalled", name, w.ref,
                             f"backing off on {_busy_reasons(busy)} since queued {run.created_at}")
                next_check = iso_utc(result_at + PAGE_RETRY_SECONDS)
                if run.payload.get("next_check_at") == next_check:
                    continue
                payload: JsonObj = {**info, "event": "claim_failed", "next_check_at": next_check,
                                    "busy": {"items": busy if isinstance(busy, list) else []}}
                if gate is not None:
                    payload["gate"] = gate
                out.ops.append(fact_op("resource.observed", actor, name, payload, at))
                continue
            event = "check_failed" if outcome in ("PREFLIGHT_FAILED", "PROTECTED_DRIFT_AT_OPEN") else "launch_failed"
            out.ops.append(fact_op("resource.observed", actor, name, {**info, "event": event, "outcome": outcome}, at))
            continue
        if outcome == "LOST_CLAIM":
            out.ops.append(fact_op("resource.observed", actor, name, {**info, "event": "lost", "outcome": outcome}, at))
            continue
        core_outcome = _clean_outcome(out, name, w.ref, run, evidence, outcome, status)
        out.ops.append(fact_op("resource.observed", actor, name,
                               {**info, "event": "released", "outcome": core_outcome}, at))
    return out


BUSY_STALL_SECONDS = 30 * 60  # B2 review L3: a non-capacity busy reason this old is reported
RUN_START_SLACK_SECONDS = 1.0  # runs/<stamp> names are truncated to the second
STALE_RESULT_SECONDS = 30 * 60  # B3: a queued entry whose spec shows only older results this long is reported


def _recovery_open(t: c.Ticket, entry: str) -> bool:
    return any(p.kind == "recovery" and p.bound.get("entry_id") == entry and p.status in c.OUTSTANDING
               for p in t.pending)


def _stamp_of(source: Json, prefix: str) -> float | None:
    text = _str(source)
    if text is None or not text.startswith(prefix):
        return None
    try:
        return float(text[len(prefix):])
    except ValueError:
        return None


def _claim_stamp(w: c.Wait, owner: str) -> float | None:
    """Registry claim time of this entry's recorded acquire (source pagewindow:<owner>:claim:<at>)."""
    satisfied = w.satisfied_by or {}
    value = satisfied.get("value")
    receipt = value if isinstance(value, dict) else satisfied
    lease = _stamp_of(receipt.get("lease"), "claim@")
    if lease is not None:
        return lease
    source = satisfied.get("source_record")
    return _stamp_of(source, f"pagewindow:{owner}:claim:")


def _release_stamp(w: c.Wait) -> float | None:
    """result.json time of the result the entry's recorded outcome came from."""
    source = _str(w.detail.get("release_source"))
    if source is None or ":result:" not in source:
        return None
    return _stamp_of(source, source.rsplit(":result:", 1)[0] + ":result:")


def _run_started(result: JsonObj, view: SpecView) -> float | None:
    """When the result's run started: open.json ``at`` in the run dir, else the
    runs/<YYYYmmdd-HHMMSS> dir name (local time, when the attempt began). None without a runDir."""
    run_dir = _str(result.get("runDir"))
    if run_dir is None:
        return None
    opened = _read_json(Path(run_dir) / "open.json")
    at = _num(opened.get("at")) if opened is not None else None
    if at is not None:
        return at
    try:
        return datetime.strptime(Path(run_dir).name, "%Y%m%d-%H%M%S").timestamp()
    except ValueError:
        return None


def _busy_reasons(busy: Json) -> str:
    if not isinstance(busy, list):
        return "?"
    return "; ".join(str(x.get("resource") or x.get("reason") or x) if isinstance(x, dict) else str(x)
                     for x in busy)[:300]


def _clean_outcome(out: Resolution, ticket: str, entry: str, run: c.Pending, run_dir: str | None,
                   outcome: str, status: str) -> str:
    """The outcome the core sees for an opened close. CLEAN continues automatically only after every
    step finished and with the spec the coordinator approved (open.json files.spec vs spec_digest)."""
    opened = _read_json(Path(run_dir) / "open.json") if run_dir else None
    files = opened.get("files") if opened is not None else None
    at_open = _digest(files.get("spec")) if isinstance(files, dict) else None
    approved = _digest(run.payload.get("spec_digest"))
    spec_state = "CLEAN"
    if at_open is None:
        spec_state = "CLEAN:SPEC_UNVERIFIED"
        out.note("page-window", "spec_digest_unrecorded", ticket, entry,
                 "open.json has no files.spec (library before cac0fb8df); spec continuity unverified")
    elif approved is None:
        spec_state = "CLEAN:SPEC_UNVERIFIED"
        out.note("page-window", "spec_digest_not_approved", ticket, entry,
                 "run_window has no spec_digest; spec continuity unverified")
    elif at_open != approved:
        spec_state = "CLEAN:SPEC_CHANGED"
        out.note("page-window", "spec_changed", ticket, entry,
                 f"window opened spec sha256 {at_open}, approved {approved}; continuation not fired")
    if outcome != "CLEAN":
        return outcome
    if status != "STEPS_DONE":
        return f"CLEAN:{status or '?'}"
    return spec_state  # only a verified, unchanged spec continues automatically (B2 re-review M4)


# ---------------------------------------------------------------- time


def time_ops(world: c.World, at: str) -> Resolution:
    """wait.satisfied for every open time wait due at ``at``. The source names the wait revision and
    due time, so a rescheduled wait (wait.update) is a new source. observed_at is the clock and is
    kept out of the op id (B2 review L1)."""
    out = Resolution()
    now = c.parse_at(at)
    for t in _sorted_tickets(world):
        for w in t.waits:
            if w.kind == "time" and w.open and c.parse_at(w.ref) <= now:
                out.ops.append(fact_op("wait.satisfied", FACT_ACTOR["time"], t.ticket, {
                    "wait_id": w.wait_id, "revision": w.revision, "observed_at": at,
                    "source_record": f"time:{w.wait_id}:r{w.revision}:{w.ref}"}, at, clock_fields=("observed_at",)))
    return out


# ---------------------------------------------------------------- capacity


def capacity_path(run_dir: Path, pool: str) -> Path:
    if not POOL_NAME.fullmatch(pool):
        raise c.ValidationError(f"capacity pool name {pool!r} is not a plain name")
    return Path(run_dir) / CAPACITY_REL / f"capacity-{pool}.json"


def _sample_ok(sample: JsonObj) -> bool:
    return (sample.get("schema") == SAMPLE_SCHEMA and _str(sample.get("pool")) is not None
            and _str(sample.get("sample_at")) is not None and _num(sample.get("value")) is not None
            and not sample.get("invalid"))


def load_capacity_samples(run_dir: Path, world: c.World, extra: Iterable[str] = ()) -> dict[str, JsonObj]:
    """pool -> latest sample, for the pools open capacity waits name plus node-memory (which every
    wait carrying the window's per-node evidence is checked against), plus ``extra`` pools (the host
    pools legacy board capacity waits are judged against)."""
    pools = {w.ref for t in world.tickets.values() for w in t.waits if w.kind == "capacity" and w.open}
    if pools:
        pools.add(NODE_POOL)
    pools.update(extra)
    out: dict[str, JsonObj] = {}
    for pool in sorted(pools):
        if not POOL_NAME.fullmatch(pool):
            continue
        sample = _read_json(capacity_path(run_dir, pool))
        if sample is not None and _sample_ok(sample) and sample.get("pool") == pool:
            out[pool] = sample
    return out


def _fresh(sample: JsonObj | None, now: float, since: float) -> str | None:
    """The sample time if the sample is fresh (<= 10 min) and taken after ``since``, else None."""
    sample_at = _str(sample.get("sample_at")) if sample is not None else None
    if sample_at is None:
        return None
    taken = _epoch(sample_at)
    return sample_at if now - taken <= CAPACITY_FRESH_SECONDS and taken >= since else None


def _candidates_fit(sample: JsonObj, detail: JsonObj) -> JsonObj | None:
    """Per-candidate free memory (allocatable - live requests) vs the window's stage requests +
    margin; the fit table when every candidate fits, else None (B2 review M1)."""
    need = detail.get("need_by_node")
    candidates = detail.get("candidates")
    rows = sample.get("nodes")
    if not isinstance(need, dict) or not isinstance(candidates, list) or not candidates or not isinstance(rows, list):
        return None
    by_node = {str(r.get("node")): r for r in rows if isinstance(r, dict)}
    fit: JsonObj = {}
    for node in candidates:
        row, want = by_node.get(str(node)), _num(need.get(str(node)))
        if row is None or want is None or not row.get("schedulable"):
            return None
        alloc, live = _num(row.get("allocatable")), _num(row.get("live_requests"))
        if alloc is None or live is None or alloc - live < want:
            return None
        fit[str(node)] = {"free": alloc - live, "need": want}
    return fit


def capacity_ops(world: c.World, samples: Mapping[str, JsonObj], at: str) -> Resolution:
    """wait.satisfied for open capacity waits a fresh probe sample shows satisfiable. Non-monotonic
    (§4.2): release only means "enough when sampled"; the library gates again at claim.

    A wait releases only on a stated need (B2 review M1): either the window's own capacity evidence
    (candidates + per-node stage requests + margin, from memory-<stage>-*.json) with every candidate's
    free memory in the node-memory sample covering it, or a numeric ``need`` met by the pool's sample
    value. A wait without either is kept and noted (capacity_need_unknown); a bare headroom >= 0 never
    releases. The sample must be fresh (<= 10 min) and newer than the wait, else capacity_no_fresh_sample
    (L3). A pool releases a ticket's wait at most once per 10 minutes (flapping back-off)."""
    out = Resolution()
    now = _epoch(at)
    pools = {NODE_POOL if isinstance(w.detail.get("need_by_node"), dict) else w.ref
             for t in world.tickets.values() for w in t.waits if w.kind == "capacity" and w.open}
    out.extend(sample_freshness(samples, pools, now))
    for t in _sorted_tickets(world):
        for w in t.waits:
            if w.kind != "capacity" or not w.open:
                continue
            pool, since = w.ref, _epoch(w.added_at)
            need = _num(w.detail.get("need"))
            if isinstance(w.detail.get("need_by_node"), dict):
                sample = samples.get(NODE_POOL)
                sample_at = _fresh(sample, now, since)
                if sample is None or sample_at is None:
                    out.note("capacity", "capacity_no_fresh_sample", t.ticket, w.wait_id,
                             f"{pool}: no node-memory sample newer than the wait within 10 minutes")
                    continue
                fit = _candidates_fit(sample, w.detail)
                if fit is None:
                    continue
                value_obj: JsonObj = {"pool": pool, "sample_at": sample_at, "candidates": fit,
                                      "stage": w.detail.get("stage"), "evidence": w.detail.get("evidence")}
            elif need is not None:
                sample = samples.get(pool)
                sample_at = _fresh(sample, now, since)
                if sample is None or sample_at is None:
                    out.note("capacity", "capacity_no_fresh_sample", t.ticket, w.wait_id,
                             f"{pool}: no sample newer than the wait within 10 minutes")
                    continue
                value = _num(sample.get("value"))
                if not sample.get("ok") or value is None or value < need:
                    continue
                value_obj = {"pool": pool, "sample_at": sample_at, "value": value, "need": need}
            else:
                out.note("capacity", "capacity_need_unknown", t.ticket, w.wait_id,
                         f"{pool}: the gate left no capacity evidence (memory-<stage>-*.json) or need; "
                         "kept until the coordinator waives it or the window is re-run")
                continue
            recent = [x for x in t.waits if x is not w and x.kind == "capacity" and x.ref == pool
                      and x.satisfied_at is not None and now - _epoch(x.satisfied_at) < CAPACITY_BACKOFF_SECONDS]
            if recent:
                out.note("capacity", "backoff", t.ticket, w.wait_id,
                         f"{pool} released {recent[-1].wait_id} at {recent[-1].satisfied_at}; next release after 10 min")
                continue
            digest = hashlib.sha256(c.canonical(sample).encode()).hexdigest()[:12]
            out.ops.append(fact_op("wait.satisfied", FACT_ACTOR["capacity"], t.ticket, {
                "wait_id": w.wait_id, "revision": w.revision, "observed_at": sample_at,
                "source_record": f"capacity:{pool}:{sample_at}:{digest}", "value": value_obj}, at))
    return out


def sample_freshness(samples: Mapping[str, JsonObj], pools: Iterable[str], now: float) -> Resolution:
    """HD-01 C: one note per pool a wait depends on whose sample is missing or older than
    CAPACITY_FRESH_SECONDS, so a stopped probe shows up instead of keeping waits open silently. The
    detail names the sample time (not its age), so the note is logged once per stale sample."""
    out = Resolution()
    for pool in sorted(set(pools)):
        sample = samples.get(pool)
        sample_at = _str(sample.get("sample_at")) if sample is not None else None
        if sample_at is None:
            out.note("capacity", "capacity_sample_missing", None, pool,
                     f"{pool}: no probe sample (control/capacity-{pool}.json); waits on it are not re-judged "
                     "until the capacity probe timer runs")
        elif now - _epoch(sample_at) > CAPACITY_FRESH_SECONDS:
            out.note("capacity", "capacity_sample_stale", None, pool,
                     f"{pool}: last sample {sample_at} is older than {CAPACITY_FRESH_SECONDS // 60} min; "
                     "waits on it are not re-judged until the capacity probe timer runs again")
    return out


# ---------------------------------------------------------------- legacy board capacity waits (HD-01 C)

_UNITS = {"gib": 1024**3, "g": 1024**3, "mib": 1024**2, "m": 1024**2, "kib": 1024, "kb": 1024, "k": 1024}
_MEM_NEED = re.compile(r"MemAvailable\s*(>=|>|≥)\s*(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|kB|G|M|K)?", re.I)
# "MemAvailable=12286776KiB<14680064": the reading was below the threshold, so the need is above it.
_MEM_SHORT = re.compile(r"MemAvailable\s*=\s*\d+(?:\.\d+)?\s*(?:GiB|MiB|KiB|kB)?\s*(<=|<|≤)\s*"
                        r"(\d+(?:\.\d+)?)\s*(GiB|MiB|KiB|kB|G|M|K)?", re.I)
_IO_WORD = re.compile(r"(?:^|[^a-z])io(?:[^a-z]|$)", re.I)
_IO_NEED = re.compile(r"busy\s*(<=|<|≤)\s*(\d+(?:\.\d+)?)\s*%", re.I)


def legacy_capacity_need(wait_on: str, blocker: str) -> JsonObj:
    """What a legacy ``capacity:...`` wait states, from its wait_on, else its blocker text:
    ``mem_bytes``/``mem_strict`` for MemAvailable (unit-less numbers are KiB, as in /proc/meminfo),
    ``io`` when IO is part of it, ``io_busy_max`` = the stated max device busy% if any."""
    out: JsonObj = {}
    for text in (wait_on, blocker):
        m = _MEM_NEED.search(text)
        if m:
            out["mem_bytes"] = float(m.group(2)) * _UNITS[(m.group(3) or "kib").lower()]
            out["mem_strict"] = m.group(1) == ">"
            break
        m = _MEM_SHORT.search(text)
        if m:
            out["mem_bytes"] = float(m.group(2)) * _UNITS[(m.group(3) or "kib").lower()]
            out["mem_strict"] = m.group(1) in ("<=", "≤")
            break
    if _IO_WORD.search(wait_on.removeprefix("capacity:")):
        out["io"] = True
    for text in (wait_on, blocker):
        m = _IO_NEED.search(text)
        if m:
            out["io"] = True
            out["io_busy_max"] = float(m.group(2))
            break
    return out


def legacy_capacity_waits(legacy_run: JsonObj | None) -> list[tuple[str, str, str, str | None]]:
    """(ticket, wait_on, blocker, updated_at) of open board tickets whose wait_on is ``capacity:...``."""
    if legacy_run is None:
        return []
    closed = legacy_closures(legacy_run)
    out: list[tuple[str, str, str, str | None]] = []
    batches = legacy_run.get("batches")
    for body in (batches.values() if isinstance(batches, dict) else []):
        tickets = body.get("tickets") if isinstance(body, dict) else None
        for name, rec in (tickets.items() if isinstance(tickets, dict) else []):
            if not isinstance(rec, dict) or str(name) in closed:
                continue
            wait_on = _str(rec.get("wait_on"))
            if wait_on and wait_on.startswith("capacity:"):
                out.append((str(name), wait_on, str(rec.get("blocker") or ""), _str(rec.get("updated_at"))))
    return sorted(out)


def legacy_capacity_findings(legacy_run: JsonObj | None, samples: Mapping[str, JsonObj], at: str) -> Resolution:
    """Board (run.json) capacity waits the core does not hold (§8 T14: gate waits come only from gate
    facts, so the seed and the CLI translation keep them as text). Notes only: ``legacy_capacity_met``
    when every judged part is met by a host sample that is fresh and taken after the record's last
    update (the controller turns it into a would_nudge), ``legacy_capacity_stale`` when a needed
    sample is missing or stale, ``legacy_capacity_unparsed`` when no numeric need can be read. IO is
    judged by device busy% (host-io), never by /proc/pressure/io. Details carry no sample values or
    times, so each finding is logged once per episode."""
    out = Resolution()
    now = _epoch(at)
    for name, wait_on, blocker, updated in legacy_capacity_waits(legacy_run):
        try:
            since = _epoch(updated) if updated else 0.0
        except c.CoreError:
            since = 0.0
        need = legacy_capacity_need(wait_on, blocker)
        if "mem_bytes" not in need and "io_busy_max" not in need:
            out.note("capacity", "legacy_capacity_unparsed", name, wait_on,
                     f"{wait_on}: no numeric MemAvailable or busy% need in wait_on/blocker; nothing re-judges it")
            continue
        met: list[str] = []
        unknown: list[str] = []
        stale: list[str] = []
        short = False
        mem = _num(need.get("mem_bytes"))
        if mem is not None:
            sample = samples.get(HOST_MEMORY_POOL)
            value = _num(sample.get("value")) if sample is not None else None
            if _fresh(sample, now, since) is None or value is None:
                stale.append(HOST_MEMORY_POOL)
            elif value > mem or (value == mem and not need.get("mem_strict")):
                met.append(f"MemAvailable {'>' if need.get('mem_strict') else '>='} {mem / 1024**3:.2f} GiB")
            else:
                short = True
        if need.get("io"):
            busy_max = _num(need.get("io_busy_max"))
            if busy_max is None:
                unknown.append("io (no busy% need stated; /proc/pressure/io is not a usable reading here)")
            else:
                sample = samples.get(HOST_IO_POOL)
                value = _num(sample.get("value")) if sample is not None else None
                if _fresh(sample, now, since) is None or value is None:
                    stale.append(HOST_IO_POOL)
                elif 100.0 - value <= busy_max:
                    met.append(f"max device busy <= {busy_max:g}%")
                else:
                    short = True
        if stale:
            out.note("capacity", "legacy_capacity_stale", name, wait_on,
                     f"{wait_on}: no fresh {'/'.join(stale)} sample after the record's last update; "
                     "the wait cannot be re-judged until the capacity probe runs")
        elif met and not short:
            tail = f"; not judged: {', '.join(unknown)}" if unknown else ""
            out.note("capacity", "legacy_capacity_met", name, wait_on,
                     f"{wait_on}: met by a fresh probe sample ({'; '.join(met)}){tail}; re-judge the legacy wait")
    return out


# ---------------------------------------------------------------- run-disk guard (§13.3)


@dataclass(frozen=True)
class DiskStatus:
    path: str
    free_bytes: int
    total_bytes: int
    ok: bool
    reason: str


class _Statvfs(Protocol):
    @property
    def f_bavail(self) -> int: ...

    @property
    def f_blocks(self) -> int: ...

    @property
    def f_frsize(self) -> int: ...


def disk_status(path: Path, statvfs: Callable[[str], _Statvfs] = os.statvfs) -> DiskStatus:
    """Free space of the run disk, outside any transaction. ok=False below 20 GiB or 2 % free: stop
    writing (probe samples, and in B3 evidence/projection) and raise an alert instead."""
    st = statvfs(str(path))
    free, total = int(st.f_bavail) * int(st.f_frsize), int(st.f_blocks) * int(st.f_frsize)
    short = []
    if free < DISK_MIN_FREE_BYTES:
        short.append(f"{free / 1024**3:.1f} GiB free < {DISK_MIN_FREE_BYTES // 1024**3} GiB")
    if total and free / total < DISK_MIN_FREE_RATIO:
        short.append(f"{100 * free / total:.2f}% free < {100 * DISK_MIN_FREE_RATIO:.0f}%")
    return DiskStatus(str(path), free, total, not short, "; ".join(short))


# ---------------------------------------------------------------- one reconciliation pass


def reconcile(world: c.World, run_dir: Path, at: str, *, table: AliasTable | None = None,
              legacy_run: JsonObj | None = None) -> Resolution:
    """Everything the B2 resolvers see at ``at``. Deterministic for the same World, files and clock."""
    run_dir = Path(run_dir)
    out = Resolution()
    try:
        text = (run_dir / ADVISOR_LOG_REL).read_bytes().decode("utf-8")
    except OSError:
        text = ""
    records, notes = parse_advisor_log(text)
    out.notes.extend(notes)
    pages = load_page_files(run_dir, world)
    out.extend(page_window_ops(world, pages, at))
    out.extend(question_ops(world, records, table if table is not None else AliasTable.load(), at,
                            drift_owners=drift_owners(world, pages)))
    out.extend(time_ops(world, at))
    legacy_waits = legacy_capacity_waits(legacy_run)
    samples = load_capacity_samples(run_dir, world, (HOST_MEMORY_POOL, HOST_IO_POOL) if legacy_waits else ())
    out.extend(capacity_ops(world, samples, at))
    if legacy_waits:
        out.extend(legacy_capacity_findings(legacy_run, samples, at))
        out.extend(sample_freshness(samples, (HOST_MEMORY_POOL,), _epoch(at)))
    out.extend(ticket_findings(world, legacy_run))
    return out


class Submitter(Protocol):
    def submit(self, op: c.Op) -> c.Receipt: ...


@dataclass(frozen=True)
class Applied:
    op: c.Op
    receipt: c.Receipt | None
    error: str | None = None


def apply(store: Submitter, ops: Iterable[c.Op]) -> list[Applied]:
    """Submit facts one transaction each. A guard rejection or op conflict is recorded and the next
    fact still goes in; storage errors propagate (stop accepting, §7.1)."""
    done: list[Applied] = []
    for op in ops:
        try:
            done.append(Applied(op, store.submit(op)))
        except (c.Rejected, c.ValidationError, c.OpConflict, c.InvariantError) as exc:
            done.append(Applied(op, None, f"{type(exc).__name__}: {exc}"))
    return done


# ---------------------------------------------------------------- CLI


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    sub = parser.add_subparsers(dest="command", required=True)
    build = sub.add_parser("build-aliases", help="build the §11.7 alias table from an advisor log (read only)")
    build.add_argument("--log", type=Path, required=True)
    build.add_argument("--out", type=Path, default=ALIAS_TABLE)
    args = parser.parse_args(argv)
    if args.command == "build-aliases":
        table = build_alias_table(Path(args.log).read_text(encoding="utf-8"))
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(table, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        loaded = AliasTable.from_json(table)
        keys = table.get("keys")
        print(f"{args.out}: {len(keys) if isinstance(keys, list) else 0} keys, {len(loaded.aliases)} aliases, "
              f"{len(loaded.ambiguous)} ambiguous")
    return 0


if __name__ == "__main__":
    sys.exit(main())

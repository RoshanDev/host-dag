"""B7 read-only legacy snapshot -> one migration.seed transaction. No process or launch APIs.

Only seed() writes sm/. shadow() copies the small input closure into a new workspace .tmp directory;
it never copies VM/build artifacts or follows a symlink. Absolute run references are rebased there.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping

import hostdag_dag_core as c
import hostdag_dag_resolvers as r
import hostdag_dag_controller as ctl
import hostdag_ticket_state as ts
from hostdag_dag_translate import external_markers
from hostdag_dag_core import Json, JsonObj

WORKSPACE = Path(__file__).resolve().parents[1]
LIMIT = "legacy coordinator records; no first-hand HOST-DAG-RESULT declaration (B5 §16.5/16.8)"
_MISSING_DELIVERY_WARNED: set[str] = set()


def _warn_missing_delivery(module: str) -> None:
    if module not in _MISSING_DELIVERY_WARNED:
        print(f"WARNING: {module} unavailable; skipping release relationship checks", file=sys.stderr)
        _MISSING_DELIVERY_WARNED.add(module)


def obj(value: Json) -> JsonObj:
    return value if isinstance(value, dict) else {}


def string(value: Json) -> str | None:
    return value if isinstance(value, str) and value else None


def stamp(value: Json, fallback: str) -> str:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return r.iso_utc(float(value))
    if isinstance(value, str):
        try:
            c.parse_at(value)
            return value
        except c.ValidationError:
            pass
    return fallback


def ident(*parts: str) -> str:
    return "seed-" + hashlib.sha256(c.canonical(list(parts)).encode()).hexdigest()[:24]


@dataclass
class Snapshot:
    root: Path
    files: dict[str, bytes] = field(default_factory=dict)

    def read(self, rel: str, *, required: bool = False) -> bytes:
        if rel in self.files:
            return self.files[rel]
        path = self.root / rel
        if (not path.resolve().is_relative_to(self.root.resolve())
                or any(p.is_symlink() for p in (path, *path.parents) if p.is_relative_to(self.root))):
            raise ctl.ControllerError(f"snapshot refuses symlink/escape: {path}")
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            if required:
                raise ctl.ControllerError(f"missing input: {path}") from None
            data = b""
        self.files[rel] = data
        return data

    def json(self, rel: str, *, required: bool = False) -> JsonObj:
        data = self.read(rel, required=required)
        if not data:
            return {}
        try:
            value = json.loads(data)
        except ValueError as exc:
            raise ctl.ControllerError(f"invalid snapshot JSON: {rel}: {exc}") from exc
        if not isinstance(value, dict):
            raise ctl.ControllerError(f"snapshot JSON must be an object: {rel}")
        return value

    def manifest(self) -> JsonObj:
        return {rel: hashlib.sha256(data).hexdigest() for rel, data in sorted(self.files.items())}

    def verify(self) -> None:
        # No shared lock can make all old producers atomic; refuse a changing input closure.
        for rel, data in self.files.items():
            path = self.root / rel
            if (not path.resolve().is_relative_to(self.root)
                    or any(p.is_symlink() for p in (path, *path.parents) if p.is_relative_to(self.root))):
                raise ctl.ControllerError(f"snapshot changed to symlink/escape: {path}")
            current = path.read_bytes() if path.exists() else b""
            if current != data:
                raise ctl.ControllerError(f"snapshot changed while reading {rel}; retry seed/shadow")


def capture(root: Path) -> Snapshot:
    snap = Snapshot(root.resolve())
    run = snap.json("run.json", required=True)
    snap.json("watch.json")
    registry = snap.json(r.REGISTRY_REL)
    snap.read(r.ADVISOR_LOG_REL)
    for rel in ("control/pagewindow-issues.jsonl", "control/pagewindow-issues-resolved.jsonl", r.CLAIMS_REL):
        snap.read(rel)
    board, duplicates = ctl.board_tickets(run)
    if duplicates:
        raise ctl.ControllerError(f"duplicate ticket identities: {sorted(duplicates)}")
    def internal(value: str, base: Path) -> Path:
        path = Path(value)
        path = path if path.is_absolute() else base / path
        # Keep the lexical path so Snapshot.read also rejects symlinks.
        path = Path(os.path.abspath(path))
        if not path.resolve().is_relative_to(snap.root):
            raise ctl.ControllerError(f"external page input is outside snapshot closure: {path}")
        return path

    def run_path(value: str) -> Path:
        # Library runDir/current-run.dir are absolute. Do not let a relative value be resolved
        # against the controller's cwd after copying instead of against this snapshot.
        if not Path(value).is_absolute():
            raise ctl.ControllerError(f"relative page run directory cannot close snapshot inputs: {value}")
        return internal(value, snap.root)

    claims = r.parse_page_claims(snap.read(r.CLAIMS_REL).decode("utf-8"))
    specs = [obj(rec.get("page_request")).get("spec") for _, rec in board.values()]
    specs.extend([registry.get("spec"), *(obj(row.get("spec")).get("path") for row in claims)])
    run_dirs: set[Path] = set()
    for value in specs:
        name = string(value)
        if name is None:
            continue
        path = internal(name, snap.root)
        spec = snap.json(str(path.relative_to(snap.root)))
        evidence = internal(string(spec.get("evidence")) or ".", path.parent)
        for filename in ("result.json", "current-run.json"):
            rec = snap.json(str((evidence / filename).relative_to(snap.root)))
            for key in ("dir", "runDir"):
                ref = string(rec.get(key))
                if ref:
                    run_dirs.add(run_path(ref))
    for value in [registry.get("runDir"), *(row.get("runDir") for row in claims)]:
        name = string(value)
        if name:
            run_dirs.add(run_path(name))
    visited: set[Path] = set()
    while run_dirs - visited:
        folder = min(run_dirs - visited)
        visited.add(folder)
        result = snap.json(str((folder / "result.json").relative_to(snap.root)))
        ref = string(result.get("runDir"))
        if ref:
            run_dirs.add(run_path(ref))
        snap.read(str((folder / "open.json").relative_to(snap.root)))
        # WAIT_RESOURCE parsing may read capacity evidence in this run.
        for path in sorted(folder.glob("memory-*.json")):
            snap.read(str(path.relative_to(snap.root)))
    snap.verify()
    return snap


def _wait(t: c.Ticket, kind: str, ref: str, at: str, *, detail: JsonObj | None = None,
          on: c.Continuation | None = None) -> c.Wait:
    w = c.Wait(ident(t.ticket, kind, ref), kind, ref, 1, "migration.seed", at,
               on_satisfied=on or c.Continuation(), detail=detail or {})
    t.waits.append(w)
    return w


def _sessions(t: c.Ticket, rec: JsonObj, tracked: JsonObj, at: str) -> None:
    sessions: dict[tuple[str, str | None, str], JsonObj] = {}
    executor = string(rec.get("executor")) or string(rec.get("session"))
    if executor:
        sessions[("executor", None, executor)] = {}
    for review_axis, review in obj(rec.get("reviews")).items():
        session = string(obj(review).get("thread"))
        if review_axis in c.AXES and session:
            sessions[("reviewer", review_axis, session)] = {}
    for key, value in tracked.items():
        role, axis = ("executor", None) if key == t.ticket else ("reviewer", key.removeprefix(t.ticket + "-review-"))
        if key != t.ticket and (not key.startswith(t.ticket + "-review-") or axis not in c.AXES):
            continue
        seen_record = obj(value)
        session = string(seen_record.get("thread")) or string(seen_record.get("run_file"))
        if session:
            sessions[(role, axis, session)] = seen_record
    for (role, axis, session), seen in sessions.items():
        live = bool(seen) and not seen.get("terminal") and seen.get("last_status") in ("running", "starting", None)
        observed = "running" if live else ("finished" if seen.get("terminal") or t.phase == c.CLOSED else "unknown")
        eid = ident(t.ticket, role, axis or "", session)
        e = c.Execution(eid, t.attempt, role, "adopt", stamp(seen.get("direction_anchor"), at),
                        observed, stamp(seen.get("last_direction_at"), at), axis=axis,
                        candidate_digest=t.candidate.digest if role == "reviewer" and t.candidate else None,
                        provider=string(seen.get("provider")) or "legacy", model=string(rec.get("model")),
                        effort=string(rec.get("effort")), session=session, contract=string(rec.get("contract")))
        if observed == "finished":
            e.result = {"imported": {"limit": LIMIT, "last_status": seen.get("last_status")}}
        t.executions.append(e)
        if role == "reviewer" and axis and t.candidate:
            t.reviews.append(c.Review(ident(eid, "review"), axis, t.candidate.digest, eid))


def _window(t: c.Ticket, rec: JsonObj, registry: JsonObj, at: str, root: Path) -> None:
    page = obj(rec.get("page_request"))
    owner = string(page.get("owner"))
    if not page:
        return
    entry = string(page.get("id")) or ident(t.ticket, "window", owner or "")
    queued = stamp(page.get("approved_at"), at)
    spec = string(page.get("spec"))
    if spec:
        spec = str(root / spec)
    detail: JsonObj = {"spec": spec, "imported": {"limit": "adopt only; never relaunch an imported window"}}
    reg_value = registry.get("at") if owner and registry.get("owner") == owner else None
    reg_stamp = stamp(reg_value, "")
    reg_matches = bool(reg_stamp) and c.parse_at(reg_stamp) >= c.parse_at(queued)
    claimed = page.get("status") == "claimed" or reg_matches
    if t.phase == c.CLOSED and not reg_matches:
        # Closure does not authenticate an old window result. Keep the unresolved history outside
        # operational waits/outbox, so closed tickets never acquire an open run_window.
        t.imported["page_window"] = {"entry_id": entry, "spec": spec, "record": page, "queued_at": queued,
                                     "limit": "historical window; terminal outcome not verified"}
        return
    w = _wait(t, "page-window", entry, queued, detail=detail)
    claim_value = reg_value if reg_matches else page.get("claimed_at_ts") or page.get("claimed_at")
    if claimed and claim_value is None:
        raise ctl.ControllerError(f"{t.ticket}: running window has no real claim time")
    if claimed:
        claim = stamp(claim_value, queued)
        if claimed and (stamp(claim_value, "") == "" or c.parse_at(claim) < c.parse_at(queued)):
            raise ctl.ControllerError(f"{t.ticket}: claim time predates this window or is invalid")
        w.satisfied_at, w.continued = claim, True
        lease = f"{c.parse_at(claim).timestamp():.6f}"
        w.satisfied_by = {"source_record": f"pagewindow:{owner}:claim:{lease}", "holder": owner, "lease": "claim@" + lease}
    # Legacy status/outcome and ticket closure are only historical claims (t.imported.record).
    # The resolver must see this entry until a bound library result confirms its end.
    status = c.DONE_P if w.satisfied_at is not None else c.CLAIMED_P
    t.pending.append(c.Pending(string(page.get("action_id")) or ident(entry, "run"), "run_window", c.OUTBOX, [], status, queued, at,
                               {"entry_id": entry}, {"spec": spec, "priority": page.get("priority") or 0,
                                                    "migration_adopt": True}))


def _user_pending(t: c.Ticket, q: c.Question, at: str) -> None:
    # Already-routed user questions get an answerable decision, not a second notification.
    t.pending.append(c.Pending(ident(q.question_id, "answer"), "answer_user", c.DECIDE,
                               c.allowed_actions(t, "answer_user"), c.QUEUED, q.asked_at, at,
                               {"question_id": q.question_id}, {"key": q.key, "title": q.title}))


def plan(snap: Snapshot, at: str) -> c.Op:
    run, watch = snap.json("run.json", required=True), snap.json("watch.json")
    board, _ = ctl.board_tickets(run)
    closures = r.legacy_closures({"batches": {"all": {"tickets": {n: rec for n, (_, rec) in board.items()}}}})
    tickets: dict[str, c.Ticket] = {}
    for name, (batch, rec) in board.items():
        created = stamp(rec.get("created_at"), stamp(rec.get("updated_at"), at))
        t = c.Ticket(name, batch, string(rec.get("title")) or name, created, created,
                     notes=str(rec.get("blocker") or ""), attempt=ts.attempt_of(rec))
        deps = rec.get("deps")
        t.deps = [str(x) for x in deps] if isinstance(deps, list) else []
        try:
            from release_coverage import release_relationships
        except ImportError:
            _warn_missing_delivery("release_coverage")
        else:
            t.release_group, t.requires_for_release = release_relationships(rec)
        t.release_obligation = rec.get("release_obligation")
        t.imported = {"limit": LIMIT, "record": rec}
        if name in closures:
            t.phase = c.CLOSED
            t.closure = c.Closure(closures[name], stamp(rec.get("accepted_at"), created),
                                  reason="imported legacy closure; " + LIMIT)
        candidate = string(rec.get("candidate"))
        has_reviews = bool(obj(rec.get("reviews"))) or any(k.startswith(name + "-review-") for k in obj(watch.get("tracked")))
        if candidate:
            t.candidate = c.Candidate(candidate, str(rec.get("evidence") or ""), "imported:" + name)
            if t.phase != c.CLOSED:
                t.phase = c.REVIEW
        elif has_reviews:
            # No fake candidate binding: sessions are retained, verdicts withheld.
            t.imported["review_limit"] = "review sessions have no recorded candidate"
        _sessions(t, rec, obj(watch.get("tracked")), created)
        if rec.get("state") == "CREATED" and not t.executions and t.phase != c.CLOSED:
            t.scheduling, t.defer = c.DEFERRED, string(rec.get("after"))
        _window(t, rec, snap.json(r.REGISTRY_REL), created, snap.root)
        if t.window_running():
            w = next(w for w in t.waits if w.kind == "page-window")
            spec_name = string(w.detail.get("spec"))
            if spec_name and w.satisfied_at:
                path = Path(spec_name)
                spec_body = snap.json(str(path.relative_to(snap.root)))
                claim_at = c.parse_at(w.satisfied_at).timestamp()
                binding, _ = r.page_claim_binding(name, spec_name, string(spec_body.get("owner")) or "",
                    r.parse_page_claims(snap.read(r.CLAIMS_REL).decode("utf-8")), claim_at,
                    lease_at=claim_at,
                    read_open=lambda filename: snap.json(str(Path(filename).relative_to(snap.root))))
                w.detail.update(binding)
        tickets[name] = t
    # Import the CURRENT question identity before matching any log. Legacy answers never replace
    # a newer question merely because their keys are equal; B2 owns file/time/revision matching.
    for name, t in tickets.items():
        rec = board[name][1]
        wait = string(rec.get("wait_on")) or ""
        kind, _, ref = wait.partition(":")
        if t.phase == c.CLOSED:
            continue
        if kind == "decision" or rec.get("state") == "NEEDS_USER":
            info = obj(rec.get("question"))
            key = string(info.get("key")) or ref or string(rec.get("wait_holder")) or name + ":needs-user"
            asked_value = info.get("asked_at") or rec.get("question_asked_at") or rec.get("asked_at")
            asked = stamp(asked_value, t.created_at)
            if not asked_value:
                t.imported["question_time_limit"] = "original ask time missing; legacy updated_at used"
            qid = string(info.get("question_id")) or string(rec.get("question_id")) or ident(name, key, asked)
            raw_revision = info.get("revision") or rec.get("question_revision")
            revision = raw_revision if isinstance(raw_revision, int) and not isinstance(raw_revision, bool) else 1
            q = c.Question(qid, revision, "user" if rec.get("state") == "NEEDS_USER" else "advisor",
                           key, string(info.get("title")) or t.title, asked,
                           string(info.get("question_file")) or string(rec.get("question_file")))
            t.questions.append(q)
            if q.revision > 1:
                t.imported["question_history_limits"] = {q.question_id: asked}
            w = _wait(t, "question", qid, asked, on=c.Continuation("apply_decision", {"question_id": qid}))
            w.wait_id = string(info.get("wait_id")) or string(rec.get("wait_id")) or w.wait_id
            if q.to == "user":
                _user_pending(t, q, at)
        elif kind in ("ticket", "batch", "time", "external") and ref:
            _wait(t, kind, ref, t.created_at)
        elif kind and kind != "page-window":
            # A legacy string is not a real gate reading; expose the limitation instead of fabricating one.
            t.imported["wait_limit"] = {"wait_on": wait, "reason": "no structured gate evidence"}
        legacy_ext = _legacy_external(rec)
        if legacy_ext is not None and not any(w.kind == "external" and w.open for w in t.waits):
            # HD-01 B: a legacy external block is imported (never refused) with whatever structure its
            # blocker text carries; the shadow controller flags the missing owner/attempts/recheck_at.
            _wait(t, "external", legacy_ext[0], t.created_at, detail=legacy_ext[1])
    log = snap.read(r.ADVISOR_LOG_REL).decode("utf-8")
    records, notes = r.parse_advisor_log(log)
    table = r.AliasTable.load()
    current_keys = {q.key for t in tickets.values() for q in t.questions}
    resolved = r.question_ops(c.World(tickets, c.Meta("seed")), records, table, at)
    notes.extend(resolved.notes)
    consumed: set[str] = set()
    for answer_op in resolved.ops:
        t = tickets[answer_op.ticket or ""]
        payload = answer_op.payload
        matched_question = t.question_by_id(str(payload["question_id"]))
        assert matched_question is not None
        q = matched_question
        source = str(payload["source_record"])
        consumed.add(source)
        q.answer = {"verdict": payload["verdict"], "text": payload["text"],
                    "source_record": source, "at": at}
        for w in t.waits:
            if w.kind == "question" and w.ref == q.question_id:
                w.satisfied_at = at
                w.satisfied_by = {"source_record": source, "verdict": payload["verdict"]}
                if payload["verdict"] == "needs_user":
                    w.continued = True
                    uq = c.Question(ident(q.question_id, "user"), q.revision + 1, "user", q.key, q.title,
                                    at, q.question_file)
                    t.questions.append(uq)
                    _wait(t, "question", uq.question_id, at,
                          on=c.Continuation("apply_decision", {"question_id": uq.question_id}))
                    _user_pending(t, uq, at)
    unbound: list[Json] = []
    for answer in records:
        if answer.source_record in consumed:
            continue
        key = answer.key.removeprefix("advisor:")
        names = [name for name in tickets if key == name or key.startswith(name + ":") or key.startswith(name + "-")]
        matched_current = any(answer.key in r.resolve_key(k, [answer.key], table)[0] for k in current_keys)
        if not names or matched_current:
            unbound.append({"source_record": answer.source_record, "key": answer.key,
                            "asked_at": r.iso_utc(answer.at), "verdict": answer.verdict, "text": answer.decision,
                            "question_file": answer.question_file,
                            "limit": "not bound to the current question by B2"})
            continue
        t = tickets[max(names, key=len)]
        t.questions.append(c.Question(ident(t.ticket, answer.source_record),
                                      1 + sum(q.key == answer.key for q in t.questions), "advisor",
                                      answer.key, answer.key, r.iso_utc(answer.at), answer.question_file,
                                      {"verdict": answer.verdict, "text": answer.decision,
                                       "source_record": answer.source_record, "at": r.iso_utc(answer.at)}))
    manifest = snap.manifest()
    digest = hashlib.sha256(c.canonical(manifest).encode()).hexdigest()
    batches: JsonObj = {name: {k: v for k, v in obj(value).items() if k != "tickets"}
               for name, value in obj(run.get("batches")).items()}
    if any(t.batch == "__root__" for t in tickets.values()):
        batches["__root__"] = {"limit": "pre-batch run.json.tickets"}
    seed: JsonObj = {"source_digest": digest, "sources": manifest, "batches": batches,
                     "unbound_questions": unbound, "notes": [n.to_json() for n in notes],
                     "registry": snap.json(r.REGISTRY_REL)}
    return c.Op("migration.seed", "migration.seed", "seed", at,
                payload={"tickets": [t.to_json() for t in tickets.values()], "seed": seed})


def counts(world: c.World) -> JsonObj:
    unbound = world.meta.seed.get("unbound_questions")
    return {"tickets": len(world.tickets), "batches": len(obj(world.meta.seed.get("batches"))),
            "sessions": sum(len(t.executions) for t in world.tickets.values()),
            "active_sessions": sum(len(t.active()) for t in world.tickets.values()),
            "questions": sum(len(t.questions) for t in world.tickets.values()),
            "unbound_questions": len(unbound) if isinstance(unbound, list) else 0,
            "windows": sum(w.kind == "page-window" for t in world.tickets.values() for w in t.waits),
            "running_windows": sum(t.window_running() for t in world.tickets.values())}


def _legacy_external(rec: JsonObj) -> tuple[str, JsonObj] | None:
    """(ref, detail) of the external wait a legacy EXTERNALLY_BLOCKED record imports as, else None."""
    if "EXTERNALLY_BLOCKED" not in (rec.get("state"), rec.get("outcome")):
        return None
    text = string(rec.get("blocker")) or "externally blocked (legacy record without blocker text)"
    return text, {**external_markers(text), "legacy_import": True}


def backfill_external_ops(world: c.World, run: JsonObj, at: str) -> list[c.Op]:
    """HD-01 B backfill for a database seeded before legacy external blocks were imported: one
    ``wait.add external`` (actor seed, ``legacy_import``) per open core ticket that run.json marks
    EXTERNALLY_BLOCKED and that has no external wait of any state yet (a waived one is a decision,
    not a gap). The op id and wait id are content-addressed (ticket + blocker text), so a re-run is
    a replay, and once the wait exists the ticket is no longer selected."""
    board, _ = ctl.board_tickets(run)
    ops: list[c.Op] = []
    for name in sorted(board):
        t = world.tickets.get(name)
        found = _legacy_external(board[name][1])
        if t is None or t.phase == c.CLOSED or found is None or any(w.kind == "external" for w in t.waits):
            continue
        text, detail = found
        wait_id = ident(name, "external", text)
        ops.append(c.Op("backfill-ext-" + wait_id.removeprefix("seed-"), "wait.add", "seed", at, name, None, None,
                        {"kind": "external", "ref": text, "wait_id": wait_id, "detail": detail,
                         "note": "HD-01 backfill: legacy EXTERNALLY_BLOCKED record"}))
    return ops


def backfill_external(cfg: ctl.Config, at: str) -> JsonObj:
    """Submit ``backfill_external_ops`` to an already seeded database, one transaction per op through
    the core (guards, invariants, audit events as for any command). Repeatable: unchanged inputs give
    no new ops; a crash between ops resumes with the rest."""
    if (cfg.sm_dir / c.QUIESCE_MARKER).exists():
        raise c.QuiescedError("quiesced; resume before the backfill")
    run = ctl._read_json(cfg.run_dir / "run.json")
    if run is None:
        raise ctl.ControllerError("backfill needs a readable run.json")
    store = c.SqliteStore.open(cfg.db, cfg.run_id)
    try:
        world = store.world()
        if not world.meta.seed:
            raise ctl.ControllerError("database is not seeded; run seed (which imports external blocks itself)")
        receipts = [store.submit(op) for op in backfill_external_ops(world, run, at)]
    finally:
        store.close()
    return {"status": "ok", "added": [x.ticket for x in receipts if x.status == "accepted"],
            "receipts": [{"op_id": x.op_id, "ticket": x.ticket, "status": x.status} for x in receipts]}


def _seed_open(cfg: ctl.Config, at: str, op: c.Op | None = None) -> JsonObj:
    store = c.SqliteStore.open(cfg.db, cfg.run_id)
    try:
        world = store.world()
        if world.meta.seed:
            return {"status": "already_seeded", **counts(world)}
        if op is None:
            snap = capture(cfg.run_dir)
            op = plan(snap, at)
            snap.verify()
        receipt = store.submit(op)
        # New databases are published only after all WAL pages reached the DB and the writer closed.
        busy, _, _ = store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if busy:
            raise ctl.ControllerError("seed checkpoint busy; database not published")
        return {"status": receipt.status, **counts(store.world()), "source_digest": obj(op.payload["seed"])["source_digest"]}
    finally:
        store.close()


def seed(cfg: ctl.Config, at: str) -> JsonObj:
    """Explicit import, no external effects. Advisor checks §15.4/§16.3 before live deployment.

    Never expose an empty state.db: its existence enables B4/B5/B6 immediately. A new database is
    built in a private directory, imported in one transaction, checkpointed and closed, then linked
    atomically without overwrite. Failed imports leave no official database for mirrors to enter.
    """
    if not cfg.run_dir.is_dir() or not (cfg.run_dir / "run.json").is_file():
        raise ctl.ControllerError("seed needs an existing run.json")
    if (cfg.sm_dir / c.QUIESCE_MARKER).exists():
        raise c.QuiescedError("seed is quiesced; resume before importing")
    disk = r.disk_status(cfg.run_dir, cfg.statvfs)
    if not disk.ok:
        raise ctl.ControllerError(f"seed disk low: {disk.reason}")
    if cfg.db.exists():
        return _seed_open(cfg, at)
    snap = capture(cfg.run_dir)
    op = plan(snap, at)
    c.reduce(c.World({}, c.Meta(cfg.run_id)), op)  # fail malformed snapshots before creating any database
    ctl.SmFiles(cfg.sm_dir).check_dir()
    cfg.sm_dir.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".seed-", dir=cfg.sm_dir) as folder:
        staged = Path(folder) / "state.db"
        c.init_db(staged, cfg.run_id, at=at)
        store = c.SqliteStore.open(staged, cfg.run_id)
        try:
            snap.verify()
            receipt = store.submit(op)
            busy, _, _ = store.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if busy:
                raise ctl.ControllerError("seed checkpoint busy; database not published")
            result: JsonObj = {"status": receipt.status, **counts(store.world()),
                               "source_digest": obj(op.payload["seed"])["source_digest"]}
        finally:
            store.close()
        snap.verify()  # staging can take seconds on USB; mirrors were still off throughout that interval
        if (cfg.sm_dir / c.QUIESCE_MARKER).exists():
            raise c.QuiescedError("quiesced while staging; database not published")
        try:
            os.link(staged, cfg.db)  # no overwrite if another seed won the publication race
        except FileExistsError:
            return _seed_open(cfg, at)
        fd = os.open(cfg.sm_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return result


def _rebase(value: Json, source: Path, target: Path) -> Json:
    if isinstance(value, str):
        return value.replace(str(source), str(target))
    if isinstance(value, list):
        return [_rebase(x, source, target) for x in value]
    if isinstance(value, dict):
        return {k: _rebase(v, source, target) for k, v in value.items()}
    return value


def shadow(source: Path, run_id: str, at: str, tmp_root: Path | None = None) -> JsonObj:
    tmp = (tmp_root or WORKSPACE / ".tmp").resolve()
    if not tmp.is_relative_to(WORKSPACE / ".tmp"):
        raise ctl.ControllerError("shadow destination must be inside this worktree's .tmp/")
    tmp.mkdir(parents=True, exist_ok=True)
    snap = capture(source)
    target = Path(tempfile.mkdtemp(prefix="dag-b7-shadow-", dir=tmp)) / "run"
    target.mkdir()
    for rel, data in snap.files.items():
        if not data:
            continue
        dest = target / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        if rel.endswith((".jsonl", ".ndjson")):
            lines = []
            for line in data.decode().splitlines():
                try:
                    lines.append(json.dumps(_rebase(json.loads(line), snap.root, target), ensure_ascii=False))
                except ValueError:
                    lines.append(line)
            dest.write_text("\n".join(lines) + "\n", encoding="utf-8")
        else:
            dest.write_text(json.dumps(_rebase(json.loads(data), snap.root, target), ensure_ascii=False), encoding="utf-8")
    cfg = ctl.Config(target, run_id, backup_root=None)
    result = seed(cfg, at)
    result["second_seed"] = seed(cfg, at)["status"]
    report = ctl.Controller(cfg, lambda: at).cycle()
    result.update(copy=str(target), cycle=report.to_json(),
                  diff_report=ctl.diff_report(ctl.SmFiles(cfg.sm_dir).jsonl(ctl.DIFF_LOG), at, 24, 300),
                  source_manifest=snap.manifest())
    snap.verify()
    ctl.SmFiles(cfg.sm_dir).write_json("seed-report.json", result)
    return result

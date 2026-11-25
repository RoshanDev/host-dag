"""Host DAG stage B5: the watcher's facts (``execution.observed``, ``review.observed``).

Design: docs/local-dev/dag-state-machine-design-20261019.md §3.3 (facts; late/out-of-order -> stale), §8 (a
PASS must point to a result the watcher can verify), §10 B5, §13.4 (a db error never stops the watcher
loop), §15.8 (the CLI relays no verdict and no candidate: they come only from here), §16 (status, and
§16.6 for the B5 review fixes I1-I4/N1-N7).

Evidence rule. A fact is derived only from what the watcher itself read from the session:

* the session's own state (Codex/Claude-host ``thread read`` status, turn id and cwd; for Cursor/Claude
  CLI runs the sibling ``cli-result.json`` of the run file) -> ``execution.observed``;
* a verdict or a candidate only from an explicit claim that ends the session's own final message: its
  last non-blank line(s), at column 0, not quoted, not indented, not inside a code fence, are
  ``HOST-DAG-RESULT: {json}`` and/or ``HOST-DAG-RESULT-FILE: /abs/path.json sha256=<64 hex>``. A file claim is
  accepted only when the file's bytes hash to the value the session itself wrote, and only from the
  session's own cwd or ``<run>/evidence/<batch>/<ticket>/``. Free text ("SOURCE PASS") is never interpreted.
* a claim is bound to one terminal turn. When the watcher first sees a session under a tracked key it
  records that session's last terminal turn (``sm/watch-mirror.json`` ``sessions``; None if it was running
  or fresh): that turn, and any turn that is the result of an earlier execution of the session on the
  ticket, is never this execution's result. A finished result is recorded only from a read that has the
  final message and, for a file claim, the file with the pinned hash; a missing/unreadable file or an empty
  message is waited for. Once recorded, a turn's result (with its claim, or ``claim_problem``) is fixed:
  a hash mismatch or an invalid claim is never retried, and the file of a fixed turn is not read again.

Nothing is ever read from run.json or watch.json fields the coordinator writes. Every fact carries
``source``: provider, session, cwd, turn, sha256 of the final message, and for a file its path, sha256,
size and mtime.

``mirror()`` runs inside the watcher's bounded daemon worker (host-dag-watch.py ``b5_mirror``), only
after that worker saw ``<run>/sm/state.db``. It never raises and never prints. One connection per call.
Quiesced, wrong interpreter, old SQLite, low disk, busy, storage error, legacy authority mode, an overrun
of the budget, any exception: this cycle's mirror is skipped and at most one alert per episode goes to
``sm/alerts.jsonl`` (none once quiesced). A core refusal or a stale receipt is a shadow result, logged in
``sm/watch-facts.jsonl``, not an alert.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping, MutableMapping, Sequence

import hostdag_dag_cli as cli
import hostdag_dag_controller as ctl
import hostdag_dag_core as c
import hostdag_dag_resolvers as r
from hostdag_dag_core import Json, JsonObj
from hostdag_cli_watch import CLI_PROVIDERS

MARKER = "HOST-DAG-RESULT:"
FILE_MARKER = "HOST-DAG-RESULT-FILE:"
FILE_CLAIM = re.compile(r"^(/\S+)\s+sha256=([0-9a-f]{64})$")
FENCE = re.compile(r"^\s*(```|~~~)")
REVIEW_MARK = "-review-"
SHA = re.compile(r"^[0-9a-f]{7,64}$")
MAX_FILE_BYTES = 256 * 1024
MAX_OPS_PER_CYCLE = 24
MIRROR_BUSY_MS = 250
WATCH_LOG = "watch-facts.jsonl"
WATCH_STATE = "watch-mirror.json"
MEMORY_KEYS = ("episode", "withheld", "refused", "sessions", "waits")
WAIT_LIMIT_SECONDS = 600  # then the turn is recorded with claim_problem "timed out waiting for ..."
SESSIONS_KEEP = 1000
REFUSED_KEEP = 200
EVIDENCE_DIR = "evidence"
# Coordinator/watcher record files: never a session's result, wherever they are (N2).
COORDINATOR_FILES = frozenset({"run.json", "watch.json", "watch-unit.json", "quota-policy.json",
                               "telegram-sent.json", "telegram-bridge.json", "alerts.jsonl"})
SEVERITY_ORDER = {s: i for i, s in enumerate(c.SEVERITIES)}  # P0 is the worst
BLOCKING = frozenset({"P0", "P1"})
CLI_RESULT = "cli-result.json"
CLI_TURN = "cli-run"  # one CLI run file is one run: its only "turn"
_OPEN_READ = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)

# Session status -> observed. Codex and the Claude host speak the same `thread read` statuses.
THREAD_OBSERVED = {"running": "running", "completed": "finished", "failed": "finished", "cancelled": "finished"}
CLI_OBSERVED = {"RUNNING": "running", "COMPLETED": "finished", "FAILED": "finished", "CANCELLED": "finished",
                "LOST": "finished", "START_FAILED": "finished", "QUOTA_EXHAUSTED": "unknown", "AUTH_FAILED": "unknown"}
CLI_STATUS = {"COMPLETED": "completed", "CANCELLED": "cancelled"}  # every other terminal status: failed
CLAIM_KEYS = ("candidate", "evidence", "outcome", "verdict", "max_severity")
RETRYABLE = ("does not exist", "cannot open", "unreadable")


# ---------------------------------------------------------------- what the watcher saw (main thread)


@dataclass(frozen=True)
class Observation:
    """One session read by the watcher in this cycle, copied out of the main thread. ``session`` is the
    identity the core records for it (Codex/Claude-host thread id, CLI run file, Grok sessionId)."""
    key: str  # watch.json tracked key: <ticket> or <ticket>-review-<axis>
    provider: str  # codex | claude | cursor | grok
    session: str
    status: str  # raw status as read
    at: str
    turn: str | None = None
    text: str | None = None  # the final message of the last turn, in full (thread sessions)
    error: str | None = None  # thread read failed with this code (session not readable)
    healed: bool = False  # the watcher re-sent a failed turn: not a finish
    cwd: str | None = None  # the session's working directory as the host reports it

    @staticmethod
    def load(o: Mapping[str, object]) -> Observation | None:
        """Runtime validation of what the untyped watcher hands over; None when unusable."""
        def s(key: str) -> str | None:
            v = o.get(key)
            return v if isinstance(v, str) and v else None
        key, provider, session, status, at = s("key"), s("provider"), s("session"), s("status"), s("at")
        if not (key and provider and session and status and at):
            return None
        text = o.get("text")
        return Observation(key, provider, session, status, at, s("turn"), text if isinstance(text, str) else None,
                           s("error"), bool(o.get("healed")), s("cwd"))

    def ticket_role(self) -> tuple[str, str, str | None]:
        """(ticket, executor|reviewer|unknown, axis). A review key must end in a known axis (N3)."""
        for axis in c.AXES:
            suffix = REVIEW_MARK + axis
            if self.key.endswith(suffix) and len(self.key) > len(suffix):
                return self.key[:-len(suffix)], "reviewer", axis
        if REVIEW_MARK in self.key:
            return self.key.rsplit(REVIEW_MARK, 1)[0], "unknown", None
        return self.key, "executor", None


# ---------------------------------------------------------------- claims (pure)


@dataclass(frozen=True)
class Markers:
    claim: JsonObj | None = None
    file: str | None = None
    file_sha256: str | None = None
    problem: str | None = None


def parse_markers(text: str) -> Markers:
    """The claim that ends the final message (I3): its last non-blank lines, at column 0, not quoted,
    not indented, outside any code fence; at most one result line and one file line. A marker anywhere
    else is an echo or an example, never a claim."""
    lines = text.splitlines()
    while lines and not lines[-1].strip():
        lines.pop()
    block: list[str] = []
    while lines and len(block) < 2 and (lines[-1].startswith(MARKER) or lines[-1].startswith(FILE_MARKER)):
        block.insert(0, lines.pop().rstrip())
    elsewhere = any(MARKER in x or FILE_MARKER in x for x in lines)
    if not block:
        return Markers(problem="a HOST-DAG-RESULT marker that does not end the message is ignored" if elsewhere
                       else None)
    if sum(1 for x in lines if FENCE.match(x)) % 2:
        return Markers(problem="the closing claim is inside a code fence")
    results = [x for x in block if x.startswith(MARKER) and not x.startswith(FILE_MARKER)]
    files = [x for x in block if x.startswith(FILE_MARKER)]
    if len(results) > 1 or len(files) > 1:
        return Markers(problem="two claim lines of the same kind")
    claim: JsonObj | None = None
    if results:
        try:
            value = json.loads(results[0][len(MARKER):].strip())
        except ValueError:
            return Markers(problem=f"{MARKER} line is not JSON")
        if not isinstance(value, dict):
            return Markers(problem=f"{MARKER} line is not a JSON object")
        claim = value
    path = digest = None
    if files:
        m = FILE_CLAIM.match(files[0][len(FILE_MARKER):].strip())
        if m is None:
            return Markers(problem=f"{FILE_MARKER} needs '<absolute path> sha256=<64 hex>'; withheld")
        path, digest = m.group(1), m.group(2)
    return Markers(claim, path, digest)


def normalize_claim(raw: Mapping[str, Json]) -> tuple[JsonObj, str | None]:
    """The fields of a claim this module understands, validated. Accepts the reviewers' existing result
    file shape too (``status`` PASS/FAIL, ``findings[].severity``, ``head`` as the reviewed candidate)."""
    out: JsonObj = {}
    verdict = raw.get("verdict")
    if verdict is None and raw.get("status") in c.VERDICTS:
        verdict = raw.get("status")
    if verdict is not None:
        if verdict not in c.VERDICTS:
            return {}, f"verdict {verdict!r} is not PASS/FAIL"
        out["verdict"] = verdict
    severity = raw.get("max_severity")
    findings = raw.get("findings")
    if severity is None and isinstance(findings, list):
        found = [f.get("severity") for f in findings if isinstance(f, dict)]
        known = [s for s in found if isinstance(s, str) and s in SEVERITY_ORDER]
        if len(known) != len(found):
            return {}, "a finding has no P0-P3 severity"
        severity = min(known, key=SEVERITY_ORDER.__getitem__) if known else None
    if severity is not None:
        if severity not in SEVERITY_ORDER:
            return {}, f"max_severity {severity!r} is not P0-P3"
        out["max_severity"] = severity
    if out.get("verdict") == "FAIL" and "max_severity" not in out:
        return {}, "a FAIL claim needs max_severity (or findings with severities)"
    if out.get("verdict") == "PASS" and out.get("max_severity") in BLOCKING:
        return {}, f"a PASS with a {out['max_severity']} finding contradicts itself (N7)"
    candidate = raw.get("candidate", raw.get("head"))
    if candidate is not None:
        if not isinstance(candidate, str) or not SHA.fullmatch(candidate):
            return {}, "candidate is not a commit id (7-64 lowercase hex)"
        out["candidate"] = candidate
    for key in ("axis", "ticket", "evidence", "outcome"):
        value = raw.get(key)
        if value is not None:
            if not isinstance(value, str) or not value:
                return {}, f"{key} must be a non-empty string"
            out[key] = value
    if "axis" in out and out["axis"] not in c.AXES:
        return {}, f"axis {out['axis']!r} is not one of {c.AXES}"
    return out, None


# ---------------------------------------------------------------- first-hand reads (I/O, worker thread)


@dataclass
class FileEvidence:
    path: str
    sha256: str | None = None
    size: int | None = None
    mtime: float | None = None
    body: JsonObj | None = None
    problem: str | None = None
    retry: bool = False  # missing or unreadable now: read again next cycle (R2-I3)

    def source(self) -> JsonObj:
        return {"path": self.path, "sha256": self.sha256, "size": self.size, "mtime": self.mtime}


def read_bytes(path: str) -> tuple[bytes | None, os.stat_result | None, str | None]:
    """N1: O_NOFOLLOW|O_NONBLOCK|O_CLOEXEC, fstat the descriptor (regular file only), read at most
    MAX+1 bytes and refuse anything larger; size and mtime come from that fstat."""
    try:
        fd = os.open(path, _OPEN_READ)
    except FileNotFoundError:
        return None, None, "does not exist"
    except OSError as exc:
        return None, None, f"cannot open: {exc.strerror or exc}"
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            return None, None, "is not a regular file"
        chunks: list[bytes] = []
        left = MAX_FILE_BYTES + 1
        while left > 0:
            chunk = os.read(fd, left)
            if not chunk:
                break
            chunks.append(chunk)
            left -= len(chunk)
        data = b"".join(chunks)
    except OSError as exc:
        return None, None, f"unreadable: {exc.strerror or exc}"
    finally:
        os.close(fd)
    if len(data) > MAX_FILE_BYTES:
        return None, None, f"is larger than {MAX_FILE_BYTES} bytes"
    return data, st, None


def allowed_roots(run_dir: Path, batch: str | None, ticket: str, cwd: str | None) -> list[Path]:
    """N2: where a session's result file may live: its own cwd (as the host reports it for the thread),
    and ``<run>/evidence/<batch>/<ticket>/``. A cwd of /, the home directory or anything shallower than
    three levels is not a usable root."""
    roots: list[Path] = []
    if cwd and os.path.isabs(cwd):
        real = Path(os.path.realpath(cwd))
        if real != Path("/") and real != Path(os.path.realpath(Path.home())) and len(real.parts) >= 4:
            roots.append(real)
    if batch:
        roots.append(Path(os.path.realpath(Path(run_dir) / EVIDENCE_DIR / batch / ticket)))
    return roots


def read_claim_file(path: str, expected: str, run_dir: Path, roots: Sequence[Path],
                    ticket_evidence: Path | None) -> FileEvidence:
    ev = FileEvidence(path)
    if not os.path.isabs(path):
        ev.problem = "result file path is not absolute"
        return ev
    real = Path(os.path.realpath(path))
    if real.name in COORDINATOR_FILES:
        ev.problem = f"{real.name} is a coordinator record, not a session result"
        return ev
    if not any(real == root or real.is_relative_to(root) for root in roots):
        ev.problem = "result file is outside the session's cwd and the ticket's evidence dir"
        return ev
    run_real = Path(os.path.realpath(run_dir))
    if real.is_relative_to(run_real) and not (ticket_evidence is not None and real.is_relative_to(ticket_evidence)):
        ev.problem = "result file is in the run dir but not in this ticket's evidence dir"
        return ev
    data, st, problem = read_bytes(path)
    if problem or data is None or st is None:
        ev.problem = f"result file {problem}"
        ev.retry = bool(problem) and str(problem).startswith(RETRYABLE)
        return ev
    ev.sha256, ev.size, ev.mtime = hashlib.sha256(data).hexdigest(), int(st.st_size), round(st.st_mtime, 3)
    if ev.sha256 != expected:
        ev.problem = "result file content does not match the sha256 the session wrote"
        return ev
    try:
        body = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        ev.problem = "result file is not JSON"
        return ev
    if not isinstance(body, dict):
        ev.problem = "result file is not a JSON object"
        return ev
    ev.body = body
    return ev


def read_cli_result(run_file: str) -> tuple[FileEvidence, str | None]:
    """I4: only the sibling ``cli-result.json`` of the run file (``run.result`` is ignored), through the
    hardened reader."""
    path = str(Path(run_file).with_name(CLI_RESULT))
    ev = FileEvidence(path)
    data, st, problem = read_bytes(path)
    if problem or data is None or st is None:
        ev.problem = f"{CLI_RESULT} {problem}"
        ev.retry = bool(problem) and str(problem).startswith(RETRYABLE)
        return ev, None
    ev.sha256, ev.size, ev.mtime = hashlib.sha256(data).hexdigest(), int(st.st_size), round(st.st_mtime, 3)
    try:
        body = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        ev.problem = f"{CLI_RESULT} is not JSON"
        return ev, None
    text = body.get("resultText") if isinstance(body, dict) else None
    return ev, text if isinstance(text, str) else None


@dataclass
class Evidence:
    obs: Observation
    observed: str | None  # starting/running/unknown/finished; None: no observation fact
    status: str | None  # result.status when finished
    turn: str | None = None
    message_sha256: str | None = None
    claim: JsonObj | None = None  # the session's closing claim (message and/or verified file)
    problem: str | None = None  # why there is no usable claim (None when there simply is none)
    file: FileEvidence | None = None
    cli_result: FileEvidence | None = None
    origin: str = ""
    wait: str | None = None  # finished, but not recordable yet (no text, file missing): no fact this cycle

    def source(self) -> JsonObj:
        out: JsonObj = {"observer": "host-dag-host-watch", "provider": self.obs.provider,
                        "session": self.obs.session, "cwd": self.obs.cwd, "turn": self.turn,
                        "status": self.obs.status, "message_sha256": self.message_sha256}
        if self.cli_result is not None:
            out["cli_result"] = self.cli_result.source()
        if self.file is not None:
            out["file"] = self.file.source()
        if self.claim is not None:
            out["claim"] = self.origin
        return out


def _merge(a: JsonObj, b: JsonObj) -> tuple[JsonObj, str | None]:
    merged = dict(a)
    for key, value in b.items():
        if key in merged and merged[key] != value:
            return {}, f"message and result file disagree on {key}"
        merged[key] = value
    return merged, None


def gather(obs: Observation, run_dir: Path, world: c.World, *, fixed: bool = False) -> Evidence:
    """Everything this observation needs from disk, read first-hand now (worker thread). ``fixed``: this
    turn's result is already recorded in the core, so no claim or file is read again (N-b)."""
    text = obs.text
    turn = obs.turn
    cli_ev: FileEvidence | None = None
    if obs.provider in CLI_PROVIDERS:
        observed = CLI_OBSERVED.get(obs.status, "unknown")
        status = CLI_STATUS.get(obs.status, "failed") if observed == "finished" else None
        turn = CLI_TURN
        if observed == "finished" and status != "completed":
            return Evidence(obs, observed, status, turn)  # LOST/START_FAILED/... write no cli-result.json (review R3-I1)
        if observed == "finished" and not fixed:
            cli_ev, text = read_cli_result(obs.session)
            if cli_ev.problem:
                ev = Evidence(obs, observed, status, turn, problem=cli_ev.problem, cli_result=cli_ev)
                ev.wait = cli_ev.problem if cli_ev.retry else None
                return ev
    elif obs.provider == "grok":
        return Evidence(obs, None, None, problem="grok sessions: facts not mirrored (retired provider)")
    else:
        if obs.healed:
            return Evidence(obs, None, None, problem="failed turn re-sent by the watcher; not a finish")
        if obs.error:
            return Evidence(obs, "unknown", None, turn, problem=f"session unreadable: {obs.error}")
        observed = THREAD_OBSERVED.get(obs.status, "unknown")
        status = obs.status if observed == "finished" else None
    ev = Evidence(obs, observed, status, turn, cli_result=cli_ev)
    if observed != "finished" or fixed or status != "completed":
        return ev  # a failed or cancelled turn carries no claim; record it as finished now (review R3-I1)
    if text is None or not text.strip():
        ev.wait = "terminal read without the final message text; waiting for the next read"
        return ev
    ev.message_sha256 = hashlib.sha256(text.encode("utf-8")).hexdigest()
    marks = parse_markers(text)
    if marks.problem:
        ev.problem = marks.problem
        return ev
    merged: JsonObj = {}
    origin: list[str] = []
    if marks.claim is not None:
        merged, problem = normalize_claim(marks.claim)
        if problem:
            ev.problem = f"message claim: {problem}"
            return ev
        origin.append("message")
    if marks.file is not None and marks.file_sha256 is not None:
        ticket, _, _ = obs.ticket_role()
        t = world.tickets.get(ticket)
        batch = t.batch if t is not None else None
        roots = allowed_roots(run_dir, batch, ticket, obs.cwd if obs.provider not in CLI_PROVIDERS else None)
        evidence_dir = Path(os.path.realpath(Path(run_dir) / EVIDENCE_DIR / batch / ticket)) if batch else None
        ev.file = read_claim_file(marks.file, marks.file_sha256, run_dir, roots, evidence_dir)
        if ev.file.problem or ev.file.body is None:
            ev.problem = ev.file.problem or "result file unreadable"
            ev.wait = ev.problem if ev.file.retry else None  # R2-I3: retry until the pinned bytes are there
            return ev
        from_file, problem = normalize_claim(ev.file.body)
        if problem:
            ev.problem = f"result file: {problem}"
            return ev
        merged, problem = _merge(merged, from_file)
        if problem:
            ev.problem = problem
            return ev
        origin.append("file")
    if origin:
        ev.origin = "+".join(origin)
        ev.claim = merged
    return ev


# ---------------------------------------------------------------- facts (pure)


@dataclass
class Note:
    key: str
    reason: str
    ticket: str | None = None
    execution_id: str | None = None
    alert: bool = False  # a finished result without an accepted claim: alert once per episode (R2-I3)

    def to_json(self) -> JsonObj:
        return {"kind": "withheld", "key": self.key, "ticket": self.ticket, "execution_id": self.execution_id,
                "reason": self.reason}


@dataclass
class Plan:
    ops: list[c.Op] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)


def _op(kind: str, ticket: str, revision: int, at: str, payload: JsonObj) -> c.Op:
    """op_id = digest of (kind, ticket, revision, payload): the same observation of the same record is a
    replay (no new row); any change of the record or of the observation is a new op."""
    digest = hashlib.sha256(c.canonical([kind, ticket, revision, payload]).encode()).hexdigest()[:32]
    return c.Op(f"watch-{digest}", kind, "watcher", at, ticket, None, None, payload)


def _result_turns(e: c.Execution) -> set[str]:
    turns: set[str] = set()
    for res in [e.result, *[h.get("result") for h in e.history]]:
        if isinstance(res, dict) and isinstance(res.get("turn"), str):
            turns.add(str(res["turn"]))
    return turns


def _segment(e: c.Execution, turn: str | None, observed: str) -> tuple[int | None, str | None]:
    """Design §3.1 / review R2: facts must name the segment they belong to. The watcher knows the turn it
    read; the previous segment's result recorded its turn. Same turn -> that older segment (stale)."""
    if e.segment == 0:
        return 0, None
    prev = e.history[-1] if e.history else {}
    prev_result = prev.get("result") if isinstance(prev, dict) else None
    prev_turn = prev_result.get("turn") if isinstance(prev_result, dict) else None
    if turn and isinstance(prev_turn, str) and turn == prev_turn:
        return e.segment - 1, None
    if turn and (observed == "running" or isinstance(prev_turn, str)):
        return e.segment, None
    return None, "cannot tell which segment this turn belongs to (no turn id recorded for the previous one)"


def session_key(key: str, session: str) -> str:
    return f"{key}|{session}"


def _turn_problem(t: c.Ticket, e: c.Execution, turn: str | None, baseline: Json) -> str | None:
    """R2-I1: is this terminal turn really this execution's result? ``baseline`` is what the watcher saw
    when it first saw the session under this key: {"turn": <last terminal turn or None>} (None: never seen
    readable, so nothing can be bound yet)."""
    if not turn:
        return "the terminal turn has no id; it cannot be bound to this execution"
    for other in t.executions:
        if other is not e and other.session == e.session and turn in _result_turns(other):
            return f"turn {turn} is the result of an earlier execution ({other.execution_id}) of this session"
    if not isinstance(baseline, dict):
        return "the watcher has not seen this session readable under this key yet"
    if baseline.get("turn") == turn:
        return f"turn {turn} was already terminal when the watcher first saw this session"
    return None


def _claimed_result(ev: Evidence, role: str) -> JsonObj:
    out: JsonObj = {"status": ev.status or "failed"}
    if ev.turn:
        out["turn"] = ev.turn
    claim = ev.claim or {}
    if ev.status == "completed":
        keys = ("candidate", "evidence", "outcome") if role == "executor" else ("verdict", "max_severity", "candidate")
        for key in keys:
            if claim.get(key) is not None:
                out[key] = claim[key]
        if not any(k in out for k in keys):  # fixed without an accepted claim: say why, durably
            out["claim_problem"] = ev.problem or "no HOST-DAG-RESULT claim closes the final message"
    return out


def fixed_turn(world: c.World, obs: Observation) -> bool:
    """The core already holds a finished result for this session's turn: it is not read again."""
    ticket, _, _ = obs.ticket_role()
    t = world.tickets.get(ticket)
    turn = CLI_TURN if obs.provider in CLI_PROVIDERS else obs.turn
    return t is not None and turn is not None and any(
        x.session == obs.session and x.observed == "finished" and isinstance(x.result, dict)
        and x.result.get("turn") == turn for x in t.executions)


def plan(world: c.World, evidences: Sequence[Evidence], parent_thread: str | None,
         sessions: Mapping[str, Json] | None = None) -> Plan:
    """Facts for this cycle from first-hand evidence and the core's current records. Pure. ``sessions``:
    the watcher's first sight of each (key, session) (see ``first_sight``)."""
    out = Plan()
    seen = sessions or {}
    executor_sessions = {e.session for t in world.tickets.values() for e in t.executions
                         if e.role == "executor" and e.session}
    for ev in evidences:
        obs = ev.obs
        ticket, role, axis = obs.ticket_role()
        if role == "unknown":
            out.notes.append(Note(obs.key, "tracked review key does not end in a known axis", ticket))
            continue
        t = world.tickets.get(ticket)
        if t is None:
            out.notes.append(Note(obs.key, "ticket not in the core", ticket))
            continue
        same = [x for x in t.executions if x.session == obs.session]
        e = next((x for x in reversed(same) if x.role == role and (role != "reviewer" or x.axis == axis)), None)
        if e is None and same:
            x = same[-1]
            out.notes.append(Note(obs.key, f"tracked as {role}/{axis or '-'} but the core has {x.role}/"
                                           f"{x.axis or '-'}", ticket, x.execution_id))
            continue
        if e is None:
            out.notes.append(Note(obs.key, "session not recorded in the core (adopt/exec request first)", ticket))
            continue
        if parent_thread and obs.session == parent_thread:
            out.notes.append(Note(obs.key, "the session is the coordinator's own thread", ticket, e.execution_id))
            continue
        if role == "reviewer" and obs.session in executor_sessions:
            out.notes.append(Note(obs.key, "the reviewer session is also recorded as an executor", ticket,
                                  e.execution_id))
            continue
        if role == "reviewer" and any(x is not e and x.role == "reviewer" and x.session == e.session
                                      and x.axis != e.axis and x.candidate_digest == e.candidate_digest
                                      for x in t.executions):
            out.notes.append(Note(obs.key, "one session reviews both axes of this candidate (N6)", ticket,
                                  e.execution_id))
            continue
        if ev.observed is None:
            out.notes.append(Note(obs.key, ev.problem or "no observation", ticket, e.execution_id))
            continue
        segment: int | None = None
        if role == "executor":
            segment, why = _segment(e, ev.turn, ev.observed)
            if why:
                out.notes.append(Note(obs.key, why, ticket, e.execution_id))
                continue
        current_segment = segment is None or segment == e.segment
        payload: JsonObj = {"execution_id": e.execution_id, "observed": ev.observed, "session": obs.session,
                            "source": ev.source()}
        if segment is not None:
            payload["segment"] = segment
        recorded: JsonObj | None = None
        if ev.observed == "finished" and current_segment:
            why = _turn_problem(t, e, ev.turn, seen.get(session_key(obs.key, obs.session)))
            if why:
                out.notes.append(Note(obs.key, why, ticket, e.execution_id, alert="already terminal when" in why))
                continue
            if e.observed == "finished" and isinstance(e.result, dict) and e.result.get("turn") == ev.turn:
                recorded = e.result  # this turn's result is fixed: nothing is completed or retried later
                if recorded.get("claim_problem"):
                    out.notes.append(Note(obs.key, f"finished without an accepted claim: {recorded['claim_problem']}",
                                          ticket, e.execution_id, alert=True))
            elif ev.wait:
                out.notes.append(Note(obs.key, f"waiting: {ev.wait}", ticket, e.execution_id, alert=True))
                continue  # R2-I3: nothing is recorded from this read; the next read decides
        claim: JsonObj = dict(ev.claim or {})
        if ev.problem and recorded is None:
            out.notes.append(Note(obs.key, f"no verdict/candidate: {ev.problem}", ticket, e.execution_id,
                                  alert=ev.observed == "finished"))
        if claim.get("ticket") not in (None, ticket):
            out.notes.append(Note(obs.key, f"claim names ticket {claim.get('ticket')}", ticket, e.execution_id))
            claim, ev.claim = {}, None
        if role == "reviewer" and claim.get("axis") not in (None, axis):
            out.notes.append(Note(obs.key, f"claim names axis {claim.get('axis')}", ticket, e.execution_id))
            claim, ev.claim = {}, None
        if role == "reviewer" and "verdict" in claim and "candidate" not in claim:
            out.notes.append(Note(obs.key, "a reviewer claim must name the candidate it reviewed", ticket,
                                  e.execution_id))
            claim, ev.claim = {}, None
        if recorded is not None:
            claim = {k: recorded[k] for k in CLAIM_KEYS if k in recorded}
        elif ev.observed == "finished":
            payload["result"] = _claimed_result(ev, role)
        unchanged = current_segment and e.observed == ev.observed and (
            recorded is not None if ev.observed == "finished" else True)
        if not unchanged:
            out.ops.append(_op("execution.observed", ticket, t.revision, obs.at, payload))
        if role != "reviewer" or ev.observed != "finished" or ev.status != "completed" or "verdict" not in claim:
            continue
        review = next((x for x in reversed(t.reviews) if x.execution_id == e.execution_id), None)
        if review is None:
            out.notes.append(Note(obs.key, "no review request bound to this reviewer", ticket, e.execution_id))
            continue
        digest = str(claim["candidate"])  # must name the bound candidate, or the core records stale
        if digest != review.candidate_digest and len(digest) >= 7 and review.candidate_digest.startswith(digest):
            digest = review.candidate_digest  # R2-I2: a unique prefix of the bound digest is that digest
        verdict, severity = claim["verdict"], claim.get("max_severity")
        if review.verdict is not None and (review.verdict, review.max_severity) == (verdict, severity) \
                and digest == review.candidate_digest:
            continue
        rv: JsonObj = {"review_request_id": review.review_request_id, "candidate_digest": digest,
                       "axis": review.axis, "verdict": verdict, "source": ev.source()}
        if severity is not None:
            rv["max_severity"] = severity
        out.ops.append(_op("review.observed", ticket, t.revision, obs.at, rv))
    return out


def first_sight(sessions: MutableMapping[str, Json], observations: Sequence[Observation]) -> bool:
    """R2-I1: the first time the watcher reads a session under a tracked key, remember its last terminal
    turn (None when it is running or fresh). Recorded before anything else in the cycle, even when this
    cycle's mirror is skipped later, so the baseline is the watcher's own and as early as possible."""
    changed = False
    for obs in observations:
        k = session_key(obs.key, obs.session)
        if k in sessions or obs.error or obs.healed or obs.provider == "grok":
            continue
        turn: str | None = CLI_TURN if obs.provider in CLI_PROVIDERS else obs.turn
        table = CLI_OBSERVED if obs.provider in CLI_PROVIDERS else THREAD_OBSERVED
        terminal = table.get(obs.status) == "finished"
        sessions[k] = {"turn": turn if terminal else None, "at": obs.at}
        changed = True
    if len(sessions) > SESSIONS_KEEP:
        def at(k: str) -> str:
            v = sessions[k]
            return str(v.get("at")) if isinstance(v, dict) else ""
        for k in sorted(sessions, key=at)[:len(sessions) - SESSIONS_KEEP]:
            del sessions[k]
    return changed


# ---------------------------------------------------------------- the bounded mirror (worker thread)


@dataclass
class Report:
    status: str  # absent | disabled | quiesced | skipped | disk_low | mode | busy | error | slow | ok
    detail: str = ""
    submitted: int = 0
    receipts: list[JsonObj] = field(default_factory=list)


class _Run:
    def __init__(self, run_dir: Path, memory: MutableMapping[str, Json]) -> None:
        self.run_dir = Path(run_dir)
        self.sm = self.run_dir / ctl.SM_DIR
        self.memory = memory
        self.at = datetime.now().astimezone().isoformat(timespec="seconds")
        self.rows: list[JsonObj] = []

    def files(self) -> ctl.SmFiles:
        return ctl.SmFiles(self.sm)

    def quiesced(self) -> bool:
        try:
            return (self.sm / c.QUIESCE_MARKER).exists()
        except OSError:
            return True

    def load_memory(self) -> None:
        """A one-shot `poll` process starts with empty memory: take the episode from sm/ (best effort)."""
        if self.memory.get("loaded"):
            return
        self.memory["loaded"] = True
        saved = self.files().read_json(WATCH_STATE) or {}
        for key in MEMORY_KEYS:
            if key in saved and key not in self.memory:
                self.memory[key] = saved[key]

    def save_memory(self, before: str) -> None:
        if self.snapshot() != before:
            self.files().write_json(WATCH_STATE, self.state())

    def state(self) -> JsonObj:
        return {key: self.memory.get(key) for key in MEMORY_KEYS}

    def snapshot(self) -> str:
        return c.canonical(self.state())

    def episode(self, status: str, detail: str, *, write: bool = True) -> Report:
        """At most one alert per episode: a new reason starts a new episode; the same one stays quiet."""
        if self.memory.get("episode") != status:
            self.memory["episode"] = status
            if write:
                self.alert(status, detail)
        return Report(status, detail)

    def alert(self, status: str, detail: str) -> None:
        if self.quiesced():
            return  # N5: nothing under a quiesced sm/
        try:
            self.files().append(ctl.ALERTS_LOG, [{"at": self.at, "kind": f"watch_mirror_{status}", "source": "watcher",
                                                  "detail": detail[:2000]}])
        except Exception:  # noqa: BLE001 - best effort, like every write here
            pass

    def strings(self, key: str) -> list[str]:
        value = self.memory.get(key)
        return [x for x in value if isinstance(x, str)] if isinstance(value, list) else []


def mirror(run_dir: Path, raw_observations: Sequence[Mapping[str, object]], parent_thread: str | None,
           memory: MutableMapping[str, Json], *, budget: float = 2.0, overruns: int = 0,
           statvfs: Callable[[str], r._Statvfs] = os.statvfs) -> Report:
    """One cycle's mirror (see module doc). Never raises."""
    started = time.monotonic()
    run = _Run(run_dir, memory)
    try:
        return _mirror(run, raw_observations, parent_thread, budget, overruns, statvfs, started)
    except Exception as exc:  # noqa: BLE001 - the watcher loop must never see it
        return run.episode("error", f"{type(exc).__name__}: {exc}")


def _mirror(run: _Run, raw_observations: Sequence[Mapping[str, object]], parent_thread: str | None,
            budget: float, overruns: int, statvfs: Callable[[str], r._Statvfs], started: float) -> Report:
    if os.environ.get(cli.ENV_MIRROR) == "0":
        return Report("disabled")
    db = run.sm / ctl.DB_NAME
    if not db.is_file():
        return Report("absent")
    if run.quiesced():
        return run.episode("quiesced", f"{run.sm / c.QUIESCE_MARKER} exists", write=False)
    run.load_memory()
    before = run.snapshot()
    known = run.memory.get("sessions")
    sessions: dict[str, Json] = dict(known) if isinstance(known, dict) else {}
    first_sight(sessions, [o for o in (Observation.load(x) for x in raw_observations) if o is not None])
    run.memory["sessions"] = sessions
    report = _mirror_open(run, raw_observations, parent_thread, budget, overruns, statvfs, started)
    elapsed = time.monotonic() - started
    if report.status == "ok" and elapsed > budget:
        report = run.episode("slow", f"mirror took {elapsed:.2f}s > budget {budget:g}s; the watcher did not wait")
    elif report.status == "ok" and run.memory.get("episode"):
        run.rows.append({"at": run.at, "kind": "recovered", "episode": run.memory.get("episode")})
        run.memory["episode"] = None
    if run.quiesced():
        return report  # quiesced meanwhile: write nothing more
    try:
        run.files().append(WATCH_LOG, run.rows)
        run.save_memory(before)
    except Exception:  # noqa: BLE001
        pass
    return report


def _mirror_open(run: _Run, raw_observations: Sequence[Mapping[str, object]], parent_thread: str | None,
                 budget: float, overruns: int, statvfs: Callable[[str], r._Statvfs], started: float) -> Report:
    if overruns:
        run.episode("slow", f"{overruns} cycle(s) skipped: the previous mirror was still running")
    problem = cli.interpreter_problem()
    if problem:
        return run.episode("skipped", problem)
    try:
        disk = r.disk_status(run.sm, statvfs)
    except OSError as exc:
        return run.episode("skipped", f"statvfs: {exc}")
    if not disk.ok:
        return run.episode("disk_low", disk.reason)
    observations = [o for o in (Observation.load(x) for x in raw_observations) if o is not None]
    try:
        conn = c.connect(run.sm / ctl.DB_NAME, cli.run_id_for(run.run_dir), busy_timeout_ms=MIRROR_BUSY_MS)
    except c.QuiescedError as exc:
        return run.episode("quiesced", str(exc), write=False)
    except Exception as exc:  # noqa: BLE001
        return run.episode("error", f"{type(exc).__name__}: {exc}")
    report = Report("ok")
    try:
        store = c.SqliteStore(conn, max_wait_seconds=min(0.5, budget / 2))
        world = store.world()
        if world.meta.mode == "legacy":
            return run.episode("mode", "core authority mode is legacy: db submissions are frozen (§8 C4)")
        evidences = [gather(o, run.run_dir, world, fixed=fixed_turn(world, o)) for o in observations]
        if time.monotonic() - started > budget:
            return run.episode("slow", "reading the sessions' result files used the whole budget")
        waits = run.memory.get("waits") if isinstance(run.memory.get("waits"), dict) else {}
        now = time.time()
        for ev in evidences:
            if not ev.wait:
                continue
            key = f"{session_key(ev.obs.key, ev.obs.session)}|{ev.turn}"
            since = waits.setdefault(key, now)
            if now - float(since) > WAIT_LIMIT_SECONDS:
                ev.problem, ev.wait = f"timed out waiting for {ev.wait}", None
        run.memory["waits"] = dict(sorted(waits.items(), key=lambda kv: kv[1])[-SESSIONS_KEEP:])
        sessions = run.memory.get("sessions")
        planned = plan(world, evidences, parent_thread, sessions if isinstance(sessions, dict) else {})
        _log_notes(run, planned.notes)
        refused = run.strings("refused")
        for op in [o for o in planned.ops if o.op_id not in refused][:MAX_OPS_PER_CYCLE]:
            if time.monotonic() - started > budget:
                run.rows.append({"at": run.at, "kind": "deferred", "detail": "budget used; rest next cycle"})
                break
            row: JsonObj = {"at": run.at, "kind": op.kind, "ticket": op.ticket, "op_id": op.op_id,
                            "payload": op.payload}
            try:
                receipt = store.submit(op)
            except c.BusyError as exc:
                return run.episode("busy", str(exc))
            except (c.Rejected, c.ValidationError, c.OpConflict, c.InvariantError) as exc:
                row.update(status="refused", detail=f"{type(exc).__name__}: {exc}")
                run.rows.append(row)
                refused = (refused + [op.op_id])[-REFUSED_KEEP:]  # not re-submitted every 30 s
                run.memory["refused"] = list(refused)
                continue
            except c.StorageError as exc:
                return run.episode("error", f"{type(exc).__name__}: {exc}")
            report.submitted += 1
            report.receipts.append(receipt.to_json())
            if not receipt.replayed:
                row.update(status=receipt.status, detail=receipt.detail, revision=receipt.revision)
                run.rows.append(row)
    finally:
        conn.close()
    return report


def _log_notes(run: _Run, notes: Sequence[Note]) -> None:
    """A withheld observation is logged when its reason changes, not every 30 s."""
    seen = run.memory.get("withheld")
    withheld: dict[str, Json] = dict(seen) if isinstance(seen, dict) else {}
    current: dict[str, Json] = {}
    for note in notes:
        current[note.key] = note.reason
        if withheld.get(note.key) != note.reason:
            run.rows.append({"at": run.at, **note.to_json()})
            if note.alert:  # a finished result without an accepted claim: once per (key, reason)
                run.alert("claim_withheld", f"{note.key}: {note.reason}")
    run.memory["withheld"] = current

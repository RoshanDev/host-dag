"""Read-only views over a Host DAG run: ticket states, progress, dependency tree, executor timelines.

Ticket records for new batches live in ``run.json["batches"][<batch>]["tickets"]`` and are written
only by the coordinator through ``host-dag-view.py ticket set``. Everything else here reads.
Executor timelines come from Codex rollout files (``~/.codex/sessions/**/rollout-*-<thread>.jsonl``).
"""

from __future__ import annotations

from hostdag_json import public_path, read_json, atomic_json

import fcntl
import copy
import glob
import hashlib
import json
import math
import uuid
import os
import re
import subprocess
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import hostdag_ticket_state as ts
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import hostdag_ticket_state as ts
sys.path.insert(0, str(Path(__file__).resolve().parent / "local-dev"))
import board_freeze as freeze_gate

from hostdag_advisor_log import is_final_answer, is_provisional_answer, validate_record

# Ticket states and Execution Outcomes from the dag skill. An unclaimed ticket has no status.
STATUSES = ("CLAIMED", "ACCEPTED", "SUPERSEDED")
OUTCOMES = ("READY_FOR_ACCEPTANCE", "NEEDS_DECISION", "EXTERNALLY_BLOCKED")
REVIEW_AXES = ("spec", "standards")
VERDICTS = ("PASS", "FAIL")

# display key -> (glyph, label)
DISPLAY = {
    "ok": ("✔", "已验收"),
    "cand": ("◆", "待验收"),
    "run": ("▶", "执行中"),
    "ask": ("?", "需决定"),
    "hold": ("⏸", "外部阻塞"),
    "ready": ("○", "可执行"),
    "res": ("⧗", "等资源"),
    "wait": ("·", "等前置"),
    "fail": ("✖", "失败"),
    "sup": ("⊘", "作废"),
    "left": ("↪", "转遗留"),
    "move": ("→", "移至"),
    "sched": ("◷", "已排期"),
}
OUTCOME_DISPLAY = {"READY_FOR_ACCEPTANCE": "cand", "NEEDS_DECISION": "ask", "EXTERNALLY_BLOCKED": "hold"}

SESSIONS_ROOT = Path(os.environ.get("HOST_DAG_CODEX_SESSIONS", str(Path.home() / ".codex" / "sessions")))
THREAD_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class ViewError(Exception):
    pass


# ---------------------------------------------------------------- run receipt


def load_json(path: Path) -> dict[str, Any]:
    try:
        data = read_json(path)
    except FileNotFoundError as exc:
        raise ViewError(f"not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ViewError(f"not valid JSON: {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ViewError(f"expected an object: {path}")
    return data


def batches(run: dict[str, Any]) -> dict[str, dict[str, Any]]:
    value = run.get("batches")
    return value if isinstance(value, dict) else {}


EXECUTOR_PREFIXES = ("codex:", "claude:")


def bare_executor(value: Any) -> Any:
    """A Codex thread id or CLI unit name without the provider prefix the board sometimes adds."""
    if isinstance(value, str):
        for prefix in EXECUTOR_PREFIXES:
            if value.startswith(prefix):
                return value[len(prefix):]
    return value


def normalize_executors(run: dict[str, Any]) -> None:
    """Strip "codex:"/"claude:" from executor and review thread ids in the loaded snapshot (never written back).

    2026-11-01: coordinators began writing "codex:<thread>" and "claude:<cli unit>"; the rollout and CLI log
    lookups expect the bare id, so every such running ticket showed no log."""
    for batch in batches(run).values():
        for t in (batch.get("tickets") or {}).values():
            if not isinstance(t, dict):
                continue
            if "executor" in t:
                t["executor"] = bare_executor(t["executor"])
            for review in (t.get("reviews") or {}).values():
                if isinstance(review, dict) and "thread" in review:
                    review["thread"] = bare_executor(review["thread"])


def ticket_order(batch: dict[str, Any]) -> list[str]:
    """Dependency order (Kahn), ties broken by ticket id, so prerequisites always come first."""
    tickets = batch.get("tickets") or {}
    remaining = {tid: [d for d in (t.get("deps") or []) if d in tickets] for tid, t in tickets.items()}
    out: list[str] = []
    while remaining:
        ready = sorted(tid for tid, deps in remaining.items() if all(d in out for d in deps))
        if not ready:  # a cycle; show the rest rather than hide it
            ready = sorted(remaining)
        for tid in ready:
            out.append(tid)
            remaining.pop(tid)
    return out


LIVE_MARK = "_live"  # in-memory only: the watcher tracks a running executor for this ticket
LOCK_WAIT_MARK = "_lock_wait"  # read-only queue snapshot, never persisted


def mark_lock_waits(run: dict[str, Any], run_dir: Path, now: float | None = None) -> None:
    """Join declared heavy-slot waits to the lock owner's waiter receipts."""
    now = time.time() if now is None else now
    locks = Path(run_dir) / "control/locks"
    cache = {}
    for batch in batches(run).values():
        for tid, t in (batch.get("tickets") or {}).items():
            if not isinstance(t, dict):
                continue
            t.pop(LOCK_WAIT_MARK, None)
            kind, _, name = str(t.get("wait_on") or "").partition(":")
            if ts.derive_state(t) not in ts.WAITING or kind != "heavy-slot" \
                    or not name or name in {".", ".."} or Path(name).name != name:
                continue
            if name not in cache:
                rows = []
                aliases = {}
                canonical = aliases.get(name, name)
                related = {canonical} | {old for old, new in aliases.items() if new == canonical}
                for path in (p for lock_name in related for p in (locks / f'{lock_name}.waiters').glob('*.json')):
                    try:
                        row = load_json(path)
                        queued = float(row["queued_at"])
                        if not math.isfinite(queued):
                            continue
                        rows.append((queued, path.name, row))
                    except (OSError, ViewError, KeyError, TypeError, ValueError):
                        continue
                try:
                    holder = load_json(locks / f"{name}.holder.json")
                except (OSError, ViewError):
                    holder = {}
                platform_holders = []
                aliases = {}
                canonical = aliases.get(name, name)
                if False:
                    for record in (locks / (canonical + '.holders')).glob('*.json'):
                        try:
                            platform_holders.append(load_json(record).get('ticket') or '—')
                        except (OSError, ViewError):
                            continue
                if platform_holders:
                    holder = {**holder, 'shared_tickets': sorted(platform_holders)}
                cache[name] = (sorted(rows, key=lambda r: (r[0], r[1])), holder)
            rows, holder = cache[name]
            for pos, (queued, _, row) in enumerate(rows, 1):
                if row.get("ticket") == tid:
                    text = f"排队等锁·{name}·第 {pos} 位·已等 {max(0, int((now - queued) / 60))} 分钟"
                    if "memory-wait" in str(holder.get("phase") or ""):
                        text += f"·持锁者 {holder.get('ticket') or '—'} memory-wait"
                    if holder.get('shared_tickets'):
                        text += '·平台持有者 ' + ', '.join(holder['shared_tickets'])
                    t[LOCK_WAIT_MARK] = text
                    break


def mark_live(run: dict[str, Any], watch: dict[str, Any]) -> None:
    """Flag tickets whose executor the watcher still tracks as running (watch.json `tracked`, not terminal)."""
    live = {k for k, rec in (watch.get("tracked") or {}).items()
            if isinstance(rec, dict) and not rec.get("terminal") and WATCH_REVIEW_MARK not in k}
    for batch in batches(run).values():
        for tid, t in (batch.get("tickets") or {}).items():
            if isinstance(t, dict):
                t[LIVE_MARK] = tid in live


# ---------------------------------------------------------------- flow v2 (control/flow-rules-20261022.md)

LEFTOVER_LEDGER = Path("control/leftover-ledger.jsonl")
CANDIDATE_SET = Path("control/candidate-set.json")
LEFTOVER_MARK = "_leftover"  # in-memory only: the ticket has rows in the leftover ledger
MOVED_MARK = "_moved_to"  # in-memory only: latest effective batch containing the same ticket id
ROUND_BUDGET = 3  # flow rules §3: first implementation + 2 corrections
CLOSURE_KEYS = ("closure", "closed", "outcome")
CLOSURE_LABELS = {"CLOSED-UNFINISHED": "CLOSED-UNFINISHED", "CLOSED-SCOPED": "CLOSED-SCOPED",
                  "UNVERIFIED-BATCH": "UNVERIFIED-BATCH", "ACCEPTED": "验收通过", "PASS": "验收通过",
                  "PASSED": "验收通过", "验收通过": "验收通过"}


def load_leftovers(run_dir: Path | None) -> list[dict[str, Any]]:
    """Rows of <run>/control/leftover-ledger.jsonl; a missing file or a bad line reads as nothing."""
    if run_dir is None:
        return []
    from hostdag_view_cache import jsonl_rows
    try:
        return jsonl_rows(Path(run_dir) / LEFTOVER_LEDGER)
    except OSError:
        return []


def leftover_ids(row: dict[str, Any]) -> set[str]:
    """Tickets a ledger row is about: its `source` and/or `ticket` field."""
    return {str(row[k]) for k in ("source", "ticket") if isinstance(row.get(k), str) and row.get(k)}


def leftover_text(row: dict[str, Any]) -> str:
    """One-line description: `description`, else the nested `finding` title/description, else the category."""
    finding = row.get("finding") if isinstance(row.get("finding"), dict) else {}
    for value in (row.get("description"), row.get("title"), finding.get("title"), finding.get("description"),
                  row.get("category")):
        if value:
            return str(value)
    return "转入遗留清单"


def mark_leftovers(run: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """Flag tickets named by a ledger row with that row's description (the latest row wins)."""
    named: dict[str, str] = {}
    for row in rows:
        for tid in leftover_ids(row):
            named[tid] = leftover_text(row)
    for batch in batches(run).values():
        for tid, t in (batch.get("tickets") or {}).items():
            if isinstance(t, dict):
                if tid in named:
                    t[LEFTOVER_MARK] = named[tid]
                else:
                    t.pop(LEFTOVER_MARK, None)


def mark_moved(run: dict[str, Any]) -> None:
    """Record the newest effective destination for each ticket, without changing the run receipt."""
    locations: dict[str, list[tuple[float, str]]] = {}
    for name, batch in batches(run).items():
        for tid, t in (batch.get("tickets") or {}).items():
            if ts.derive_state(t) == ts.CANCELED:
                continue
            # Prefer the ticket's update time; older receipts only timestamp the batch.
            try:
                updated = datetime.fromisoformat(str(t.get("updated_at") or batch.get("updated_at"))).timestamp()
            except ValueError:
                updated = float("-inf")
            locations.setdefault(tid, []).append((updated, name))
    for destinations in locations.values():
        destinations.sort(key=lambda item: item[0], reverse=True)
    for name, batch in batches(run).items():
        for tid, t in (batch.get("tickets") or {}).items():
            t[MOVED_MARK] = next((n for _, n in locations.get(tid, []) if n != name), "")


def batch_leftovers(batch: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ledger rows whose source or ticket is one of this batch's tickets."""
    tickets = batch.get("tickets") or {}
    return [row for row in rows if leftover_ids(row) & set(tickets)]


def batch_closure(batch: dict[str, Any]) -> str | None:
    """The batch's closing result from closure/closed/outcome metadata, or None while it is open."""
    for key in CLOSURE_KEYS:
        value = batch.get(key)
        if isinstance(value, dict):
            value = next((value[k] for k in ("status", "state", "outcome", "result") if value.get(k)), None)
        if value in (None, "", False):
            continue
        if value is True:
            return "已收口"
        text = str(value).strip()
        return CLOSURE_LABELS.get(text.upper(), CLOSURE_LABELS.get(text, text))
    return None


_SHA = re.compile(r"^[0-9a-f]{7,64}$")


def _sha_of(value: Any) -> str | None:
    if isinstance(value, dict):
        value = next((value[k] for k in ("sha", "head", "tip", "commit") if value.get(k)), None)
    return str(value) if isinstance(value, str) and _SHA.match(value) else None


def candidate_set(run_dir: Path | None) -> dict[str, Any] | None:
    """{'branch': str, 'repos': [(name, short sha)]} from <run>/control/candidate-set.json; None when absent.

    The file records the cumulative candidate's per-repo shas (final-plan §6); accept a
    `shaTuple`/`repositories`/`repos`/`shas`/`heads` map or list, or repo -> sha at the top level, with the branch under branch/ref/name."""
    if run_dir is None:
        return None
    try:
        data = json.loads((Path(run_dir) / CANDIDATE_SET).read_text(encoding="utf-8"))
    except OSError:
        return None
    except ValueError:
        return {"branch": "", "repos": [], "error": "candidate-set.json 无法解析"}
    if not isinstance(data, dict):
        return {"branch": "", "repos": [], "error": "candidate-set.json 格式未知"}
    branch = next((str(data[k]) for k in ("branch", "ref", "name") if isinstance(data.get(k), str)), "")
    source = next((data[k] for k in ("shaTuple", "repos", "shas", "heads", "tips", "repositories") if data.get(k)), data)
    repos: list[tuple[str, str]] = []
    items = source.items() if isinstance(source, dict) else \
        [((x.get("repo") or x.get("name") or x.get("path") or "?"), x) for x in source if isinstance(x, dict)] \
        if isinstance(source, list) else []
    for name, value in items:
        sha = _sha_of(value)
        if sha:
            if not branch and isinstance(value, dict) and isinstance(value.get("branch"), str):
                branch = value["branch"]
            repos.append((Path(str(name)).name or str(name), sha[:8]))
    return {"branch": branch, "repos": repos}


def round_budget(t: dict[str, Any]) -> tuple[str, bool] | None:
    """('第 r/3 轮', warn) from `round`; completed/canceled tickets never warn."""
    try:
        rnd = int(t.get("round"))
    except (TypeError, ValueError):
        return None
    if rnd <= 0:
        return None
    warn = rnd > ROUND_BUDGET and ts.derive_state(t) not in ts.DONE | {ts.CANCELED}
    return f"第 {rnd}/{ROUND_BUDGET} 轮", warn


def display_state(batch: dict[str, Any], tid: str, everything: dict[str, Any] | None = None) -> tuple[str, str]:
    """(display key, phase) for one ticket, from its state (hostdag_ticket_state.derive_state).

    Phase is the small text after the label: 审查, r2 (second execution attempt), 限定/缩减, 已集成.
    """
    tickets = batch.get("tickets") or {}
    t = tickets[tid]
    everything = tickets if everything is None else everything
    state = ts.derive_state(t, everything)
    if state == ts.FROZEN_GATE:
        return "hold", "冻结至最新候选包"
    if state in ts.DEGRADE_STATES:
        return {ts.DEGRADING:('run','降级执行'),ts.READY_DEGRADE_A:('ready','窄修合同待派'),ts.CANDIDATE_BLOCKED:('hold','本候选阻断')}[state]
    pending, missing = ts.dependency_status(t, everything)
    if state in ts.WAITING and t.get(LOCK_WAIT_MARK):
        return "res", t[LOCK_WAIT_MARK]
    if missing and state in (ts.CREATED, ts.WAITING_DEPENDENCY):
        return "fail", "依赖不存在：" + ",".join(missing)
    if state == ts.CANCELED:
        if t.get(MOVED_MARK):
            return "move", t[MOVED_MARK]
        # SUPERSEDED/CANCELED also closes tickets moved to the leftover ledger.
        return ("left", "") if t.get(LEFTOVER_MARK) else ("sup", "")
    if state in (ts.CREATED, ts.WAITING_RESOURCE):
        # A follow-up created for later (stage A rule), or a resource wait parked until a set time
        # (2026-10-23: ARM-D2-SOAK, a timed observation, showed as 等资源·主机锁): not runnable before then.
        until = re.search(r"WAIT-UNTIL:(\S+)", str(t.get("blocker") or ""))
        when = parse_ts(until.group(1)) if until else 0.0
        if when > time.time():
            return "sched", datetime.fromtimestamp(when).strftime("%m-%d %H:%M")
    action = t.get("pending_action") or {}
    if action and not pending and state not in ts.DONE and not t.get(LIVE_MARK):
        # A wait the program released (stage A): the next move is the coordinator's.
        return "ready", {"apply_decision": "待执行裁定", "review_returned": "待父处理"}.get(action.get("kind"), "待派下一步")
    if t.get(LIVE_MARK) and state in (ts.CREATED, ts.WAITING_RESOURCE, ts.WAITING_DEPENDENCY):
        # The coordinator left a waiting state while the executor kept working (2026-10-15, KV17-05 r7):
        # trust the live session and say the written state is stale.
        return "run", "状态未更新"
    if state in ts.DONE:
        scope = ts.accept_scope(t)
        phase = "已集成" if state == ts.INTEGRATED else ""
        if scope in ("SCOPED", "REDUCED"):
            phase = ts.SCOPE_LABEL[scope] + ("·" + phase if phase else "")
        return "ok", phase
    if state == ts.FAILED:
        return "fail", ""
    if state == ts.NEEDS_USER:
        return "ask", ""
    if state == ts.EXTERNALLY_BLOCKED:
        return "hold", ""
    if state == ts.WAITING_RESOURCE:
        return "res", ""
    if state == ts.REVIEWING:
        return ("cand", "") if t.get("outcome") == "READY_FOR_ACCEPTANCE" else ("run", "审查")
    if state == ts.RUNNING:
        attempt = ts.attempt_of(t)
        return "run", f"r{attempt}" if attempt > 1 else ""
    if state == ts.WAITING_DEPENDENCY:
        return "wait", ",".join(pending) if ts.explicit_state(t) == ts.CREATED else ""
    return "ready", ""


@dataclass
class Progress:
    total: int
    implemented: int
    accepted: int
    counts: dict[str, int]
    superseded: int
    stages: dict[str, int]


def progress(batch: dict[str, Any], everything: dict[str, Any] | None = None) -> Progress:
    by_ticket = {tid: display_state(batch, tid, everything)[0] for tid in (batch.get("tickets") or {})}
    keys = list(by_ticket.values())
    stages = wait_stage_counts(batch, everything, by_ticket)
    counts = {k: keys.count(k) for k in DISPLAY}
    superseded = counts["sup"] + counts["left"] + counts["move"]
    total = len(keys) - superseded
    # A ticket under review has handed in a candidate (2026-10-23: V20 showed 已实现 = 已验收 with five in review).
    reviewing = sum(1 for tid, t in (batch.get("tickets") or {}).items()
                    if ts.derive_state(t, everything or batch.get("tickets") or {}) == ts.REVIEWING
                    and display_state(batch, tid, everything)[0] == "run")
    return Progress(total, counts["ok"] + counts["cand"] + reviewing, counts["ok"], counts, superseded, stages)


def tree_lines(batch: dict[str, Any]) -> list[tuple[str, str]]:
    """Indented dependency tree as (prefix + ticket id, ticket id). A ticket with several
    prerequisites expands under the first one only; later appearances end with '↑ 见上'."""
    tickets = batch.get("tickets") or {}
    order = ticket_order(batch)
    children = {tid: [c for c in order if tid in (tickets[c].get("deps") or [])] for tid in order}
    seen: set[str] = set()
    out: list[tuple[str, str]] = []

    def walk(tid: str, prefix: str, last: bool, root: bool) -> None:
        branch = "" if root else ("└─ " if last else "├─ ")
        if tid in seen:
            out.append((f"{prefix}{branch}{tid} ↑ 见上", tid))
            return
        seen.add(tid)
        out.append((f"{prefix}{branch}{tid}", tid))
        kids = children[tid]
        nxt = "" if root else prefix + ("   " if last else "│  ")
        for i, kid in enumerate(kids):
            walk(kid, nxt, i == len(kids) - 1, False)

    roots = [tid for tid in order if not [d for d in (tickets[tid].get("deps") or []) if d in tickets]]
    for tid in roots:
        walk(tid, "", True, True)
    for tid in order:  # anything only reachable through a cycle
        if tid not in seen:
            walk(tid, "", True, True)
    return out


def remote_state(batch: dict[str, Any]) -> tuple[str, str]:
    """('ok'|'warn'|'none', text) comparing the accepted integration tip with the remote readback."""
    tip = batch.get("accepted_tip")
    remote = batch.get("remote") or {}
    readback = remote.get("readback")
    if not tip or not remote:
        return "none", "远端 —"
    if readback == tip:
        return "ok", f"远端 ✓ 已同步 {tip[:7]}"
    behind = ""
    repo = batch.get("repo")
    if repo and readback:
        try:
            res = subprocess.run(["git", "-C", repo, "rev-list", "--count", f"{readback}..{tip}"],
                                 capture_output=True, text=True, timeout=5)
            if res.returncode == 0:
                behind = f" 落后 {res.stdout.strip()}"
        except (OSError, subprocess.SubprocessError):
            pass
    return "warn", f"远端 ⚠ 未同步{behind}"


def batch_done(batch: dict[str, Any]) -> bool:
    p = progress(batch)
    # A batch whose every ticket was superseded or moved to the leftover ledger is closed too
    # (2026-10-27: ARM-D5's only ticket was superseded and the batch never got its check).
    return (p.total > 0 or p.superseded > 0) and p.accepted == p.total


def ordered_batch_names(run: dict[str, Any]) -> list[str]:
    """Unfinished batches first (in recorded order), then finished ones."""
    items = list(batches(run).items())
    return [n for n, b in items if not batch_done(b)] + [n for n, b in items if batch_done(b)]


RECENT_DONE = 3


def batch_finished_at(batch: dict[str, Any]) -> float:
    """When a finished batch finished: its latest acceptance, as epoch seconds. Metadata updates
    (a title, a remote readback) don't count unless no ticket records when it was accepted."""
    stamps = [t.get("accepted_at") for t in (batch.get("tickets") or {}).values() if t.get("accepted_at")]
    stamps = stamps or [batch.get("updated_at")]
    best = 0.0
    for stamp in stamps:
        try:
            best = max(best, datetime.fromisoformat(str(stamp)).timestamp())
        except ValueError:
            continue
    return best


def visible_batch_names(run: dict[str, Any], show_all: bool = False, keep: str | None = None) -> list[str]:
    """Every unfinished batch plus the RECENT_DONE most recently finished; all of them with show_all.
    `keep` stays listed even when it would be hidden, so the board never loses the batch on screen."""
    names = ordered_batch_names(run)
    if show_all:
        return names
    all_batches = batches(run)
    done = sorted((n for n in names if batch_done(all_batches[n])),
                  key=lambda n: batch_finished_at(all_batches[n]), reverse=True)
    shown = set(done[:RECENT_DONE]) | {n for n in names if not batch_done(all_batches[n])} | ({keep} if keep else set())
    return [n for n in names if n in shown]


def batch_title(batch: dict[str, Any], root: Path | None = None) -> str:
    """The batch's Chinese name: its recorded title, else the H1 of its DAG doc without the DAG/code noise."""
    if batch.get("title"):
        return str(batch["title"])
    dag = batch.get("dag")
    if not dag:
        return ""
    path = Path(dag) if Path(dag).is_absolute() else (root or Path(__file__).resolve().parents[1]) / dag
    try:
        with path.open(encoding="utf-8") as fh:
            h1 = next((line[2:].strip() for line in fh if line.startswith("# ")), "")
    except OSError:
        return ""
    h1 = re.sub(r"[（(][A-Z0-9-]+[)）]", "", h1).replace("DAG", "")
    return re.sub(r"\s+", " ", h1).strip(" ：:")


# ---------------------------------------------------------------- known runs

RUNS_INDEX = Path(os.environ.get("HOST_DAG_RUNS_INDEX")
                  or Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "host-dag/runs.json")
# Scanning a runs root walks evidence trees that can take a minute on a USB disk; skip the bulky ones.
SCAN_SKIP = {"evidence", "artifacts", "logs", "scratch", "source", "cache", "control", "node_modules", ".git", "queue"}


def known_runs(index: Path | None = None) -> list[Path]:
    try:
        data = json.loads((index or RUNS_INDEX).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    return [Path(p) for p in data.get("runs", []) if isinstance(p, str)]


def register_run(path: Path, index: Path | None = None) -> bool:
    """Remember a run directory for `host-dag runs`. Returns True when it was new."""
    index = index or RUNS_INDEX
    path = public_path(path)
    runs = [str(p) for p in known_runs(index)]
    if str(path) in runs:
        return False
    index.parent.mkdir(parents=True, exist_ok=True)
    tmp = index.with_name(index.name + ".tmp")
    tmp.write_text(json.dumps({"runs": runs + [str(path)]}, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    os.replace(tmp, index)
    return True


def scan_runs(root: Path, max_depth: int = 6) -> list[Path]:
    """run.json files under root that carry `batches`, skipping evidence-like trees."""
    found: list[Path] = []
    root = Path(root)
    for dirpath, dirnames, filenames in os.walk(root):
        depth = len(Path(dirpath).relative_to(root).parts)
        dirnames[:] = [] if depth >= max_depth else [d for d in dirnames if d not in SCAN_SKIP]
        if "run.json" in filenames:
            try:
                if b'"batches"' in (Path(dirpath) / "run.json").read_bytes():
                    found.append(Path(dirpath))
            except OSError:
                continue
    return found


# Mirrors host-dag-watch.py: urgent kinds skip the quiet window; only review sessions settle.
WATCH_URGENT_KINDS = {"failed", "attention", "unknown_status", "rejected", "unanswered", "cancelled"}
WATCH_REVIEW_MARK = "-review-"


def wake_status(watch: dict[str, Any], coordinator_running: bool, now: float | None = None) -> str | None:
    """Why queued watcher events have not woken the coordinator yet, or None when nothing waits."""
    now = now or time.time()
    events = [e for e in watch.get("pending_events") or [] if isinstance(e, dict)]
    if not events:
        return None
    tickets = list(dict.fromkeys(str(e.get("ticket") or e.get("kind") or "?") for e in events))
    names = "、".join(tickets[:2]) + ("…" if len(tickets) > 2 else "")
    groups = {t.rsplit(WATCH_REVIEW_MARK, 1)[0] for t in tickets if WATCH_REVIEW_MARK in t}
    settle = float(watch.get("settle_seconds") or 0)
    if coordinator_running:
        reason = "协调者忙，空闲后送达"
    elif any(e.get("kind") in WATCH_URGENT_KINDS for e in events) or not groups or settle <= 0:
        reason = "下一轮检查送达（≤30s）"
    elif any(t.rsplit(WATCH_REVIEW_MARK, 1)[0] in groups and t not in tickets and not rec.get("terminal")
             for t, rec in (watch.get("tracked") or {}).items() if WATCH_REVIEW_MARK in t):
        reason = "等另一审查轴完成"
    else:
        stamps = [float(e["queued_at"]) for e in events if e.get("queued_at") is not None] or [now]
        due = min(max(stamps) + settle, min(stamps) + float(watch.get("max_hold_seconds") or 600))
        reason = f"静默期剩 {dur(due - now)}"
    return f"待唤醒 {len(events)}（{names}，{reason}）"


# ---------------------------------------------------------------- ticket writer


class RunLock:
    """Exclusive fcntl lock on run.json.lock; atomic replace on save."""

    def __init__(self, run_path: Path) -> None:
        self.run_path = run_path
        self.fd = -1

    def __enter__(self) -> RunLock:
        self.fd = os.open(str(self.run_path) + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc: object) -> None:
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)


def save_run(path: Path, run: dict[str, Any], expected_mtime: float) -> None:
    if path.stat().st_mtime != expected_mtime:
        raise ViewError("run.json changed while updating; retry")
    atomic_json(path, run)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def now_epoch() -> float:
    """Wall clock for stored epoch stamps (review registered_at); one seam so tests can pin it."""
    return time.time()


def next_updated_at(previous: str | None) -> str:
    """Ticket CAS version: even two writes in one second must differ."""
    stamp = now_iso()
    try:
        prior = datetime.fromisoformat(previous or "")
        if datetime.fromisoformat(stamp) <= prior:
            stamp = (prior + timedelta(microseconds=1)).isoformat()
    except (ValueError, TypeError):
        pass
    return stamp


def apply_batch_update(run: dict[str, Any], name: str, fields: dict[str, Any]) -> dict[str, Any]:
    batch = run.setdefault("batches", {}).setdefault(name, {"tickets": {}})
    batch.setdefault("tickets", {})
    for key, value in fields.items():
        if key == "remote":
            batch["remote"] = {**(batch.get("remote") or {}), **value}
        elif value is not None:
            batch[key] = value
    batch["updated_at"] = now_iso()
    return batch


# What a WAITING_RESOURCE ticket waits on (user 2026-10-16: "把什么资源写明白"). `kind` or `kind:detail`.
WAIT_KINDS = {
    "page-window": "本机页面窗口（本机 API/FE 切换验证）",
    "devcluster-shared": "开发集群共享配置锁",
    "heavy-slot": "本机重任务槽（构建/全量测试）",
    "host": "主机锁",
    "capacity": "存储容量",
    # 11-19 user: the old text said "Codex/Claude" while an agy queue was waiting; name the pool from the detail.
    "executor-slot": "执行者名额（按执行者分别计，冒号后为哪一类）",
    "quota": "模型额度",
    "decision": "顾问裁定",
    "ticket": "另一张票的产物",
}


# decision:<key> keys that name a release milestone, not a question for the advisor. 11-11 user:
# "怎么这么多等顾问裁定的？" — 17 deliberately deferred tickets read as 顾问裁定 on the board.
MILESTONE_DECISIONS = {
    "r5-release-sign": "r5 签发后",
    "v38-formal-release": "待排期（本版发布后）",
    "post-r5-roadmap": "r5 之后路线图",
}


def milestone_label(wait_on: Any) -> str:
    """'' unless wait_on is decision:<milestone>; then what the ticket is deliberately deferred to."""
    kind, _, detail = str(wait_on or "").partition(":")
    if kind != "decision":
        return ""
    if detail in MILESTONE_DECISIONS:
        return "排在" + MILESTONE_DECISIONS[detail]
    if detail.startswith("post-"):
        return "排在 " + detail[len("post-"):] + " 之后"
    return ""


# decision:<key> keys the user answers in person (L2223 Q1, 11-12): the keys listed under "## 待答" in
# control/user-decisions-pending.md. 11-12 user: "等 顾问裁定 是什么情况？这么多。" — they read as the
# advisor's queue although they were waiting on the user. Run.reload refreshes this set.
USER_DECISIONS_FILE = Path("control") / "user-decisions-pending.md"
USER_DECISION_KEYS: set[str] = set()


def load_user_decisions(run_path: Path) -> set[str]:
    """Keys of the open user decisions: '### <key>（…）' headings inside the '## 待答' section."""
    try:
        text = (Path(run_path) / USER_DECISIONS_FILE).read_text(encoding="utf-8")
    except OSError:
        return set()
    keys: set[str] = set()
    open_section = False
    for line in text.splitlines():
        if line.startswith("## "):
            open_section = line[3:].strip().startswith("待答")
        elif open_section and line.startswith("### "):
            key = re.split(r"[（(\s]", line[4:].strip(), maxsplit=1)[0]
            if key:
                keys.add(key)
    return keys


def user_decision_label(wait_on: Any) -> str:
    """'' unless wait_on is decision:<key> for an open user decision; then who it waits on."""
    kind, _, detail = str(wait_on or "").partition(":")
    return "你决定 " + detail if kind == "decision" and detail in USER_DECISION_KEYS else ""


def wait_label(wait_on: str | None, holder: str | None = None) -> str:
    if not wait_on:
        return ""
    kind, _, detail = str(wait_on).partition(":")
    text = milestone_label(wait_on) or user_decision_label(wait_on) \
        or (WAIT_KINDS.get(kind, kind) + (f" {detail}" if detail else ""))
    return text + (f" · 占用：{holder}" if holder else "")


# ---------------------------------------------------------------- derived waiting stages
#
# "等前置" is one display state, but its tickets are at very different points (Fable L2175 §4).
# These stages are derived on every refresh from fields the ticket already carries: candidate,
# reviews/verdict, review-pass timing, blocker markers, wait_on, target/release fields and kind.
# No new state is written to run.json.
#
# Conflict order (fixed here and covered by test_hostdag_host_view_derived_stage):
#   1. 现场冻结 — a human/site constraint is hard evidence; it outranks both a passed candidate
#      and an explicit schedule (a ticket can be ready and still have to wait for the site).
#   2. 待排期 — an explicit later-schedule decision (pool/ROADMAP/next-version design, a deferred
#      release disposition that names no required_by version, or a target field that says 待定/无)
#      outranks the generic candidate stages: the ticket was deliberately moved out of the active
#      queue. A deferral that names its version (required_by r4) is not 待排期 and keeps its stage.
#   3. 复审通过待集成 — candidate + PASS, or the coordinator's REVIEWED_WAIT_INTEGRATE /
#      INTEGRATED_IN_R4 marker. The marker also counts after WAIT-UNTIL and without a candidate:
#      historically coordinators prepend WAIT-UNTIL and sometimes clear the candidate field once
#      the work is already in an integration list.
#   4. 有候选待复审 — a candidate is present but has no PASS, or the candidate/review round is
#      newer than the recorded PASS.
#   5. 待实现 — default; no candidate and no explicit later schedule.
WAIT_STAGES = ("implement", "review", "integrate", "frozen", "scheduled")
WAIT_STAGE_LABEL = {
    "implement": "待实现",
    "review": "有候选待复审",
    "integrate": "复审通过待集成",
    "frozen": "现场冻结",
    "scheduled": "待排期",
}
WAIT_STAGE_STYLE = {
    "implement": "dim",
    "review": "cand",
    "integrate": "cand",
    "frozen": "hold",
    "scheduled": "dim",
}

# The real run has both the full and a truncated spelling; the marker may follow WAIT-UNTIL, either
# after a clause separator or (the common batch form) after a plain space:
#   "WAIT-UNTIL:2026-11-11T18:00:00+08:00 INTEGRATED_IN_R4：…".
# A blocker marker counts only where a coordinator writes a status: at the start of the blocker,
# after a clause separator, or right after WAIT-UNTIL:<ts>. A mention inside prose or a KEY=VALUE
# note (batch: "合入后状态注明 SITE_RECHECK=PENDING") is not the ticket's own status.
MARKER_ANCHOR = r"(?:^|[；;。\n]|WAIT-UNTIL:\S+?\s)\s*"
REVIEW_WAIT_RE = re.compile(
    MARKER_ANCHOR + r"(?:REVIEWED_WAIT_INTEGRATE|REVIEWED_WAIT_INTEGR|INTEGRATED_IN_R4)(?![A-Z0-9_])",
    re.IGNORECASE)
# Same anchoring for the site-freeze markers; a leading ⛔/🛑/⚠ sign is allowed ("⛔ 用户 HOLD（…）").
SITE_FREEZE_RE = re.compile(
    MARKER_ANCHOR + r"(?:[⛔🛑⚠\ufe0f]\s*)*(?:PENDING_SITE|用户\s*HOLD|USER[_\- ]HOLD)(?![A-Z0-9_])",
    re.IGNORECASE)
UNSCHEDULED_TARGETS = {"", "待定", "未定", "无", "tbd", "tba", "none", "null",
                       "unscheduled", "待排期", "roadmap", "pool"}


def candidate_text(t: dict[str, Any]) -> str:
    """The candidate as comparable text; empty when the ticket has not produced one."""
    value = t.get("candidate")
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        return ", ".join(str(x).strip() for x in value if str(x).strip())
    if isinstance(value, dict):
        return ", ".join(f"{k}:{x}" for k, x in value.items() if x)
    return ""


def review_passed(t: dict[str, Any]) -> bool:
    return any(str((r or {}).get("verdict") or "").upper() == "PASS"
               for r in (t.get("reviews") or {}).values() if isinstance(r, dict))


def _review_pass_at(t: dict[str, Any]) -> float | None:
    stamps: list[float] = []
    raw = t.get("review_pass_at")
    try:
        if raw is not None:
            stamps.append(float(raw))
    except (TypeError, ValueError):
        pass
    for review in (t.get("reviews") or {}).values():
        if not isinstance(review, dict) or str(review.get("verdict") or "").upper() != "PASS":
            continue
        try:
            if review.get("registered_at") is not None:
                stamps.append(float(review["registered_at"]))
        except (TypeError, ValueError):
            pass
    return max(stamps) if stamps else None


def _review_after_pass(t: dict[str, Any]) -> bool:
    """A FAIL or still-open review registered after the latest PASS reopens the candidate."""
    pass_at = _review_pass_at(t)
    if pass_at is None:
        return False
    for review in (t.get("reviews") or {}).values():
        if not isinstance(review, dict) or str(review.get("verdict") or "").upper() == "PASS":
            continue
        try:
            at = float(review.get("registered_at"))
        except (TypeError, ValueError):
            continue
        if at > pass_at + 1e-6:
            return True
    return False


def candidate_needs_review(t: dict[str, Any]) -> bool:
    """A candidate without a PASS, or one that moved after the PASS was registered."""
    if not candidate_text(t):
        return False
    if not review_passed(t):
        return True
    current = candidate_text(t)
    rounds = [r for r in (t.get("review_rounds") or []) if isinstance(r, dict)]
    latest = rounds[-1] if rounds else None
    if latest:
        reviewed = str(latest.get("candidate") or "").strip()
        if reviewed and current and reviewed != current:
            return True
        pass_at = _review_pass_at(t)
        at = latest.get("at")
        if pass_at is not None and isinstance(at, (int, float)) and at > pass_at + 1e-6:
            return True
    if _review_after_pass(t):
        return True
    for key in ("review_candidate", "candidate_reviewed"):
        reviewed = t.get(key)
        if isinstance(reviewed, str) and reviewed.strip() and reviewed.strip() != current:
            return True
    return False


def reviewed_wait_integrate(t: dict[str, Any]) -> bool:
    """Coordinator marker that the review passed and the ticket waits for integration."""
    return bool(REVIEW_WAIT_RE.search(str(t.get("blocker") or "")))


def site_frozen(t: dict[str, Any]) -> bool:
    """A host lock or a user-visible site hold freezes the ticket regardless of its candidate.
    The PENDING_SITE / 用户 HOLD marker must be anchored like REVIEW_WAIT_RE (see MARKER_ANCHOR)."""
    wait_on = str(t.get("wait_on") or "").strip().lower()
    if wait_on.startswith("host:"):
        return True
    return bool(SITE_FREEZE_RE.search(str(t.get("blocker") or "")))


def deferred_release(t: dict[str, Any]) -> bool:
    disposition = t.get("release_disposition")
    return isinstance(disposition, dict) and str(disposition.get("kind") or "").strip().lower() == "deferred"


def deferred_without_target(t: dict[str, Any]) -> bool:
    """A deferral that names no later version. A deferral with required_by (batch: 10 tickets moved
    from one release to a later release) has a target version and keeps its candidate/review stage."""
    if not deferred_release(t):
        return False
    required = str(t["release_disposition"].get("required_by") or "").strip().lower()
    return required in UNSCHEDULED_TARGETS


def scheduled_later(t: dict[str, Any]) -> bool:
    """Explicitly outside this version's active queue: pool/ROADMAP/design, deferred release,
    or a target/release field that itself names no version. A missing target field is *not*
    enough: older tickets inherit the batch's target and must stay 待实现 rather than 待排期."""
    kind = str(t.get("kind") or t.get("type") or "").strip().lower()
    if kind in {"pool", "roadmap", "design", "backlog", "deferred"}:
        return True
    if milestone_label(t.get("wait_on")):
        return True
    if deferred_without_target(t):
        return True
    text = f"{t.get('title') or ''} {t.get('blocker') or ''}"
    if any(marker in text.upper() for marker in ("ROADMAP", "池票", "下版设计", "后设计项", "V38 后设计")):
        return True
    for key in ("target", "release", "release_target", "target_version", "integration_target"):
        if key not in t:
            continue
        value = t.get(key)
        if isinstance(value, dict):
            value = value.get("target") or value.get("release") or value.get("version")
        if str(value or "").strip().lower() in UNSCHEDULED_TARGETS:
            return True
    return False


def wait_stage(t: dict[str, Any]) -> str:
    """One of WAIT_STAGES for a ticket the board renders as waiting; see the conflict order above."""
    if site_frozen(t):
        return "frozen"
    if scheduled_later(t):
        return "scheduled"
    if reviewed_wait_integrate(t):
        return "integrate"
    if candidate_text(t):
        return "review" if candidate_needs_review(t) else "integrate"
    return "implement"


def wait_stage_counts(batch: dict[str, Any], everything: dict[str, Any] | None = None,
                      keys: dict[str, str] | None = None) -> dict[str, int]:
    """Five derived counts for the rows currently shown as 等前置 (resource/scheduled rows stay out).
    `keys` (ticket -> display key) lets progress() reuse the display states it already computed."""
    counts = {key: 0 for key in WAIT_STAGES}
    for tid, t in (batch.get("tickets") or {}).items():
        if not isinstance(t, dict):
            continue
        key = keys[tid] if keys is not None else display_state(batch, tid, everything)[0]
        if key == "wait":
            counts[wait_stage(t)] += 1
    return counts


# A defect keeps its explicit type (new tickets) or its defect_class (the field older tickets used).
DEFECT_TYPES = {"defect", "bug", "缺陷", "product-defect"}
PRIORITY_RE = re.compile(r"^\s*[Pp]?(\d{1,2})\s*$")
# Older tickets carry the priority only in prose ("P1 kk 一键安装后…", "（UUC 审计实际 P1）"), so fall
# back to a P0–P3 token in the title. The token must start a word and not continue into an identifier,
# which keeps "SP3" and "P10" out.
PRIORITY_IN_TEXT_RE = re.compile(r"(?:^|[^0-9A-Za-z_])P([0-3])(?![0-9A-Za-z])")


# defect_class alone marks a product defect; tool/ops tickets that carry it are not 本版缺陷
# (Fable L2175 type column keeps 缺陷 and 工具 apart).
DEFECT_CLASS_KINDS = {"", "product"}


def is_defect_ticket(t: dict[str, Any]) -> bool:
    kind = str(t.get("kind") or "").strip().lower()
    if str(t.get("type") or "").strip().lower() in DEFECT_TYPES or kind in DEFECT_TYPES:
        return True
    return kind in DEFECT_CLASS_KINDS and bool(str(t.get("defect_class") or "").strip())


def ticket_priority(t: dict[str, Any]) -> int | None:
    """P0..Pn as an integer; None means the priority is unknown/undecided (shown as 未定).

    The explicit `priority` field wins; tickets written before it existed keep the rank in their
    title, which is the only other place the run records it."""
    raw = t.get("priority")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return int(raw)
    if raw is not None:
        match = PRIORITY_RE.match(str(raw))
        if match:
            return int(match.group(1))
    match = PRIORITY_IN_TEXT_RE.search(str(t.get("title") or ""))
    return int(match.group(1)) if match else None


def priority_label(rank: int | None) -> str:
    return "未定" if rank is None else f"P{rank}"


def open_defects(batch: dict[str, Any], everything: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Open defects of this batch: explicit type/defect_class, not accepted/integrated/canceled,
    not deferred by a release disposition. Type-unknown tickets are deliberately excluded (Fable
    L2175 §2: an empty type is 待裁定, never silently promoted to a must-fix defect)."""
    all_tickets = everything if everything is not None else (batch.get("tickets") or {})
    rows: list[dict[str, Any]] = []
    for tid, t in (batch.get("tickets") or {}).items():
        if not isinstance(t, dict) or not is_defect_ticket(t):
            continue
        state = ts.derive_state(t, all_tickets)
        if state in ts.DONE or state == ts.CANCELED:
            continue
        if deferred_release(t):
            continue
        rank = ticket_priority(t)
        rows.append({"ticket": tid, "priority": rank, "priority_label": priority_label(rank),
                     "state": state, "title": t.get("title") or ""})
    rows.sort(key=lambda row: (row["priority"] is None, row["priority"] if row["priority"] is not None else 0,
                               row["ticket"]))
    return rows


def all_tickets(run: dict[str, Any]) -> dict[str, Any]:
    return ts.all_tickets(run)


def advisor_answers(run_dir: Path | None) -> dict[str, dict[str, Any]]:
    """advisor question key -> its latest log entry (answered or not)."""
    out: dict[str, dict[str, Any]] = {}
    if run_dir is None:
        return out
    try:
        lines = (Path(run_dir) / "advisor/log.jsonl").read_text(encoding="utf-8").splitlines()
    except OSError:
        return out
    for raw in lines:
        try:
            rec = json.loads(raw)
        except ValueError:
            continue
        if isinstance(rec, dict) and rec.get("key"):
            out[str(rec["key"])] = rec
    return out


def _answer_record(answers: dict[str, dict[str, Any]], key: str) -> dict[str, Any] | None:
    if not key:
        return None  # an empty key would match every question by suffix
    return answers.get(key) or next((r for k, r in answers.items() if k.endswith(key) or key.endswith(k)), None)


def answer_for(answers: dict[str, dict[str, Any]], key: str) -> dict[str, Any] | None:
    rec = _answer_record(answers, key)
    return rec if is_final_answer(rec) else None


def wait_problem(run: dict[str, Any], tid: str, t: dict[str, Any], run_dir: Path | None) -> str | None:
    """Why a waiting record would be wrong from the moment it is written (stage A of the
    state-machine design, docs/local-dev/dag-state-machine-design-20261019.md):
    a wait that names nothing, a ticket that is already accepted or does not exist, an advisor
    question that is already answered."""
    everything = all_tickets(run)
    state = ts.derive_state(t, everything)
    if state == ts.FROZEN_GATE:
        return "hold", "冻结至最新候选包"
    if state not in ts.WAITING:
        return None
    kind, _, detail = str(t.get("wait_on") or "").partition(":")
    undone = [d for d in t.get("deps") or [] if ts.derive_state(everything.get(d) or {}, everything) not in ts.DONE]
    if not kind and not undone:
        return (f"{tid} 写成 {state}，但没有未完成的前置票，也没有 --wait-on。还不派的后续票请用 "
                f"--state CREATED 并在 --blocker 写排期；真在等什么就用 --wait-on 写明")
    if kind == "ticket":
        target = detail or t.get("wait_holder")
        if not target or target not in everything:
            return f"--wait-on ticket:{target or '?'}：找不到这张票（跨批次也找过）"
        if ts.derive_state(everything[target], everything) in ts.DONE:
            return f"--wait-on ticket:{target}：{target} 已验收，不能再等它；请派下一步"
    if kind == "decision" and detail:
        answered = _answer_record(advisor_answers(run_dir), detail)
        if answered is not None:
            try:
                validate_record(answered)
            except ValueError as exc:
                raise ViewError(f"advisor/log.jsonl key={answered.get('key')}: {exc}") from exc
        if is_final_answer(answered):
            when = datetime.fromtimestamp(float(answered.get("at") or 0)).strftime("%H:%M")
            return (f"--wait-on decision:{detail}：顾问 {when} 已裁定（{answered.get('verdict')}），不能再等；"
                    f"请按裁定执行。裁定：{str(answered.get('decision') or '')[:600]}")
    return None


def approve_window(t: dict[str, Any], tid: str, spec_path: str, priority: Any = None) -> None:
    """Stage A #4: the coordinator approves a page-window spec ahead of its turn; the watcher runs it
    when the page is free. An empty path withdraws a request that has not started."""
    current = t.get("page_request") or {}
    if not spec_path:
        if current.get("status") == "claimed":
            raise ViewError(f"{tid} 的页面请求已在运行，不能撤回")
        t.pop("page_request", None)
        return
    if current.get("status") == "claimed":
        raise ViewError(f"{tid} 已有一个页面请求在运行（{current.get('id')}），等它结束再批下一个")
    path = public_path(spec_path)
    try:
        spec = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ViewError(f"--approve-window：读不到 spec {path}：{exc}")
    if (spec.get("grant") or {}).get("allowRealWindow") is not True:
        raise ViewError("--approve-window：spec.grant.allowRealWindow 不是 true；先在 spec 里写授权再批")
    if not spec.get("owner"):
        raise ViewError("--approve-window：spec 没有 owner")
    t["page_request"] = {"id": f"pr-{uuid.uuid4().hex[:12]}", "spec": str(path), "owner": spec["owner"],
                         "window": spec.get("window"), "priority": int(priority or 0),
                         "approved_at": now_iso(), "status": "pending"}
    t["state"], t["wait_on"] = ts.WAITING_RESOURCE, "page-window"
    t.pop("wait_holder", None)


def done_wait_revision(run: dict[str, Any], batch: str, tid: str) -> str | None:
    """Opaque CAS token for this exact waiting ticket and its accepted prerequisite.

    No stored revision or second state source. Include all fields so attempt,
    candidate, review and prerequisite edits invalidate an observation, even if
    an older writer did not bump updated_at. Ambiguous ids/records stay manual.
    """
    rows = [(name, key, row) for name, body in (run.get("batches") or {}).items()
            for key, row in (body.get("tickets") or {}).items()]
    matches = [(name, row) for name, key, row in rows if key == tid]
    if len(matches) != 1 or matches[0][0] != batch:
        return None
    t = matches[0][1]
    if not isinstance(t, dict) or t.get("state") not in ts.WAITING or t.get("outcome"):
        return None
    if t.get("status") not in (None, "CLAIMED") or t.get("page_request"):
        return None
    wait = t.get("wait_on")
    if not isinstance(wait, str) or not wait.startswith("ticket:"):
        return None
    dep = wait.removeprefix("ticket:")
    if not dep or dep == tid or t.get("wait_holder") not in (None, "", dep):
        return None
    deps = t.get("deps", [])
    if not isinstance(deps, list) or any(not isinstance(d, str) or d == tid for d in deps):
        return None
    targets = [(name, row) for name, key, row in rows if key == dep]
    if len(targets) != 1 or not isinstance(targets[0][1], dict):
        return None
    other = targets[0][1]
    if other.get("state") not in (None, *ts.DONE) or other.get("status") not in (None, "ACCEPTED"):
        return None
    if ts.derive_state(other) not in ts.DONE or other.get("outcome"):
        return None
    payload = [batch, tid, t, dep, targets[0]]
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


TICKET_KINDS = ("tool", "doc", "product", "ops", "runtime-only")


def normalize_ticket_kind(value: str) -> str:
    kind = str(value).lower()
    if kind not in TICKET_KINDS:
        raise ViewError(f"kind must be one of {TICKET_KINDS}")
    return kind


def critical_tool(ticket: dict[str, Any]) -> bool:
    return ((ticket.get("critical_path") is True and ticket.get("kind") == "tool")
            or (ticket.get("kind") in ("tool", "ops") and bool((ticket.get("freeze_exempt") or {}).get("blocked_step"))))


def review_cap_trigger(ticket: dict[str, Any]) -> tuple[float | None, str | None]:
    """Earliest of third-round FAIL and six hours without a registered PASS.

    The timestamp is stable, so patrol/digest reads never count polling as triggers.
    """
    def stamp(value):
        try:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return float(value) if math.isfinite(value) else None
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (ValueError, TypeError, OverflowError):
            return None
    candidates = []
    recorded = stamp(ticket.get("review_cap_trigger_at"))
    if recorded is not None:
        candidates.append((recorded, ticket.get("review_cap_trigger_reason") or "round-limit"))
    first = stamp(ticket.get("first_reviewing_at"))
    passed = stamp(ticket.get("review_pass_at"))
    finished = stamp(ticket.get("degrade_decided_at"))
    if ts.derive_state(ticket) in ts.TERMINAL:
        finished = stamp(ticket.get("accepted_at") or ticket.get("updated_at")) or finished
    if (first is not None and (passed is None or passed > first + 21600)
            and (finished is None or first + 21600 <= finished)):
        candidates.append((first + 21600, "six-hours"))
    return min(candidates, default=(None, None))


def step_review_policy(run, ticket):
    """Review limits belong to the ticket, never to its blocked release step.

    Keep the existing ledger shape for readers, but do not import another
    ticket's step high-water, clock, verdict or degradation trigger.
    """
    if not (ticket.get('freeze_exempt') or {}).get('blocked_step'):
        return {}
    policy = {key: copy.deepcopy(ticket[key]) for key in (
        'first_reviewing_at', 'review_cap_trigger_at',
        'review_cap_trigger_reason', 'review_pass_at') if key in ticket}
    history = copy.deepcopy(ticket.get('review_rounds') or [])
    reviews = list((ticket.get('reviews') or {}).values())
    policy['review_round_count'] = max(
        [int(ticket.get('review_round_count') or 0), int(ticket.get('round') or 0),
         int(bool(reviews))] + [int(e.get('round') or 0) for e in history])
    policy['review_rounds'] = history
    times = [freeze_gate.freeze_stamp(policy.get('first_reviewing_at'))]
    times += [freeze_gate.freeze_stamp(e.get('at')) for e in history]
    times += [freeze_gate.freeze_stamp(r.get('registered_at')) for r in reviews]
    known = [at for at in times if at is not None]
    if known:
        policy['first_reviewing_at'] = min(known)
    cycle_at = max([freeze_gate.freeze_stamp(e.get('at')) or 0 for e in history], default=0)
    if reviews and all(
        r.get('verdict') == 'PASS' and
        (r.get('candidate') is None or r['candidate'] == ticket.get('candidate')) and
        (r.get('round') is None or r['round'] == ticket.get('round')) and
        freeze_gate.freeze_stamp(r.get('registered_at')) is not None and
        freeze_gate.freeze_stamp(r['registered_at']) >= cycle_at for r in reviews):
        policy.setdefault('review_pass_at', max(freeze_gate.freeze_stamp(r['registered_at']) for r in reviews))
    return policy


def record_review_round(before: dict[str, Any], ticket: dict[str, Any], fields: dict[str, Any], step_policy=None) -> None:
    """§16: count review cycles, never reviewer threads/axes or verdict writes.

    One update opens at most one round: REVIEWING entry, --round increase,
    --clear-reviews, or candidate change while REVIEWING. Explicit --round is a
    high-water floor (legacy rows included); decreases cannot reset the count.
    Legacy board-only rows retain this accounting. Once a dispatch budget group
    exists, physical review dispatches own rounds; registration never adds one.
    """
    if not critical_tool(ticket) and not (step_policy or {}).get('review_budget_enforced'):
        return
    now = time.time()
    if fields.get("degrade_decision") is not None and fields["degrade_decision"] != before.get("degrade_decision"):
        ticket.setdefault("degrade_decided_at", now)
    active = ts.derive_state(ticket) == ts.REVIEWING
    was_active = ts.derive_state(before) == ts.REVIEWING
    previous = int(before["review_round_count"]) if "review_round_count" in before else (
        int(before.get("round") or 0) if was_active else 0)
    opens = active and (not was_active or not previous or bool(fields.get("clear_reviews"))
                        or (fields.get("round") is not None and int(fields["round"]) > max(previous, int(before.get("round") or 0)))
                        or (fields.get("candidate") is not None and fields["candidate"] != before.get("candidate")))
    step_previous = int((step_policy or {}).get('review_round_count') or 0)
    if before.get('review_budget_group') or fields.get('review_from'):
        opens = False  # physical dispatch owns cycles after migration
        count = max(previous, step_previous)
    else:
        count = max(max(previous, step_previous) + int(opens), int(ticket.get("round") or 0) if active else previous)
    for key in ('first_reviewing_at', 'review_cap_trigger_at', 'review_cap_trigger_reason', 'review_pass_at'):
        if key in (step_policy or {}):
            ticket[key] = step_policy[key]

    if active and count > 3:
        raise ViewError("REVIEW_CAP: critical-path tool round 4 refused; execute structured degradation via lint")
    ticket["review_round_count"] = count
    if active:
        ticket.setdefault("first_reviewing_at", now)
    if opens:
        ticket.setdefault("review_rounds", []).append({"round": count, "at": now,
                                                       "candidate": ticket.get("candidate")})
    reviews = ticket.get("reviews") or {}
    round_at = (ticket.get("review_rounds") or [{}])[-1].get("at", now)
    # Only verdicts registered in this cycle can stop its clock or trigger the cap.
    valid = {axis: r for axis, r in reviews.items() if (
        (r.get("candidate") is None or r["candidate"] == ticket.get("candidate")) and
        (r.get("round") is None or r["round"] == ticket.get("round")) and
        (float(r.get("registered_at") or 0) >= round_at or
         (fields.get("reviews") or {}).get(axis, {}).get("verdict"))
        )}
    if active and reviews and len(valid) == len(reviews) and all(r.get("verdict") == "PASS" for r in valid.values()):
        ticket.setdefault("review_pass_at", now)
    if active and count >= 3 and any(r.get("verdict") == "FAIL" for r in valid.values()):
        ticket.setdefault("review_cap_trigger_at", now)
        ticket.setdefault("review_cap_trigger_reason", "round-limit")


_DELIVERY_NOTICE_SHOWN: set[str] = set()


def _warn_missing_delivery(module: str, detail: str = "release checks") -> None:
    if module not in _DELIVERY_NOTICE_SHOWN:
        print(f"WARNING: {module} unavailable; skipping {detail}", file=sys.stderr)
        _DELIVERY_NOTICE_SHOWN.add(module)


def accepted_at_guard(value: Any, prior: Any) -> None:
    """--accepted-at backfills a real earlier acceptance; it must not move a row into the pre-hook window,
    where the legacy tool rule and the post-hook acceptance lint stop looking at it."""
    try:
        from release_coverage import LEGACY_TOOL_CUTOFF
    except ImportError:
        LEGACY_TOOL_CUTOFF = None

    def parse(raw: Any):
        try:
            stamp = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
        return stamp if stamp.tzinfo is not None else None
    stamp = parse(value)
    if stamp is None:
        raise ViewError("ACCEPTED_AT_INVALID: --accepted-at needs an ISO time with timezone")
    if LEGACY_TOOL_CUTOFF is None:
        _warn_missing_delivery("release_coverage")
        return
    before = parse(prior) if prior else None
    if stamp < LEGACY_TOOL_CUTOFF and not (before is not None and before < LEGACY_TOOL_CUTOFF):
        raise ViewError(f"ACCEPTED_AT_BACKFILL_PRE_HOOK: {value} precedes the {LEGACY_TOOL_CUTOFF.isoformat()} "
                        "acceptance hook; only a row already accepted before it may keep a pre-hook time")


INFRA_REFUSAL = ("INFRA_ERROR: board guard could not decide ({detail}); write refused "
                 "(BLOCKED, not a review FAIL; no verdict, state or attempt recorded){cause}")


def infra_refusal(failures: list[str], cause: BaseException | None = None) -> ViewError:
    detail = "; ".join(dict.fromkeys(failures)) if failures else "guard crashed"
    return ViewError(INFRA_REFUSAL.format(detail=detail, cause=f"; guard said: {cause}" if cause else ""))


def apply_ticket_update(run: dict[str, Any], name: str, tid: str, fields: dict[str, Any],
                        run_dir: Path | None = None) -> dict[str, Any]:
    """Fail-closed wrapper (TOOL-GUARD-FAIL-CLOSED, Fable L2177 §4(b)).

    Any git infrastructure failure a guard hit while deciding this write (killed, timed out, could not
    start, garbage output; see release_coverage.infra_watch) refuses the write as INFRA_ERROR, whether
    the guard then refused for a product reason or let the write through. Callers write nothing when
    this raises."""
    try:
        from release_coverage import infra_watch
    except ImportError:
        _warn_missing_delivery("release_coverage")
        return _apply_ticket_update(run, name, tid, fields, run_dir)
    with infra_watch() as failures:
        try:
            ticket = _apply_ticket_update(run, name, tid, fields, run_dir)
        except ViewError as exc:
            if failures:
                raise infra_refusal(failures, exc) from exc
            raise
    if failures:
        raise infra_refusal(failures)
    return ticket


def _apply_ticket_update(run: dict[str, Any], name: str, tid: str, fields: dict[str, Any],
                         run_dir: Path | None = None) -> dict[str, Any]:
    """Apply one coordinator update. Enforces the dag rules a panel would otherwise misreport:
    outcomes only on claimed tickets, acceptance only after every prerequisite is accepted.

    `state` (hostdag_ticket_state.STATES) also rewrites status/outcome to match; an update that sets
    only status/outcome drops a written state so it is derived again from those fields."""
    batch = batches(run).get(name)
    if batch is None:
        raise ViewError(f"unknown batch {name}; create it with 'batch set' first")
    tickets = batch.setdefault("tickets", {})
    t: dict[str, Any] = copy.deepcopy(tickets.get(tid) or {"deps": []})
    reference_scope = None
    if fields.get('review_from'):
        from hostdag_dispatch_guard import validate_review_registration, DispatchError
        try:
            reference_scope = validate_review_registration(run, tid, fields, name)
        except DispatchError as exc:
            raise ViewError(exc.message) from exc
        t['review_budget_group'] = reference_scope[0]
    if fields.get("withdraw_release_disposition") is not None:
        # A withdrawal changes only disposition metadata, never acceptance,
        # dependency/review accounting or updated_at on an already closed row.
        try:
            from release_coverage import accepted_candidate, decision_lines, names_ticket, names_kind
        except ImportError:
            _warn_missing_delivery("release_coverage")
            accepted_candidate = decision_lines = names_ticket = names_kind = None
        try:
            if any(k != "withdraw_release_disposition" and
                   (k in ("status", "outcome") or (value is not None and value is not False))
                   for k, value in fields.items()):
                raise ValueError("withdrawal cannot be combined with other ticket edits")
            if ts.derive_state(t) not in (ts.ACCEPTED, ts.INTEGRATED):
                raise ValueError("withdrawal requires ACCEPTED or INTEGRATED state")
            if accepted_candidate is not None:
                if not accepted_candidate(t.get("candidate")):
                    raise ValueError("withdrawal requires accepted_candidate (repo:40hex)")
                reference = fields["withdraw_release_disposition"]
                if not isinstance(reference, str) or not re.fullmatch(r"advisor/log\.jsonl#L[1-9][0-9]*", reference):
                    raise ValueError("withdrawal requires one run-relative advisor/log.jsonl#Ln")
                rows = decision_lines(reference, run_dir, advisor_only=True)
                if not all(names_ticket(row, tid) and names_kind(row, "withdraw") for row in rows):
                    raise ValueError("withdrawal decision must name ticket and word withdraw")
            else:
                reference = fields["withdraw_release_disposition"]
            old = t.get("release_disposition")
            if not isinstance(old, dict) or not old:
                raise ValueError("ticket has no release_disposition to withdraw")
            history = t.get("release_disposition_history", [])
            if not isinstance(history, list):
                raise ValueError("release_disposition_history must be a list")
        except (ValueError, OSError, IndexError, TypeError) as exc:
            raise ViewError("RELEASE_DISPOSITION_WITHDRAW_REJECTED: " + str(exc)) from exc
        t["release_disposition_history"] = history + [dict(
            old, withdrawn_at=now_iso(), withdraw_decision_ref=reference)]
        del t["release_disposition"]
        tickets[tid] = t
        return t
    migration_validated = False
    if fields.get("coverage_migration_plan") is not None:
        try:
            from release_coverage_history import validate_plan
        except ImportError:
            _warn_missing_delivery("release_coverage_history", "coverage migration check")
            validate_plan = None
        if validate_plan is not None:
            try:
                changes, proof = validate_plan(fields["coverage_migration_plan"], run_dir, name, tid, t)
            except (ValueError, OSError, TypeError, KeyError) as exc:
                raise ViewError("COVERAGE_MIGRATION_REJECTED: " + str(exc)) from exc
            # The host's later core mirror receives these same verified changes.
            fields.update(changes)
            migration_validated = True
            fields["coverage_migration"] = proof
            fields["coverage_migration_plan"] = None  # control input; the validated proof is mirrored
    if fields.get("production_write_fence") is not None and fields["production_write_fence"] is not True:
        raise ViewError("production_write_fence can only be enabled, never cleared")
    if (ts.derive_state(t) in (ts.ACCEPTED, ts.INTEGRATED) and fields.get("candidate") is not None
            and fields["candidate"] != t.get("candidate")):
        raise ViewError("CANDIDATE_LOCKED: reopen accepted/integrated ticket before changing candidate")
    if fields.get("accepted_at"):
        accepted_at_guard(fields["accepted_at"], (tickets.get(tid) or {}).get("accepted_at"))
    expected = fields.get("expected_updated_at")
    if expected is not None and t.get("updated_at") != expected:
        raise ViewError(f"{tid} changed since the observed updated_at; no update applied")
    for key in ("release_group", "requires_for_release", "title", "deps", "round", "executor", "model", "effort", "candidate",
                "base", "branch", "worktree", "evidence", "blocker", "scope", "owner", "defect_class", "regression", "candidate_decision_ref", "production_write_fence", "coverage_migration", "critical_path", "degrade_plan", "degrade_decision", "freeze_exempt", "freeze_reason", "root_ticket", "chain_id"):
        if fields.get(key) is not None:
            t[key] = fields[key]
    try:
        from release_coverage import release_relationships
    except ImportError:
        _warn_missing_delivery("release_coverage")
    else:
        try:
            release_relationships(t)
            old = tickets.get(tid) or {}
            if old.get("release_obligation") and ts.derive_state(old) in ts.DONE:
                if any(t.get(k) != old.get(k) for k in ("release_group", "requires_for_release")):
                    raise ValueError("RELEASE_OBLIGATION_LOCKED: reopen before changing release relationships")
        except ValueError as exc:
            raise ViewError(str(exc)) from exc
    if fields.get("kind") is not None:
        t["kind"] = normalize_ticket_kind(fields["kind"])
    if "critical_path" in t and not isinstance(t["critical_path"], bool):
        raise ViewError("critical_path must be boolean")
    for key in ("degrade_plan", "degrade_decision", "freeze_exempt"):
        if key in t and (not isinstance(t[key], dict) or not t[key]):
            raise ViewError(f"{key} must be a nonempty JSON object")

    if fields.get("release_disposition") is not None:
        try:
            from release_coverage import validate_disposition
        except ImportError:
            _warn_missing_delivery("release_coverage")
            t["release_disposition"] = fields["release_disposition"]
        else:
            try:
                t["release_disposition"] = validate_disposition(fields["release_disposition"])
            except (ValueError, TypeError) as exc:
                raise ViewError(str(exc)) from exc
    # Revalidate later candidate/disposition edits too: a tool ticket cannot be
    # classified once and then have a product candidate substituted.
    try:
        from release_coverage import tool_ticket, verify_kind_paths
    except ImportError:
        _warn_missing_delivery("release_coverage")
        if fields.get("kind") is not None:
            t["kind_proof"] = dict(status="skipped", method="release_coverage_missing", kind=t["kind"], sources=[])
    else:
        inferred_with_candidate = not t.get("kind") and tool_ticket(tid, t) and bool(t.get("candidate") or t.get("commits"))
        if str(t.get("kind") or "").lower() in ("tool", "doc", "docs") or inferred_with_candidate:
            try:
                source = {k: t[k] for k in ("candidate", "commits", "worktree", "release_disposition") if k in t}
                kind = "tool" if inferred_with_candidate else ("doc" if str(t["kind"]).lower() == "docs" else str(t["kind"]).lower())
                t["kind_proof"] = dict(verify_kind_paths(source, run_dir, kind=kind, ticket_id=tid), kind=kind)
            except (ValueError, OSError, TypeError) as exc:
                raise ViewError(str(exc)) from exc
        elif fields.get("kind") is not None:
            t["kind_proof"] = dict(status="verified", method="non_tool", kind=t["kind"], sources=[])
    if fields.get("clear_reviews"):
        t.pop("reviews", None)
    for axis, review in (fields.get("reviews") or {}).items():
        if axis not in REVIEW_AXES:
            raise ViewError(f"review axis must be one of {REVIEW_AXES}")
        cur = dict((t.get("reviews") or {}).get(axis) or {})
        identity_changed = any(review.get(k) is not None and review[k] != cur.get(k)
                               for k in ("thread", "candidate", "round", "attempt", "evidence"))
        previous_verdict = cur.get("verdict")
        cur.update({k: v for k, v in review.items() if v is not None})
        if reference_scope:
            source_batch, source_tid = fields['review_from'].split('/')
            cur.update(source={'batch': source_batch, 'ticket': source_tid},
                       token=fields['review_token'])
        elif review.get('thread') and review['thread'] != (t.get('reviews') or {}).get(axis, {}).get('thread'):
            cur.pop('source', None)
            cur.pop('token', None)
        if review.get("verdict") and (review["verdict"] != previous_verdict or identity_changed):
            cur["registered_at"] = now_epoch()
        elif identity_changed:
            cur.pop("registered_at", None)
        if cur.get("verdict") and cur["verdict"] not in VERDICTS:
            raise ViewError(f"verdict must be one of {VERDICTS}")
        t.setdefault("reviews", {})[axis] = cur
    if "status" in fields:
        status = fields["status"]
        if status is None:
            t.pop("status", None)
            t.pop("outcome", None)
        else:
            if status not in STATUSES:
                raise ViewError(f"status must be one of {STATUSES} or none")
            t["status"] = status
            if status != "CLAIMED":
                t.pop("outcome", None)
    if fields.get("attempt") is not None:
        spec = str(fields["attempt"])
        t["attempt"] = ts.attempt_of(t if "attempt" in t else {}) + 1 if spec == "+1" else int(spec)
    if fields.get("accept_scope") is not None:
        scope = str(fields["accept_scope"]).upper()
        if scope not in ts.SCOPES:
            raise ViewError(f"accept scope must be one of {ts.SCOPES}")
        t["accept_scope"] = scope
    if "outcome" in fields:
        outcome = fields["outcome"]
        if outcome is None:
            t.pop("outcome", None)
        else:
            if outcome not in OUTCOMES:
                raise ViewError(f"outcome must be one of {OUTCOMES} or none")
            if t.get("status") != "CLAIMED":
                raise ViewError("an outcome needs status CLAIMED")
            t["outcome"] = outcome
    if fields.get("wait_on") is not None:
        kind = str(fields["wait_on"]).partition(":")[0]
        if fields["wait_on"] and kind not in WAIT_KINDS:
            raise ViewError(f"--wait-on kind must be one of {', '.join(WAIT_KINDS)} (as kind or kind:detail)")
        t["wait_on"] = fields["wait_on"] or None
    if fields.get("wait_holder") is not None:
        t["wait_holder"] = fields["wait_holder"] or None
    if fields.get("state") is not None:
        apply_state(t, str(fields["state"]).upper())
        if t["state"] == ts.WAITING_RESOURCE and not t.get("wait_on"):
            raise ViewError("WAITING_RESOURCE needs --wait-on naming the resource "
                            f"({', '.join(WAIT_KINDS)}; e.g. page-window, capacity:ZFS, host:12-5)")
        if t["state"] not in (ts.WAITING_RESOURCE, ts.WAITING_DEPENDENCY):
            t.pop("wait_on", None)
            t.pop("wait_holder", None)
    elif "status" in fields or "outcome" in fields:
        t.pop("state", None)  # old-style write: let the state be derived from the fields just set
    if fields.get("window_spec") is not None:
        approve_window(t, tid, str(fields["window_spec"]), fields.get("window_priority"))
    if fields.get("state") is not None or "status" in fields:
        t.pop("pending_action", None)  # the coordinator acted on what the program left for it
    for dep in t.get("deps") or []:
        if dep == tid:
            raise ViewError(f"{tid} cannot depend on itself")
    if any(fields.get(k) is not None for k in ("state", "wait_on", "wait_holder", "blocker")):
        problem = wait_problem(run, tid, {**t}, run_dir)
        if problem:
            raise ViewError(problem)
    # Check the state actually written/derived above, after normalization. Do
    # not re-accept an old row on metadata edits.
    state_write = fields.get("state") is not None or "status" in fields or fields.get("candidate") is not None
    accepting = state_write and ts.derive_state(t) == ts.ACCEPTED
    # A new acceptance, by either done state (a direct jump to INTEGRATED included), must carry what the
    # coverage gate later demands; rewrites of an already accepted row keep the candidate-only check.
    newly_done = (state_write and ts.derive_state(t) in ts.DONE
                  and ts.derive_state(tickets.get(tid) or {"deps": []}) not in ts.DONE)
    if accepting:  # INTEGRATED: verify_integration below refuses a missing/short candidate itself
        try:
            from release_coverage import acceptance_problem
        except ImportError:
            _warn_missing_delivery("release_coverage")
        else:
            problem = acceptance_problem(tid, t, run_dir, attribution=False)
            if problem:
                raise ViewError(f"{problem[0]}: {problem[1]}")
    written_fields = {k for k, value in fields.items()
                      if value is not None and value is not False and k != "expected_updated_at"}
    disposition_only = ("release_disposition" in written_fields
                        and written_fields <= {"release_disposition", "coverage_migration"}
                        and ("coverage_migration" not in written_fields or migration_validated))
    integrating = (ts.derive_state(t) == ts.INTEGRATED and
                   (fields.get("state") is not None or "status" in fields or
                    any(fields.get(k) is not None for k in ("candidate", "integrate_heads"))))
    if integrating:
        try:
            from release_coverage import tool_kind_proof, verify_integration
        except ImportError:
            _warn_missing_delivery("release_coverage")
        else:
            try:
                # Same tool decision as dag-lint and the coverage gate: a path-verified tool
                # ticket lands on the GC tool head; everything else on the frozen release head.
                source = {k: t[k] for k in ("kind", "candidate", "commits", "worktree", "release_disposition", "accepted_at") if k in t}
                tool = tool_kind_proof(tid, source, run_dir) is not None
                proof = verify_integration(t.get("candidate"), run_dir, fields.get("integrate_heads"), tool=tool)
            except (ValueError, OSError, TypeError) as exc:
                raise ViewError(str(exc)) from exc
            t["integration_verification"] = "verified"
            t["integration_evidence"] = dict(proof, at=now_iso())
    if t.get("status") == "ACCEPTED" and not disposition_only:
        # A prerequisite may live in another batch (T-2 depends on T-1).
        def dep_status(d: str) -> Any:
            if d in tickets:
                return tickets[d].get("status")
            for other in (run.get("batches") or {}).values():
                if d in (other.get("tickets") or {}):
                    return other["tickets"][d].get("status")
            return None
        pending = [d for d in t.get("deps") or [] if dep_status(d) != "ACCEPTED"]
        if pending:
            raise ViewError(f"{tid} cannot be ACCEPTED before its prerequisites: {', '.join(pending)}")
        if newly_done:
            try:
                from release_coverage import acceptance_problem
            except ImportError:
                _warn_missing_delivery("release_coverage")
            else:
                problem = acceptance_problem(tid, t, run_dir, tickets=all_tickets(run))
                if problem:
                    raise ViewError(f"{problem[0]}: {problem[1]}")
        if fields.get("accepted_at"):  # backfilling a ticket accepted earlier
            t["accepted_at"] = fields["accepted_at"]
        t.setdefault("accepted_at", now_iso())
    if ts.derive_state(t) in ts.DONE and (newly_done or not t.get("release_obligation")):
        try:
            from release_coverage import delivery_obligation
        except ImportError:
            _warn_missing_delivery("release_coverage")
        else:
            t["release_obligation"] = delivery_obligation(tid, t, t.get("accepted_at"))
    if ts.derive_state(t, all_tickets(run)) == ts.RUNNING and not str(t.get("worktree") or "").strip():
        print(f"WARNING: {tid} RUNNING 缺少 worktree；请用 --worktree 登记实际工作树", file=sys.stderr)
    policy = {k: fields[k] for k in ("production_write_fence", "coverage_migration")
              if fields.get(k) is not None and fields[k] != (tickets.get(tid) or {}).get(k)}
    if policy:
        import uuid
        from hostdag_dag_core import Op
        op = Op("coverage-policy-" + str(uuid.uuid4()), "ticket.amend", "coordinator", now_iso(),
                tid, payload=copy.deepcopy(policy))
        op.validate()
        t.setdefault("coverage_policy_audit", []).append({"batch": name, "op": op.to_json()})
    if run_dir is not None:
        admission = (bool(fields.get('reviews')) or bool(fields.get('clear_reviews')) or
                     fields.get('executor') is not None or
                     (ts.derive_state(t) in (ts.RUNNING, ts.REVIEWING) and
                      any(fields.get(k) is not None for k in ('state', 'status', 'outcome', 'round', 'candidate'))))
        problem = freeze_gate.freeze_gate_problem(Path(run_dir), run, tid, t, name)
        if problem and admission:
            raise ViewError(problem)
        if (problem and not tickets.get(tid) and t.get('kind') in ('tool', 'ops')
                and ts.derive_state(t) not in ts.TERMINAL):
            apply_state(t, ts.FROZEN_GATE)
            t['freeze_reason'] = 'step_busy' if 'step_busy' in problem else 'missing_exempt'
    step_policy = step_review_policy(run, tickets.get(tid) or {})
    if t.get('root_ticket') or t.get('chain_id') or t.get('review_budget_group'):
        from hostdag_dispatch_guard import review_policy_snapshot, DispatchError
        try:
            root_policy = review_policy_snapshot(run, tid, t)
        except DispatchError as exc:
            raise ViewError(exc.message) from exc
        step_policy = {**step_policy, **root_policy,
                       'review_round_count': max(step_policy.get('review_round_count',0),root_policy.get('review_round_count',0))}
    record_review_round(tickets.get(tid) or {}, t, fields, step_policy)
    step = (t.get('freeze_exempt') or {}).get('blocked_step')
    if step and critical_tool(t):
        # Commit shared accounting only after all refusal paths above have passed.
        for key in ('review_round_count', 'first_reviewing_at', 'review_cap_trigger_at',
                    'review_cap_trigger_reason', 'review_pass_at'):
            if key in t:
                step_policy[key] = t[key]
        t['review_step_accounted'] = step
        t['review_rounds'] = [dict(e, ticket=e.get('ticket', tid)) for e in t.get('review_rounds', [])]
        before_events = step_policy.get('review_rounds', [])
        events = {json.dumps(e, sort_keys=True): e for e in before_events + t.get('review_rounds', [])}
        step_policy['review_rounds'] = sorted(events.values(), key=lambda e: e.get('at', 0))
        run.setdefault('review_steps', {})[step] = step_policy
    t["updated_at"] = next_updated_at(t.get("updated_at"))
    if reference_scope:
        gid, group, old, _ = reference_scope
        ledger = run.setdefault('review_budget', {}).setdefault('groups', {})
        for old_id in old:
            ledger.pop(old_id, None)
        ledger[gid] = group
    tickets[tid] = t
    return t


# The old fields each state implies, so readers that know only status/outcome stay correct.
STATE_LEGACY = {
    ts.DEGRADING: ("CLAIMED", None), ts.READY_DEGRADE_A: ("CLAIMED", None),
    ts.CANDIDATE_BLOCKED: ("CLAIMED", "EXTERNALLY_BLOCKED"),
    ts.FROZEN_GATE: (None, None), ts.CREATED: (None, None), ts.WAITING_DEPENDENCY: (None, None),
    ts.WAITING_RESOURCE: ("CLAIMED", None), ts.RUNNING: ("CLAIMED", None), ts.REVIEWING: ("CLAIMED", None),
    ts.NEEDS_USER: ("CLAIMED", "NEEDS_DECISION"), ts.EXTERNALLY_BLOCKED: ("CLAIMED", "EXTERNALLY_BLOCKED"),
    ts.ACCEPTED: ("ACCEPTED", None), ts.INTEGRATED: ("ACCEPTED", None),
    ts.FAILED: ("CLAIMED", None), ts.CANCELED: ("SUPERSEDED", None),
}


def apply_state(t: dict[str, Any], state: str) -> None:
    if state not in ts.STATES:
        raise ViewError(f"state must be one of {ts.STATES}")
    t["state"] = state
    status, outcome = STATE_LEGACY[state]
    if status is None and t.get("status") == "CLAIMED":
        status = "CLAIMED"  # a claimed ticket waiting on a dependency stays claimed
    for key, value in (("status", status), ("outcome", outcome)):
        if value is None:
            t.pop(key, None)
        else:
            t[key] = value
    if state not in ts.DONE:
        t.pop("accept_scope", None)


def update_run(run_path: Path, mutate) -> Any:
    with RunLock(run_path):
        mtime = run_path.stat().st_mtime
        run = load_json(run_path)
        result = mutate(run)
        save_run(run_path, run, mtime)
        return result


# ---------------------------------------------------------------- rollout timeline


def find_rollout(thread: str, root: Path | None = None) -> Path | None:
    root = root or SESSIONS_ROOT
    hits = glob.glob(str(root / "*" / "*" / "*" / f"rollout-*-{thread}.jsonl"))
    return Path(max(hits, key=os.path.getmtime)) if hits else None


def parse_ts(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def first_line(text: str, limit: int = 1000) -> str:
    for line in str(text).splitlines():
        line = line.strip()
        if line:
            return line if len(line) <= limit else line[: limit - 1] + "…"
    return ""


_CMD_RE = re.compile(r"""\bcmd\s*:\s*(?:"((?:[^"\\]|\\.)*)"|'((?:[^'\\]|\\.)*)'|`((?:[^`\\]|\\.)*)`)""")
_PATCH_FILE_RE = re.compile(r"\*\*\* (?:Update|Add|Delete) File: (\S+)")


_HEREDOC_RE = re.compile(r"<<-?\s*['\"]?(\w+)['\"]?\s*$")


def command_summary(cmd: str) -> str:
    """First line of a command; for `... <<'PY'` also the first meaningful heredoc line."""
    lines = [x.strip() for x in cmd.splitlines() if x.strip()]
    if not lines:
        return ""
    head = lines[0]
    if _HEREDOC_RE.search(head):
        body = next((x for x in lines[1:] if not x.startswith(("#", "from ", "import ", "set -"))), "")
        if body:
            head = f"{head} ↵ {body}"
    return first_line(head)


def describe_call(name: str, raw: str) -> tuple[str, str]:
    """(kind, text) for a tool call. kind is shell/patch/tool."""
    files = _PATCH_FILE_RE.findall(raw)
    if "apply_patch" in raw or name == "apply_patch":
        return "patch", " ".join(dict.fromkeys(files)) or first_line(raw)
    cmds = []
    for m in _CMD_RE.finditer(raw):
        s = next(g for g in m.groups() if g is not None)
        try:
            s = json.loads(f'"{s}"') if m.group(1) is not None else s
        except json.JSONDecodeError:
            pass
        cmds.append(command_summary(s))
    if cmds:
        more = f"  (+{len(cmds) - 1})" if len(cmds) > 1 else ""
        return "shell", cmds[0] + more
    if name in {"shell", "exec_command", "local_shell"}:
        try:
            args = json.loads(raw)
            cmd = args.get("cmd") or args.get("command")
            if isinstance(cmd, list):
                cmd = " ".join(cmd)
            if cmd:
                return "shell", first_line(cmd)
        except (json.JSONDecodeError, AttributeError):
            pass
    return "tool", f"{name} {first_line(raw, 300)}".strip()


def output_text(output: Any) -> str:
    if isinstance(output, str):
        return output
    if isinstance(output, list):
        return "\n".join(str(p.get("text", "")) for p in output if isinstance(p, dict))
    if isinstance(output, dict):
        return str(output.get("output") or output.get("text") or "")
    return ""


def inner_output_lines(text: str) -> list[str]:
    """The command's own output lines, unwrapped from exec chunk JSON when present."""
    lines: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{") and '"output"' in line:
            try:
                lines.extend(str(json.loads(line).get("output", "")).splitlines())
                continue
            except json.JSONDecodeError:
                pass
        lines.append(line)
    return [x for x in lines if x.strip()]


@dataclass
class Event:
    ts: float
    kind: str          # start / msg / shell / patch / tool / end / abort
    text: str
    result: str = ""   # ok / fail / running / ""
    code: int | None = None
    done_ts: float = 0.0
    detail: list[str] = field(default_factory=list)


class Timeline:
    """Incremental rollout parser. Call ``refresh()`` to read newly appended lines."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.offset = 0
        self.partial = b""
        self.events: list[Event] = []
        self.calls: dict[str, Event | tuple[str, str]] = {}
        self.cells: dict[str, Event] = {}
        self.running = False
        self.turn_started = 0.0
        self.last_ts = 0.0
        self.tokens = 0
        self.cached_ratio = 0.0
        self.want_start = False
        self.model = ""
        self.effort = ""
        self.cwd = ""
        self.truncated = False
        self.file_identity = None
        self.file_mtime_ns = None

    def reset(self) -> None:
        """Reset the concrete parser, including subclass accumulators."""
        self.__init__(self.path)

    def refresh(self, chunk: int = 4 << 20) -> None:
        """Read what was appended since the last call, in chunks so a long coordinator
        rollout (100+ MB) never sits in memory at once."""
        try:
            fh = open(self.path, "rb")
        except OSError:
            return
        with fh:
            stat = os.fstat(fh.fileno())
            identity = (stat.st_dev, stat.st_ino)
            if self.file_identity is not None and (
                    self.file_identity != identity or stat.st_size < self.offset or
                    (stat.st_size == self.offset and stat.st_mtime_ns != self.file_mtime_ns)):
                self.reset()
            self.file_identity = identity
            self.file_mtime_ns = stat.st_mtime_ns
            if self.offset == 0 and stat.st_size > chunk:
                self.truncated = True
                self.running = None  # missing start/end evidence is not proof of idleness
                fh.seek(stat.st_size - chunk)
                fh.readline()  # skip the cut JSON record, never parse a partial prefix
                self.offset = fh.tell()
            fh.seek(self.offset)
            while True:
                data = fh.read(chunk)
                if not data:
                    return
                self.offset += len(data)
                lines = (self.partial + data).split(b"\n")
                self.partial = lines.pop()
                for raw in lines:
                    if raw.strip():
                        try:
                            self.feed(json.loads(raw))
                        except (json.JSONDecodeError, TypeError, AttributeError):
                            continue

    def feed(self, rec: dict[str, Any]) -> None:
        ts = parse_ts(rec.get("timestamp"))
        if ts:
            self.last_ts = max(self.last_ts, ts)
        kind = rec.get("type")
        p = rec.get("payload") or {}
        ptype = p.get("type")
        if kind == "session_meta":
            self.cwd = p.get("cwd") or self.cwd
        elif kind == "turn_context":
            self.model = p.get("model") or self.model
            self.effort = p.get("effort") or p.get("reasoning_effort") or self.effort
        elif kind == "event_msg":
            if ptype == "task_started":
                self.running = True
                self.turn_started = float(p.get("started_at") or ts)
                self.want_start = True
            elif ptype == "item_completed" and self.want_start:
                item = p.get("item") or {}
                if item.get("type") == "UserMessage":
                    text = " ".join(str(c.get("text", "")) for c in item.get("content") or [] if isinstance(c, dict))
                    self.events.append(Event(ts, "start", first_line(text)))
                    self.want_start = False
            elif ptype == "task_complete":
                self.running = False
                self.want_start = False
                self.events.append(Event(ts, "end", first_line(p.get("last_agent_message") or "")))
            elif ptype == "turn_aborted":
                self.running = False
                self.want_start = False
                self.events.append(Event(ts, "abort", str(p.get("reason") or "")))
            elif ptype == "token_count":
                usage = ((p.get("info") or {}).get("total_token_usage")) or {}
                inp = int(usage.get("input_tokens") or 0)
                self.tokens = int(usage.get("total_tokens") or 0)
                self.cached_ratio = (int(usage.get("cached_input_tokens") or 0) / inp) if inp else 0.0
        elif kind == "response_item":
            if ptype == "message" and p.get("role") == "assistant":
                text = " ".join(str(c.get("text", "")) for c in p.get("content") or [] if isinstance(c, dict))
                if text.strip():
                    self.events.append(Event(ts, "msg", first_line(text)))
            elif ptype in {"custom_tool_call", "function_call", "local_shell_call"}:
                name = str(p.get("name") or ptype)
                raw = p.get("input") if ptype == "custom_tool_call" else p.get("arguments")
                if ptype == "local_shell_call":
                    raw = json.dumps(p.get("action") or {})
                raw = raw if isinstance(raw, str) else json.dumps(raw or {})
                call_id = str(p.get("call_id") or p.get("id") or "")
                if name in {"wait", "write_stdin"}:
                    try:
                        args = json.loads(raw)
                    except json.JSONDecodeError:
                        args = {}
                    self.calls[call_id] = ("poll", str(args.get("cell_id") or args.get("session_id") or ""))
                    return
                ev_kind, text = describe_call(name, raw)
                ev = Event(ts, ev_kind, text, result="running")
                self.events.append(ev)
                self.calls[call_id] = ev
            elif ptype in {"custom_tool_call_output", "function_call_output"}:
                target = self.calls.pop(str(p.get("call_id") or ""), None)
                text = output_text(p.get("output"))
                if isinstance(target, tuple):
                    ev = self.cells.get(target[1])
                    if ev is not None and not text.startswith("Script running"):
                        self._resolve(ev, text, ts)
                        self.cells.pop(target[1], None)
                    return
                if target is None:
                    return
                m = re.match(r"Script running with cell ID (\S+)", text)
                if m:
                    self.cells[m.group(1)] = target
                    return
                self._resolve(target, text, ts)

    def _resolve(self, ev: Event, text: str, ts: float) -> None:
        codes = [int(c) for c in re.findall(r'"exit_code"\s*:\s*(-?\d+)', text)]
        m = re.search(r"(?:Exit code|exit code|Process exited with code)[: ]+(-?\d+)", text)
        if m:
            codes.append(int(m.group(1)))
        bad = [c for c in codes if c != 0]
        failed = bool(bad) or text.startswith("Script failed")
        ev.done_ts = ts
        if failed:
            ev.result = "fail"
            ev.code = bad[0] if bad else None
            ev.detail = inner_output_lines(text)[-2:]
        else:
            ev.result = "ok"

    def status_line(self, now: float | None = None) -> str:
        now = now or time.time()
        if self.running is None:
            return '状态未知 · 截尾记录缺少回合开始/结束事件'
        if self.running:
            return (f"运行中 · 本回合 {dur(now - self.turn_started)} · 最后活动 {dur(now - self.last_ts)}前"
                    f" · tokens {self.tokens // 1000}k (缓存 {self.cached_ratio:.0%})")
        if self.last_ts:
            return f"空闲 · 上次活动 {clock(self.last_ts)} · tokens {self.tokens // 1000}k"
        return "无记录"

    def last_activity(self, now: float | None = None) -> str:
        now = now or time.time()
        if not self.events:
            return ""
        ev = self.events[-1]
        age = dur(now - (ev.done_ts or ev.ts))
        if ev.kind in {"shell", "patch", "tool"} and ev.result == "running" and self.running:
            return f"{age} {short_cmd(ev.text)} ⏳"
        if ev.kind == "end":
            return f"{clock(ev.ts)} 回合结束"
        return f"{age} {short_cmd(ev.text)}"


class CliTimeline(Timeline):
    """Stream-json log of a Cursor Agent CLI or Claude Code CLI executor (host-dag-cli-agent.py `cli.ndjson`).

    One run is one turn: it is running from `system/init` until the closing `result` event.
    """

    CURSOR_TOOL_KEYS = {"readToolCall": "path", "grepToolCall": "pattern", "globToolCall": "globPattern",
                        "lsToolCall": "path", "deleteToolCall": "path"}

    def feed(self, rec: dict[str, Any]) -> None:
        # Cursor stamps every record with timestamp_ms; Claude Code puts an ISO `timestamp` on
        # assistant/user records only, so init/result/system records take the last time seen.
        ms = rec.get("timestamp_ms")
        ts = float(ms) / 1000 if isinstance(ms, (int, float)) else parse_ts(rec.get("timestamp")) or self.last_ts
        if ts:
            self.last_ts = max(self.last_ts, ts)
        kind = rec.get("type")
        content = (rec.get("message") or {}).get("content") or []
        if kind == "system" and rec.get("subtype") == "init":
            self.cwd = rec.get("cwd") or self.cwd
            self.model = rec.get("model") or self.model
            self.running, self.turn_started, self.want_start = True, ts, True
        elif kind == "user":
            texts = [str(c.get("text", "")) for c in content if isinstance(c, dict) and c.get("type") == "text"]
            if self.want_start and texts:
                self.events.append(Event(ts, "start", first_line(" ".join(texts))))
                self.want_start = False
            for c in content:  # Claude Code: tool results come back as a user message
                if isinstance(c, dict) and c.get("type") == "tool_result":
                    target = self.calls.pop(str(c.get("tool_use_id") or ""), None)
                    if isinstance(target, Event):
                        self._finish(target, ts, bool(c.get("is_error")), None, output_text(c.get("content")))
        elif kind == "assistant":
            for c in content:
                if not isinstance(c, dict):
                    continue
                if c.get("type") == "text" and str(c.get("text", "")).strip():
                    self.events.append(Event(ts, "msg", first_line(c["text"])))
                elif c.get("type") == "tool_use":  # Claude Code
                    name, args = str(c.get("name") or "tool"), c.get("input") or {}
                    if name == "Bash":
                        ev = Event(ts, "shell", command_summary(str(args.get("command") or "")), result="running")
                    elif name in {"Edit", "Write", "MultiEdit", "NotebookEdit"}:
                        ev = Event(ts, "patch", str(args.get("file_path") or ""), result="running")
                    else:
                        ev = Event(ts, "tool", f"{name} {first_line(json.dumps(args, ensure_ascii=False), 300)}",
                                   result="running")
                    self.events.append(ev)
                    self.calls[str(c.get("id") or "")] = ev
        elif kind == "tool_call":
            body = rec.get("tool_call") or {}
            name = next(iter(body), "tool")
            call = body.get(name) or {}
            call_id = str(rec.get("call_id") or "")
            if rec.get("subtype") == "started":
                args = call.get("args") or {}
                if name == "shellToolCall":
                    ev = Event(ts, "shell", command_summary(str(args.get("command") or "")), result="running")
                elif name == "editToolCall":
                    ev = Event(ts, "patch", str(args.get("path") or ""), result="running")
                else:
                    label = name.removesuffix("ToolCall")
                    ev = Event(ts, "tool", f"{label} {args.get(self.CURSOR_TOOL_KEYS.get(name, ''), '')}".strip(),
                               result="running")
                self.events.append(ev)
                self.calls[call_id] = ev
            elif rec.get("subtype") == "completed":
                target = self.calls.pop(call_id, None)
                if isinstance(target, Event):
                    result = call.get("result") or {}
                    ok = result.get("success")
                    code = str((ok or {}).get("exitCode", "0")) if isinstance(ok, dict) else None
                    failed = ok is None or (code not in (None, "", "0"))
                    out = "\n".join(str((ok or {}).get(k) or "") for k in ("stdout", "stderr")) if ok else \
                        json.dumps(result, ensure_ascii=False)
                    self._finish(target, ts, failed, int(code) if code and code.lstrip("-").isdigit() else None, out)
        elif kind == "result":
            self.running, self.want_start = False, False
            text = rec.get("result") if isinstance(rec.get("result"), str) else rec.get("subtype") or ""
            self.events.append(Event(ts, "end", first_line(str(text))))

    def _finish(self, ev: Event, ts: float, failed: bool, code: int | None, text: str) -> None:
        ev.done_ts = ts
        ev.result = "fail" if failed else "ok"
        if failed:
            ev.code = code if code not in (None, 0) else None
            ev.detail = [x for x in text.splitlines() if x.strip()][-2:]


class GrokTimeline(Timeline):
    """ACP JSON-RPC log of a Grok executor (host-dag-grok wrapper `grok.ndjson`, `{"direction", "message"}` lines).

    One `session/prompt` is one turn: running until `_x.ai/session/prompt_complete` or the wrapper's
    `observer` lifecycle record. Agent text arrives as chunks and is folded into one message event.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.msg: Event | None = None
        self.msg_text = ""

    def feed(self, rec: dict[str, Any]) -> None:
        msg = rec.get("message")
        if rec.get("direction") == "observer":
            if isinstance(rec.get("ts"), (int, float)):
                self.last_ts = max(self.last_ts, float(rec["ts"]))
            if rec.get("event") == "lifecycle" and rec.get("status") in {"completed", "failed", "cancelled"}:
                self._end(self.last_ts, str(rec.get("stopReason") or rec.get("status")), rec.get("status") != "completed")
            return
        if not isinstance(msg, dict):
            return
        method, params = msg.get("method"), msg.get("params") or {}
        meta = params.get("_meta") or {}
        ms = meta.get("agentTimestampMs")
        # 11-23: the agy ACP wrapper (host-dag-agy-acp-run) stamps each line with `ts`; Grok uses agentTimestampMs.
        stamp = rec.get("ts")
        ts = (float(ms) / 1000 if isinstance(ms, (int, float))
              else float(stamp) if isinstance(stamp, (int, float)) else self.last_ts)
        if ts:
            self.last_ts = max(self.last_ts, ts)
        if isinstance(meta.get("totalTokens"), int):
            self.tokens = meta["totalTokens"]
        if method in ("session/load", "session/new"):
            self.cwd = params.get("cwd") or self.cwd
        elif method == "_x.ai/models/update":
            self.model = params.get("currentModelId") or self.model
        elif method == "session/set_model":
            self.model = params.get("modelId") or self.model
        elif method == "session/prompt":
            text = " ".join(str(c.get("text", "")) for c in params.get("prompt") or [] if isinstance(c, dict))
            self.running, self.turn_started, self.msg = True, self.last_ts, None
            self.events.append(Event(self.last_ts, "start", first_line(text)))
        elif method == "_x.ai/session/prompt_complete":
            reason = str(params.get("stopReason") or "")
            self._end(ts, reason, reason not in {"", "end_turn"})
        elif method == "_x.ai/session/update":
            usage = (params.get("update") or {}).get("usage") or {}
            inp = int(usage.get("inputTokens") or 0)
            if inp:
                self.cached_ratio = int(usage.get("cachedReadTokens") or 0) / inp
        elif method == "session/update":
            self._update(params.get("update") or {}, ts)

    def _update(self, u: dict[str, Any], ts: float) -> None:
        kind = u.get("sessionUpdate")
        if kind == "usage_update" and isinstance(u.get("used"), int) and u["used"]:
            self.tokens = u["used"]  # agy ACP: context tokens in use
            return
        if kind == "agent_message_chunk":
            text = str((u.get("content") or {}).get("text") or "")
            if self.msg is None:
                self.msg_text = ""
                self.msg = Event(ts, "msg", "")
                self.events.append(self.msg)
            self.msg_text += text
            self.msg.text = first_line(self.msg_text)
            return
        if kind not in {"tool_call", "tool_call_update"}:
            return
        self.msg = None
        call_id = str(u.get("toolCallId") or "")
        ev = self.calls.get(call_id)
        if kind == "tool_call" and not isinstance(ev, Event):
            ev = Event(ts, "tool", "", result="running")
            self.events.append(ev)
            self.calls[call_id] = ev
        if isinstance(ev, Event) and (u.get("rawInput") or u.get("title")) and (ev.kind == "tool" or not ev.text):
            self._describe(ev, u)
        if not isinstance(ev, Event):
            return
        status = u.get("status")
        out = u.get("rawOutput") or {}
        code = str(out.get("exit_code")) if isinstance(out, dict) and out.get("exit_code") not in (None, "") else None
        if status == "failed" or (status == "completed" and code not in (None, "0")):
            ev.done_ts, ev.result = ts, "fail"
            ev.code = int(code) if code and code.lstrip("-").isdigit() else None
            text = str(out.get("output_for_prompt") or "") if isinstance(out, dict) else ""
            if not text:
                text = " ".join(output_text((c or {}).get("content")) if isinstance(c, dict) else str(c)
                                for c in u.get("content") or [])
            ev.detail = [x for x in text.splitlines() if x.strip()][-2:]
            self.calls.pop(call_id, None)
        elif status == "completed":
            ev.done_ts, ev.result = ts, "ok"
            self.calls.pop(call_id, None)

    @staticmethod
    def _describe(ev: Event, u: dict[str, Any]) -> None:
        """Name a call from its update; early updates may carry only the bare tool name as title."""
        raw = u.get("rawInput") or {}
        tool = (u.get("_meta") or {}).get("x.ai/tool") or {}
        kind = u.get("kind") or tool.get("kind")
        title = str(u.get("title") or "")
        command = raw.get("command") or raw.get("command_line")  # agy ACP names it command_line
        if kind == "execute" and command:
            ev.kind, ev.text = "shell", command_summary(str(command))
        elif kind == "edit" or raw.get("file_path") and "new_string" in raw:
            path = next((str(x.get("path")) for x in u.get("locations") or [] if isinstance(x, dict)), "")
            ev.kind, ev.text = "patch", path or str(raw.get("file_path") or title)
        elif title and (" " in title or not ev.text):
            arg = "" if " " in title else raw.get("pattern") or raw.get("target_file") or raw.get("path") or ""
            ev.text = first_line(f"{title.replace('`', '')} {arg}".strip(), 300)

    def _end(self, ts: float, reason: str, failed: bool) -> None:
        if self.running is False:
            return
        self.running, self.msg = False, None
        last = next((e.text for e in reversed(self.events) if e.kind == "msg"), "")
        self.events.append(Event(ts, "abort" if failed else "end", last if not failed else reason))


_CLI_LOG_CACHE: dict[str, Path] = {}
_RUN_FILES = ("cli-run.json", "*dispatch.json")
SIBLING_SCAN_LIMIT = 40


def _ticket_dir(path: Path) -> Path | None:
    """The ticket's evidence dir: the nearest ancestor (or self) that holds round-N dirs, or is named round-N's parent."""
    for p in [path, *path.parents][:6]:
        if p.name.startswith("round-") and p.parent != p:
            return p.parent
        if p.is_dir() and any(p.glob("round-*")):
            return p
    return None


def _unit_log(executor: str) -> Path | None:
    """A live CLI unit names its own output dir (`supervise --out <dir>`): the last resort when the
    coordinator's evidence field points somewhere the run file cannot be found from."""
    if not executor.startswith("host-dag-cli-"):
        return None
    try:
        out = subprocess.run(["systemctl", "--user", "show", "-p", "ExecStart", executor + ".service"],
                             capture_output=True, text=True, timeout=3, check=False).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    match = re.search(r"--out (\S+)", out)
    log = Path(match.group(1)) / "cli.ndjson" if match else None
    return log if log is not None and log.exists() else None


CLI_LOG_LOOKUP_TTL = 120.0  # seconds a lookup result (found or not) is reused
_CLI_LOG_LOOKUP: dict[tuple[str, str | None], tuple[float, Path | None]] = {}


def dispatched_unit(t: dict[str, Any]) -> str | None:
    """The CLI unit named by the run file beside the ticket's evidence, when it differs from ticket.executor.

    2026-10-22: five rounds dispatched at 09:50 updated the evidence but not the executor, so the board showed
    the previous round's log and the watcher tracked none of them."""
    evidence = t.get("evidence")
    if not evidence:
        return None
    base = Path(str(evidence))
    base = base.parent if base.suffix else base
    try:
        run_file = base / "cli-run.json"
        rec = json.loads(run_file.read_text())
        unit = None if rec.get("sideRun") else rec.get("unit")
        written = run_file.stat().st_mtime
    except (OSError, ValueError, AttributeError):
        return None
    if not (isinstance(unit, str) and unit and unit != t.get("executor")):
        return None
    # example: a ticket was redispatched to a Codex thread while its evidence still pointed at the 14:48 CLI
    # run, so the detail view showed the old unit (no log found) and only the full-screen peek found the session.
    started = thread_started_at(t.get("executor"))
    return None if started is not None and started > written else unit


def thread_started_at(thread: str | None) -> float | None:
    """Creation time of a Codex thread id (a UUIDv7 carries its millisecond timestamp), else None."""
    if not thread or not THREAD_RE.match(thread):
        return None
    try:
        u = uuid.UUID(thread)
    except ValueError:
        return None
    return (u.int >> 80) / 1000.0 if u.version == 7 else None


def effective_executor(t: dict[str, Any]) -> str | None:
    """The executor whose log describes the ticket now: the latest dispatched unit, else ticket.executor."""
    return dispatched_unit(t) or t.get("executor")


def find_cli_log(executor: str, evidence: str | None) -> Path | None:
    """find_cli_log_uncached, remembered for CLI_LOG_LOOKUP_TTL per (executor, evidence).

    2026-10-22: the board froze on opening a ticket's detail. Each refresh re-ran the evidence globs
    (two lookups per detail render), misses were never cached, and on a cold USB disk one lookup took
    up to 44 s. A new log appears within the TTL; a live CLI unit's log is still cached for good."""
    key = (executor, evidence)
    hit = _CLI_LOG_LOOKUP.get(key)
    now = time.monotonic()
    if hit is not None and now - hit[0] < CLI_LOG_LOOKUP_TTL and (hit[1] is None or hit[1].exists()):
        return hit[1]
    found = find_cli_log_uncached(executor, evidence)
    _CLI_LOG_LOOKUP[key] = (now, found)
    return found


def find_cli_log_uncached(executor: str, evidence: str | None) -> Path | None:
    """The log of a CLI executor: a Cursor/Claude unit's `cli.ndjson` found through the `cli-run.json` that
    names it, or a Grok session's ACP log found through the `*dispatch.json` that names its sessionId.

    The run file is matched by the executor's identity, not by where the coordinator happened to point the
    evidence (ticket dir, round dir, a result or review-dispatch file, a sibling attempt dir all occur).
    The evidence only locates the ticket; every round of that ticket is searched.
    """
    if not executor:
        return None
    if not evidence:
        return _unit_log(executor)
    cached = _CLI_LOG_CACHE.get(executor)
    if cached and cached.exists():
        return cached
    root = Path(evidence)
    if root.is_file() and (root.name == "cli-run.json" or root.name.endswith("dispatch.json")) \
            and executor in root.read_text(errors="replace"):
        candidates = [root]  # the run file of this very executor: that round's log, not the newest
    else:
        base = root.parent if root.is_file() else root
        ticket = _ticket_dir(base)
        candidates = [f for pattern in ("", "*/", "*/*/") for name in _RUN_FILES for f in base.glob(pattern + name)]
        if ticket is not None:
            candidates += [f for pattern in ("round-*/", "round-*/*/", "round-*/*/*/") for name in _RUN_FILES
                           for f in ticket.glob(pattern + name)]
        else:
            # 11-02: tickets without round-N dirs keep run-r2-*/ and review-r2/cli/ side by side; the ticket
            # evidence names the run dir, so a review's cli-run.json is only reachable from the parent.
            # Capped so an evidence field naming a batch dir never globs hundreds of ticket dirs.
            try:
                siblings = [d for d in base.parent.iterdir() if d.is_dir() and d != base]
            except OSError:
                siblings = []
            if len(siblings) <= SIBLING_SCAN_LIMIT:
                candidates += [f for d in siblings for pattern in ("", "*/") for name in _RUN_FILES
                               for f in d.glob(pattern + name)]
    found: list[tuple[float, Path]] = []
    for run_file in dict.fromkeys(candidates):
        try:
            rec = json.loads(run_file.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(rec, dict) or executor not in (rec.get("unit"), rec.get("sessionId")):
            continue
        log = Path(rec.get("log") or run_file.with_name("cli.ndjson"))
        if log.exists():
            found.append((log.stat().st_mtime, log))
    if not found:
        log = _unit_log(executor)
        if log is not None:
            _CLI_LOG_CACHE[executor] = log
        return log
    # A Grok session resumed over several rounds has one dispatch per round; the newest log is the live one.
    # Only a CLI unit name is unique to one run; a Grok sessionId spans rounds, so it is not cached.
    log = max(found)[1]
    if executor.startswith("host-dag-cli-"):
        _CLI_LOG_CACHE[executor] = log
    return log


CLAUDE_HOST_STATE = Path(os.environ.get("HOST_DAG_CLAUDE_HOST_STATE",
                                       str(Path.home() / ".local/state/host-dag-claude-host")))
CLAUDE_PROJECTS = Path(os.environ.get("HOST_DAG_CLAUDE_PROJECTS", str(Path.home() / ".claude" / "projects")))


def claude_host_record(thread: str) -> dict[str, Any]:
    try:
        rec = json.loads((CLAUDE_HOST_STATE / f"{thread}.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return rec if isinstance(rec, dict) else {}


def find_claude_session(thread: str) -> Path | None:
    """A host-dag-claude-host thread (`claude-<uuid>`, the Claude coordinator) is a Claude Code session:
    its transcript is ~/.claude/projects/<cwd with / and . as ->/<session>.jsonl."""
    rec = claude_host_record(thread)
    session, cwd = rec.get("session"), rec.get("cwd")
    if not session or not cwd:
        return None
    path = CLAUDE_PROJECTS / re.sub(r"[/.]", "-", str(cwd)) / f"{session}.jsonl"
    return path if path.exists() else None


class ClaudeSessionTimeline(CliTimeline):
    """A Claude Code session transcript: the same user/assistant records as stream-json, without the
    init/result events, so running comes from the claude-host turn record instead."""

    def __init__(self, path: Path, thread: str = "") -> None:
        super().__init__(path)
        self.thread = thread

    def reset(self) -> None:
        self.__init__(self.path, self.thread)

    def refresh(self, chunk: int = 4 << 20) -> None:
        super().refresh(chunk)
        turns = claude_host_record(self.thread).get("turns") or []
        last = turns[-1] if turns and isinstance(turns[-1], dict) else {}
        alive = False
        if last.get("status") == "running" and last.get("pid"):
            try:
                os.kill(int(last["pid"]), 0)
                alive = True
            except (OSError, ValueError):
                alive = False
        if last:
            self.running = alive
        if alive:
            self.turn_started = float(last.get("started") or self.turn_started or 0)
        self.model = self.model or claude_host_record(self.thread).get("model")

    def feed(self, rec: dict[str, Any]) -> None:
        if rec.get("type") == "user" and not self.want_start:
            content = (rec.get("message") or {}).get("content")
            if isinstance(content, str) or any(isinstance(c, dict) and c.get("type") == "text"
                                               for c in content or []):
                self.want_start = True  # each prompt sent to the session opens a turn
        if rec.get("type") == "user" and isinstance((rec.get("message") or {}).get("content"), str):
            rec = {**rec, "message": {**rec["message"], "content": [{"type": "text", "text": rec["message"]["content"]}]}}
        super().feed(rec)


class AgyTimeline(Timeline):
    """Stream-json log of an Antigravity CLI (`agy -p --output-format stream-json`) executor.

    11-19: agy executors were invisible on the board (no duration, last reply or log). agy records carry
    no timestamps, so host-dag-agy-run stamps each line with `ts` (epoch seconds); unstamped lines take the
    last time seen. One `agy -p` run is one turn: running from `init` until `result`.
    """

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.steps: dict[int, Event] = {}
        self.texts: dict[int, str] = {}

    def feed(self, rec: dict[str, Any]) -> None:
        ts = rec.get("ts")
        ts = float(ts) if isinstance(ts, (int, float)) else self.last_ts
        if ts:
            self.last_ts = max(self.last_ts, ts)
        kind = rec.get("event")
        if kind == "init":
            init = rec.get("init") or {}
            self.cwd = init.get("cwd") or self.cwd
            self.model = init.get("model") or self.model
            self.running, self.turn_started, self.want_start = True, ts, True
            return
        if kind == "result":
            res = rec.get("result") or {}
            usage = res.get("usage") or {}
            if isinstance(usage.get("total_tokens"), int):
                self.tokens = usage["total_tokens"]
            self.running, self.want_start = False, False
            status = str(res.get("status") or "")
            text = first_line(str(res.get("response") or "")) or status
            self.events.append(Event(ts, "end" if status in ("", "SUCCESS") else "abort", text))
            return
        if kind != "step_update":
            return
        step = rec.get("step_update") or {}
        idx, stype, state = step.get("step_index"), step.get("step_type"), step.get("state")
        if stype == "user_input":
            if self.want_start:
                self.events.append(Event(ts, "start", "agy 开始执行"))
                self.want_start = False
        elif stype == "agent_response":
            self.texts[idx] = self.texts.get(idx, "") + str(step.get("text_delta") or "")
            if state == "DONE" and self.texts.get(idx, "").strip():
                self.events.append(Event(ts, "msg", first_line(self.texts.pop(idx))))
        elif stype == "tool":
            info = step.get("tool_info") or {}
            name = str(step.get("tool_name") or info.get("name") or "tool")
            args = info.get("parameters") or {}
            ev = self.steps.get(idx)
            if ev is None:
                if name == "run_command":
                    ev = Event(ts, "shell", command_summary(str(args.get("CommandLine") or "")), result="running")
                elif name in {"write_to_file", "replace_file_content", "multi_replace_file_content", "sed_file"}:
                    target = args.get("TargetFile") or args.get("AbsolutePath") or args.get("File") or ""
                    ev = Event(ts, "patch", str(target), result="running")
                else:
                    ev = Event(ts, "tool", f"{name} {first_line(json.dumps(args, ensure_ascii=False), 300)}",
                               result="running")
                self.events.append(ev)
                self.steps[idx] = ev
            if state in ("DONE", "ERROR", "FAILED", "CANCELED", "CANCELLED"):
                self.steps.pop(idx, None)
                out = str(info.get("output") or "")
                m = re.search(r"exited with code (-?\d+)", out)
                code = int(m.group(1)) if m else None
                failed = state != "DONE" or (code not in (None, 0))
                ev.done_ts = ts
                ev.result = "fail" if failed else "ok"
                if failed:
                    ev.code = code if code not in (None, 0) else None
                    ev.detail = [x for x in out.splitlines() if x.strip()][-2:]


def cli_timeline_class(path: Path) -> type[Timeline]:
    """GrokTimeline for a Grok ACP log (lines carry a `direction`), AgyTimeline for agy stream-json
    (lines carry an `event`), CliTimeline for Cursor/Claude stream-json."""
    try:
        with open(path, "rb") as fh:
            head = fh.readline(1 << 16)
    except OSError:
        return CliTimeline
    if head.startswith(b'{"direction"'):
        return GrokTimeline
    return AgyTimeline if head.startswith(b'{"event"') else CliTimeline


def short_cmd(text: str, limit: int = 18) -> str:
    return text if width(text) <= limit else fit(text, limit - 1).rstrip() + "…"


def dur(seconds: float) -> str:
    s = max(0, int(seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s // 3600}h{(s % 3600) // 60:02d}m"


def dur_long(seconds: float) -> str:
    """Elapsed time in the two largest units that apply: 1w2d, 3d4h, 5h12m, 7m30s."""
    s = max(0, int(seconds))
    units = (("w", 7 * 86400), ("d", 86400), ("h", 3600), ("m", 60), ("s", 1))
    for i, (name, size) in enumerate(units):
        if s >= size or size == 1:
            big, rest = divmod(s, size)
            if size == 1:
                return f"{big}s"
            nxt_name, nxt_size = units[i + 1]
            small = rest // nxt_size
            return f"{big}{name}{small}{nxt_name}" if small else f"{big}{name}"
    return "0s"


CLI_STAMP_RE = re.compile(r"-(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})$")


def executor_started(executor: str | None, year: int) -> float | None:
    """When an executor session began, from its id alone (no log read): a Codex thread id is a UUIDv7
    (its first 48 bits are Unix milliseconds); a CLI unit ends in MMDDHHMMSS."""
    if not executor:
        return None
    if THREAD_RE.match(executor) and executor[14] == "7":
        return int(executor[:8] + executor[9:13], 16) / 1000
    m = CLI_STAMP_RE.search(executor)
    if m:
        mo, d, h, mi, se = (int(x) for x in m.groups())
        try:
            return datetime(year, mo, d, h, mi, se).timestamp()
        except ValueError:
            return None
    return None


def _stamp(value: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(value)).timestamp() if value else None
    except ValueError:
        return None


def ticket_span(t: dict[str, Any], now: float) -> tuple[float, float] | None:
    """(start, end) of a ticket: its executor session start to acceptance, its last update once it
    failed, or now while it is open. None when the start is unknown or the ticket was canceled."""
    if ts.derive_state(t) == ts.CANCELED:  # its updated_at moves with later bookkeeping, not with work
        return None
    year = datetime.fromtimestamp(now).year
    start = executor_started(t.get("executor"), year)
    if start is None:
        return None
    if start > now:  # a CLI stamp from last December read with this year's number
        start = executor_started(t.get("executor"), year - 1) or start
    end = _stamp(t.get("accepted_at"))
    # example: a reused cut ticket showed 用时 0 for a day. The cut ticket is reused per build, so its
    # accepted_at (b22, 08:19) predated the b23 executor start; a stamp from before this session or on a
    # ticket that is open again does not end the span.
    if end is not None and (end < start or ts.derive_state(t) not in ts.DONE):
        end = None
    if end is None and ts.derive_state(t) in ts.TERMINAL:
        end = _stamp(t.get("updated_at"))
    return start, max(start, end if end is not None else now)


def batch_span(batch: dict[str, Any], now: float) -> tuple[float, float] | None:
    """First ticket start to the last ticket end (now while the batch is not done)."""
    spans = [sp for t in (batch.get("tickets") or {}).values() if (sp := ticket_span(t, now))]
    if not spans:
        return None
    end = max(e for _, e in spans) if batch_done(batch) else now
    return min(s for s, _ in spans), end


def clock(ts: float) -> str:
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S") if ts else "--:--:--"


KIND_LABEL = {"start": "▶ 开始", "msg": "💬 消息", "shell": "⚙ shell", "patch": "✎ patch",
              "tool": "⚙ tool", "end": "■ 结束", "abort": "✗ 中止"}

TOOL_KINDS = {"shell", "patch", "tool"}
# peek/coord Tab cycle (user 11-10): 全部 → 只看消息 → 只看工具 → 全部.
FILTER_ALL, FILTER_MSG, FILTER_TOOL = "all", "msg", "tool"
PEEK_FILTERS = (FILTER_ALL, FILTER_MSG, FILTER_TOOL)
PEEK_FILTER_LABEL = {FILTER_ALL: "全部", FILTER_MSG: "只看消息", FILTER_TOOL: "只看工具"}


def next_peek_filter(current: str | None) -> str:
    """The next state of the tri-state Tab cycle; anything unknown starts over at 全部."""
    try:
        return PEEK_FILTERS[(PEEK_FILTERS.index(current or FILTER_ALL) + 1) % len(PEEK_FILTERS)]
    except ValueError:
        return FILTER_ALL


def peek_filter(value: str | bool | None, mode: str | None = None) -> str:
    """Normalize the old boolean ``tools_only`` (CLI --tools) and the new three-state mode."""
    chosen = mode if mode is not None else value
    if isinstance(chosen, bool):
        return FILTER_TOOL if chosen else FILTER_ALL
    return chosen if chosen in PEEK_FILTERS else FILTER_ALL


def event_facets(ev: Event, prev_msg: str = "") -> set[str]:
    """Which filters an event belongs to. A closing message is BOTH a message and the turn-end
    marker: filtering by content keeps its text instead of dropping the whole row. System-only
    events (an empty end, an abort) belong to 全部 alone."""
    if ev.kind in {"start", "msg"}:
        return {FILTER_MSG}
    if ev.kind in TOOL_KINDS:
        return {FILTER_TOOL}
    if ev.kind == "end" and ev.text and ev.text != prev_msg:
        return {FILTER_MSG}
    return set()


EVENT_INDENT = 19   # width of "HH:MM:SS  " + the 9-column kind label
DETAIL_PREFIX = "          │ "


def shorten(text: str, aliases: tuple[tuple[str, str], ...] | list[tuple[str, str]] = ()) -> str:
    """Replace long path prefixes (run dir, worktree root, home) with short aliases, longest first."""
    for prefix, alias in sorted(aliases, key=lambda a: -len(a[0])):
        if prefix:
            text = text.replace(prefix, alias)
    return text


def event_lines(tl: Timeline, tools_only: bool = False, now: float | None = None,
                aliases: tuple[tuple[str, str], ...] | list[tuple[str, str]] = (),
                mode: str | None = None) -> list[tuple[str, str]]:
    """Rendered timeline as (text, style) where style is ok/fail/running/dim/''.

    ``mode`` is the peek/coord tri-state (全部/只看消息/只看工具); the legacy boolean
    ``tools_only`` still selects the tools state for the CLI."""
    now = now or time.time()
    chosen = peek_filter(tools_only, mode)
    out: list[tuple[str, str]] = []
    turn_start, prev_msg = 0.0, ""
    for ev in tl.events:
        if ev.kind == "start":
            turn_start = ev.ts
        facets = event_facets(ev, prev_msg)
        if ev.kind == "msg":
            prev_msg = ev.text
        if chosen != FILTER_ALL and chosen not in facets:
            continue
        label = KIND_LABEL.get(ev.kind, ev.kind)
        body = shorten(ev.text, aliases)
        text = f'"{body}"' if ev.kind in {"start", "msg", "end"} else body
        if ev.kind == "end" and (not ev.text or ev.text == prev_msg):
            # The closing message was already shown as the last 💬 line; the end row reports the turn
            # instead (2026-10-27: repeated text read as "a message, then it ended at once").
            text = f"本轮用时 {dur_long(ev.ts - turn_start)}" if turn_start else "本轮结束"
        if ev.result == "ok":
            tail, style = f"  ✓ {dur(ev.done_ts - ev.ts)}", "ok"
        elif ev.result == "fail":
            tail, style = f"  ✗ exit {ev.code}" if ev.code is not None else "  ✗ 失败", "fail"
        elif ev.result == "running" and tl.running:
            tail, style = f"  ⏳ 运行中 {dur(now - ev.ts)}", "running"
        elif ev.result == "running":
            tail, style = "  … 无结果", "dim"
        else:
            tail, style = "", ""
        out.append((f"{clock(ev.ts)}  {pad(label, 9)}{text}{tail}", style))
        for d in ev.detail:
            out.append((f"{DETAIL_PREFIX}{shorten(d, aliases)}", "dim"))
    return out


def wrap(line: list[tuple[str, str]], cols: int, indent: int, max_lines: int) -> list[list[tuple[str, str]]]:
    """Wrap styled segments to ``cols`` display columns. Continuation rows start with ``indent``
    spaces; past ``max_lines`` rows the last one ends with '…'. cols <= 0 returns the line whole."""
    if cols <= 0:
        return [line]
    limit = cols - 1
    rows: list[list[tuple[str, str]]] = [[]]
    used = 0
    for text, st in line:
        buf = ""
        for ch in text:
            w = width(ch)
            if used + w > limit:
                if buf:
                    rows[-1].append((buf, st))
                rows.append([(" " * indent, "")])
                buf, used = "", indent
            buf += ch
            used += w
        if buf:
            rows[-1].append((buf, st))
    if len(rows) <= max_lines:
        return rows
    rows = rows[:max_lines]
    last, room = [], limit - 1
    for text, st in rows[-1]:
        part = fit(text, room)
        if part:
            last.append((part, st))
        room -= width(part)
    rows[-1] = last + [("…", rows[-1][-1][1] if rows[-1] else "")]
    return rows


# ---------------------------------------------------------------- terminal width helpers


def width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 0 if unicodedata.combining(ch) else 1
               for ch in text)


def fit(text: str, cols: int) -> str:
    out, used = [], 0
    for ch in text:
        w = width(ch)
        if used + w > cols:
            break
        out.append(ch)
        used += w
    return "".join(out)


def pad(text: str, cols: int) -> str:
    text = fit(text, cols)
    return text + " " * (cols - width(text))

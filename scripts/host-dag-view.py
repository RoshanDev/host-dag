#!/usr/bin/env python3
"""Read-only Host DAG views, plus the coordinator's ticket-state writer.

  host-dag peek KV13-03 [-f] [--tools] [--role spec]   one executor's timeline
  host-dag board                                       curses panel (read-only)
  host-dag runs [--add DIR] [--scan ROOT]              registered runs and their batches
  host-dag batch set --batch V13 --dag ... --accepted-tip ...      coordinator only
  host-dag ticket set --batch V13 --ticket KV13-03 --status CLAIMED ...  coordinator only

The run directory comes from --run, then $HOST_DAG_RUN, then the running host-dag watcher unit.
"""
# The docstring above is the argparse description: it is printed by `host-dag --help` and must stay
# byte-identical for the live coordinator (B4 review P2-1), so the stage B4 notes live here instead.
#
# Stage B4 (docs/local-dev/dag-state-machine-design-20261019.md §15): when <run>/sm/state.db exists,
# `ticket set` / `batch set` are also translated into one atomic core command (shadow mirror; run.json
# stays the authority, output and exit code never change). Core-only commands, which need state.db and
# the pinned interpreter (hostdag_dag_core.SM_PYTHON), are not listed in the top-level usage/help:
#   host-dag exec request --ticket T --role executor --mode new_attempt|resume|adopt ...
#   host-dag question ask|link ...      host-dag decide ...      host-dag decisions --pending
#   host-dag wait add|update|waive ...  host-dag pagewindow enqueue|policy ...
#   host-dag authority switch ...       host-dag sm quiesce|resume|status
# `host-dag graph [-o FILE|-]` exports the dependency graph as text; also left out of the top-level usage.
# `host-dag activity` says whether work is really running, including sessions on already-accepted tickets.
# Each has its own `--help` (e.g. `host-dag exec request --help`).

from __future__ import annotations

from hostdag_json import public_path, read_json

import argparse
import contextlib
import datetime as _dt
import copy
import hashlib
import json
import math
import os
import re
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import hostdag_dag_view as v  # noqa: E402
from hostdag_display import memory_text
from hostdag_view_cache import JsonFile, Snapshot, cache_path, clock_text, jsonl_rows

Seg = tuple[str, str]   # (text, style)
Line = list[Seg]


# ---------------------------------------------------------------- run discovery


def match_runs(key: str) -> list[Path]:
    """Registered runs whose id prefix, directory name or a batch name matches key."""
    hits = []
    for path in v.known_runs():
        if not (path / "run.json").is_file():
            continue
        label = run_label(path)
        names = []
        try:
            names = list(v.batches(v.load_json(path / "run.json")))
        except v.ViewError:
            pass
        if label.startswith(key) or path.name == key or key in names:
            hits.append(path)
    return hits


def find_run(arg: str | None) -> Path:
    """--run / $HOST_DAG_RUN as a directory, or a short key from `host-dag runs` (run id prefix,
    directory name or batch name); otherwise the run of the active host-dag watcher."""
    for cand in (arg, os.environ.get("HOST_DAG_RUN")):
        if cand:
            path = Path(cand).expanduser()
            if (path / "run.json").is_file():
                return path
            hits = match_runs(cand)
            if len(hits) == 1:
                return hits[0]
            if hits:
                raise v.ViewError(f"{cand} matches several runs: " + ", ".join(run_label(h) for h in hits))
            raise v.ViewError(f"no run.json in {path} and no registered run matches {cand!r}; see `host-dag runs`")
    try:
        out = subprocess.run(
            ["systemctl", "--user", "show", "--state=active", "-p", "ExecStart", "host-dag-*watch*"],
            capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        out = ""
    for m in re.finditer(r"--state\s+(\S+/watch\.json)", out):
        path = Path(m.group(1)).parent
        if (path / "run.json").is_file():
            return path
    raise v.ViewError("cannot find the run directory; pass --run DIR or set HOST_DAG_RUN")


class Run:
    """run.json + watch.json snapshot with cached executor timelines."""

    def __init__(self, path: Path, *, load: bool = True, json_source: JsonFile | None = None) -> None:
        self.path = path
        self.timelines: dict[Path, v.Timeline] = {}
        self.by_thread: dict[str, v.Timeline] = {}
        self.run: dict[str, Any] = {}
        self.watch: dict[str, Any] = {}
        self.leftovers: list[dict[str, Any]] = []
        # mtime+size keyed parse reuse with stage timings (user 11-10); read-only.
        self.json = json_source or JsonFile()
        if load:
            self.reload()

    def load_json(self, path: Path) -> dict[str, Any]:
        try:
            value = self.json.load(path)
        except FileNotFoundError as exc:
            raise v.ViewError(f"not found: {path}") from exc
        except (OSError, ValueError) as exc:
            raise v.ViewError(f"not valid JSON: {path}: {exc}") from exc
        if not isinstance(value, dict):
            raise v.ViewError(f"expected an object: {path}")
        return value

    def reload(self) -> None:
        self.run = self.load_json(self.path / "run.json")
        v.normalize_executors(self.run)
        try:
            self.watch = self.load_json(self.path / "watch.json")
        except v.ViewError:
            self.watch = {}
        v.mark_live(self.run, self.watch)
        v.mark_lock_waits(self.run, self.path)
        self.leftovers = v.load_leftovers(self.path)
        v.mark_leftovers(self.run, self.leftovers)
        v.mark_moved(self.run)
        v.USER_DECISION_KEYS = v.load_user_decisions(self.path)

    def batches(self) -> dict[str, dict[str, Any]]:
        return v.batches(self.run)

    def advisor_answer(self, key: str) -> dict[str, Any] | None:
        """Latest raw entry, including provisional guidance; callers must check finality."""
        path = self.path / "advisor/log.jsonl"
        try:
            rows = jsonl_rows(path)
        except OSError:
            return None
        hits = [a for a in rows if a.get("key") == key or str(a.get("key", "")).endswith(key)]
        return hits[-1] if hits else None

    def freeze_owner(self) -> str:
        """Owner of the live board freeze (control/locks/board-freeze.json), or "" when there is none."""
        try:
            return str(read_json(self.path / "control/locks/board-freeze.json")
                       .get("owner") or "?")
        except (OSError, ValueError):
            return ""

    def all_tickets(self) -> dict[str, Any]:
        """Every ticket of every batch by id: a wait may name a ticket of another batch (T-A -> T-B)."""
        return v.all_tickets(self.run)

    def observer_view(self) -> list[Line]:
        """One read-only scan at a time; slow evidence disks must never hold the curses input loop."""
        if not hasattr(self, "_observer"):
            self._observer = {"lines": [], "finished": 0.0, "worker": None, "error": ""}
        state = self._observer
        worker = state["worker"]
        if (worker is None or not worker.is_alive()) and time.time() - state["finished"] >= 30:
            def scan() -> None:
                try:
                    # A separate Run keeps timeline parsing off the UI's mutable coord cache.
                    state["lines"] = observer_lines(Run(self.path))
                    state["error"] = ""
                except Exception as exc:
                    state["error"] = f"读取失败：{type(exc).__name__}"
                finally:
                    state["finished"] = time.time()

            worker = state["worker"] = threading.Thread(target=scan, name="host-view-observer", daemon=True)
            worker.start()
        status = "扫描中（可 Esc/o 返回）" if worker.is_alive() else "每 30 秒刷新"
        if state["finished"]:
            status += " · 上次扫描结束 " + observer_time(state["finished"])
        if state["error"]:
            status += " · " + state["error"]
        lines = state["lines"] or [[(s + " · 等待只读扫描", "dim")] for s in (
            "守护单元", "未消费事件", "协调者", "顾问通知队列", "最近 10 条 watcher 唤醒 / self-heal / nudge")]
        return [[("观察器 · " + status, "accent")], *lines]

    def cached_timeline(self, thread: str | None) -> v.Timeline | None:
        """The timeline if it was already loaded, refreshed; never parses a new log (keeps the board fast)."""
        tl = self.by_thread.get(thread or "")
        if tl is not None:
            tl.refresh()
        return tl

    def timeline(self, thread: str | None) -> v.Timeline | None:
        tl = self._timeline(thread)
        if tl is not None and thread:
            self.by_thread[thread] = tl
        return tl

    def _timeline(self, thread: str | None) -> v.Timeline | None:
        """A Codex thread's rollout, the stream-json log of a Cursor/Claude CLI unit, or a Grok ACP log."""
        if not thread:
            return None
        path, cls = (v.find_rollout(thread), v.Timeline) if v.THREAD_RE.match(thread) else (None, v.Timeline)
        if path is None and thread.startswith("claude-"):  # the Claude coordinator (host-dag-claude-host)
            path = v.find_claude_session(thread)
            if path is None:
                return None
            tl = self.timelines.get(path)
            if tl is None:
                tl = self.timelines[path] = v.ClaudeSessionTimeline(path, thread)
            tl.refresh()
            return tl
        if path is None:  # a CLI unit, or a Grok session (its id looks like a Codex thread id)
            evidence = next((t.get("evidence") for b in self.batches().values()
                             for t in (b.get("tickets") or {}).values() if t.get("executor") == thread), None)
            if evidence is None:
                evidence = next((review.get("evidence") or t.get("evidence")
                                 for b in self.batches().values()
                                 for t in (b.get("tickets") or {}).values()
                                 for review in (t.get("reviews") or {}).values()
                                 if review.get("thread") == thread), None)
            path = v.find_cli_log(thread, evidence)
            cls = v.cli_timeline_class(path) if path else v.CliTimeline
        if path is None:
            return None
        tl = self.timelines.get(path)
        # 11-20 user: an agy log first seen while still empty (the runner creates cli.ndjson before agy's
        # first event) was parsed as Cursor/Claude for the board's whole life; re-sniff and swap the parser.
        if tl is None or type(tl) is not cls:
            tl = self.timelines[path] = cls(path)
        tl.refresh()
        return tl

    def aliases(self) -> list[tuple[str, str]]:
        """Display-only path aliases: the run dir, the shared worktree root and home."""
        home = Path.home()
        return [(str(self.path), "$RUN"), (str(home / "Developer/.worktrees/host-dag"), "$WT"), (str(home), "~")]

    def coordinator(self) -> str:
        return self.watch.get("parent_thread") or self.run.get("coordinator_session") or ""

    def coordinator_label(self) -> str:
        return coordinator_label(self.coordinator())

    def heartbeat_age(self) -> float | None:
        try:
            return time.time() - float((self.path / "watch.json.heartbeat").read_text().strip())
        except (OSError, ValueError):
            return None

    def resolve(self, target: str, role: str) -> tuple[str, str | None, dict[str, Any]]:
        """(label, thread, ticket record) for a ticket id or thread id. role: exec/spec/standards."""
        if v.THREAD_RE.match(target):
            return target, target, {}
        for batch in self.batches().values():
            t = (batch.get("tickets") or {}).get(target)
            if t is not None:
                if role == "exec":
                    return target, t.get("executor"), t
                return f"{target} {role} 审查", ((t.get("reviews") or {}).get(role) or {}).get("thread"), t
        tracked = self.watch.get("tracked") or {}
        if role == "exec":
            rec = tracked.get(target)
        else:
            rec = next((r for k, r in tracked.items() if k.startswith(target) and k.endswith(f"review-{role}")), None)
        if rec is None:
            raise v.ViewError(f"{target}: not in run.json batches or watch.json tracked")
        if not rec.get("thread"):
            raise v.ViewError(f"{target}: {rec.get('provider', 'CLI')} executor, peek reads Codex threads only;"
                              f" log: {rec.get('log', '?')}")
        return target if role == "exec" else f"{target} {role} 审查", rec["thread"], {}


# ---------------------------------------------------------------- pure renderers


# ---- background units (view only) ----------------------------------------------------------------
# An executor turn can end after it starts a long systemd --user unit (cold install, packaging); the
# coordinator then parks the ticket as WAITING_RESOURCE / WAIT-UNTIL while that unit does the work.
# The board shows such a ticket as 执行中 with the unit and its progress.json phase. Nothing is
# written: run.json state and hostdag_ticket_state.derive_state are untouched for other callers.
BG_TTL = 20.0
BG_OVERRIDE = {"res", "sched", "wait", "ready", "hold"}  # never ask/cand (need a person) or closed rows
BG_ACTIVE = {"active", "activating", "reloading"}
BG_SKIP = re.compile(r"watch|chatgpt-chrome|nudge|rotation")  # long-lived infrastructure, not ticket work
BG_UNIT = re.compile(r"(?<![A-Za-z0-9_/.@-])(host-dag-[A-Za-z0-9_.@-]*[A-Za-z0-9_@-])")
BG_AFTER_UNIT = re.compile(r"\bunits?\s*[:=]?\s*`?([A-Za-z0-9][A-Za-z0-9_.@]*-[A-Za-z0-9_.@-]*[A-Za-z0-9_@])")
BG_SUFFIXES = (".service", ".scope")
_BG_CACHE: dict[str, tuple[float, dict[str, tuple[str, str]]]] = {}


def _bg_names(text: str) -> list[str]:
    return BG_UNIT.findall(text) + BG_AFTER_UNIT.findall(text)


def bg_progress(t: dict[str, Any]) -> dict[str, Any]:
    """The ticket's progress.json beside (or inside) its evidence path; {} when absent or unreadable."""
    evidence = str(t.get("evidence") or "").strip()
    if not evidence:
        return {}
    try:
        path = Path(evidence)
        path = path / "progress.json" if path.is_dir() else path if path.name == "progress.json" \
            else path.parent / "progress.json"
        if not path.is_file() or path.stat().st_size > 1 << 20:
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def ticket_units(t: dict[str, Any]) -> tuple[list[str], str]:
    """Candidate unit names (without .service) and the progress.json phase for one ticket.

    Sources: progress.json ``unit`` / ``units``, the current blocker text (before the first 【历史】),
    and a ``host:`` wait_on that names a unit."""
    progress = bg_progress(t)
    names: list[str] = []
    unit = progress.get("unit")
    if isinstance(unit, str):
        names.append(unit.strip())
    units = progress.get("units")
    for value in (units.values() if isinstance(units, dict) else units if isinstance(units, list) else ()):
        if isinstance(value, str) and "/" not in value and " " not in value.strip():
            names.append(value.strip())
    names += _bg_names(str(t.get("blocker") or "").split("【历史】", 1)[0])
    wait_on = str(t.get("wait_on") or "")
    if wait_on.startswith("host:"):
        names += BG_UNIT.findall(wait_on)
    out: list[str] = []
    for name in names:
        name = name.removesuffix(".service")
        if name and not name.endswith("@") and not BG_SKIP.search(name) and name not in out:
            out.append(name)
    phase = progress.get("phase")
    return out, phase.strip() if isinstance(phase, str) else ""


def unit_states(names: list[str]) -> dict[str, str]:
    """{unit name: ActiveState} from one ``systemctl --user show``; any error gives {} (= not running)."""
    if not names:
        return {}
    args = [n if n.endswith(BG_SUFFIXES) else n + ".service" for n in names]
    try:
        out = subprocess.run(["systemctl", "--user", "show", "-p", "Id", "-p", "ActiveState", "--no-pager", "--", *args],
                             capture_output=True, text=True, timeout=3).stdout
    except (OSError, subprocess.SubprocessError, ValueError):
        return {}
    states: dict[str, str] = {}
    for block in str(out or "").split("\n\n"):
        props = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        unit = props.get("Id", "")
        if unit:
            states[unit.removesuffix(".service")] = props.get("ActiveState", "")
    return states


def background_running(r: "Run", now: float | None = None, ttl: float = BG_TTL) -> dict[str, tuple[str, str]]:
    """{ticket id: (active unit, phase)} for open tickets with a running background unit; cached per run."""
    now = time.time() if now is None else now
    key = str(r.path)
    hit = _BG_CACHE.get(key)
    if hit and 0 <= now - hit[0] < ttl:
        return hit[1]
    out: dict[str, tuple[str, str]] = {}
    try:
        tickets = r.all_tickets()
        wanted: dict[str, tuple[list[str], str]] = {}
        for tid, t in tickets.items():
            if not isinstance(t, dict) or v.ts.derive_state(t, tickets) in v.ts.TERMINAL:
                continue
            names, phase = ticket_units(t)
            if names:
                wanted[tid] = (names, phase)
        states = unit_states(sorted({n for names, _ in wanted.values() for n in names}))
        for tid, (names, phase) in wanted.items():
            live = [n for n in names if states.get(n) in BG_ACTIVE]
            if live:
                out[tid] = (live[0], phase)
    except Exception:  # noqa: BLE001 - a display hint must never take the board down
        out = {}
    _BG_CACHE[key] = (now, out)
    return out


def display_state(batch: dict[str, Any], tid: str, everything: dict[str, Any] | None = None,
                  bg: dict[str, tuple[str, str]] | None = None) -> tuple[str, str]:
    key, phase = v.display_state(batch, tid, everything)
    hit = (bg or {}).get(tid)
    if hit and key in BG_OVERRIDE:
        return "run", f"后台运行中 {hit[0]}" + (f" · {hit[1]}" if hit[1] else "")
    return key, phase


def bg_counts(batch: dict[str, Any], everything: dict[str, Any], p: Any,
              bg: dict[str, tuple[str, str]]) -> tuple[dict[str, int], dict[str, int]]:
    """progress() counts and wait stages with background-running rows moved to 执行中."""
    counts, stages = dict(p.counts), dict(p.stages)
    for tid, t in (batch.get("tickets") or {}).items():
        if tid not in bg:
            continue
        base = v.display_state(batch, tid, everything)[0]
        if base in BG_OVERRIDE:
            counts[base] -= 1
            counts["run"] += 1
            if base == "wait":
                stages[v.wait_stage(t)] -= 1
    return counts, stages


def state_seg(batch: dict[str, Any], tid: str, everything: dict[str, Any] | None = None,
              bg: dict[str, tuple[str, str]] | None = None) -> Seg:
    key, phase = display_state(batch, tid, everything, bg)
    glyph, label = v.DISPLAY[key]
    if phase.startswith("后台运行中"):
        phase = "后台"  # unit and phase go in the 最后活动 column / detail; the state cell is narrow
    if key == "move":
        return f"{glyph} {label} {phase}", key
    return f"{glyph} {label}{'·' + phase if phase else ''}", key


def model_text(t: dict[str, Any], tl: v.Timeline | None) -> str:
    model = t.get("model") or (tl.model if tl else "")
    effort = t.get("effort") or (tl.effort if tl else "")
    if not model:
        return "—"
    return f"{model}/{effort[:1]}" if effort else model


def session_text(t: dict[str, Any], phase: str) -> str:
    reviews = t.get("reviews") or {}
    if phase == "审查" and reviews:
        mark = {"PASS": "✓", "FAIL": "✗"}
        return " ".join(f"{'std' if a == 'standards' else a}{mark.get(str((reviews.get(a) or {}).get('verdict')), '▶')}"
                        for a in v.REVIEW_AXES if a in reviews)
    return short_thread(t.get("executor"))


def last_text(t: dict[str, Any], key: str, tl: v.Timeline | None) -> str:
    if key in {"ask", "hold", "ready", "wait", "res", "fail"} and t.get("blocker"):
        return memory_text(t["blocker"])  # e.g. runnable by the DAG but scheduled later by the coordinator
    if key == "ok" and t.get("accepted_at"):
        return f"{t['accepted_at'][11:16]} 验收"
    if key == "left":
        return memory_text(t.get(v.LEFTOVER_MARK) or t.get("blocker") or "转入遗留清单")
    if key == "sup":
        return memory_text(t.get("blocker") or "作废")
    if key == "run" and t.get("blocker") and not (tl is not None and tl.running):
        return memory_text(t["blocker"])  # executor idle between rounds: show why it is waiting
    if tl is not None and key in {"run", "cand"}:
        return tl.last_activity()
    return ""




def coordinator_label(thread: str) -> str:
    """Who coordinates: a host-dag-claude-host session `claude-<uuid>` is Claude, a bare UUID is a Codex thread."""
    if not thread:
        return "—"
    if thread.startswith("claude-"):
        return f"Claude·{thread[len('claude-'):][:8]}"
    if v.THREAD_RE.match(thread):
        return f"Codex·{thread[:8]}"
    return thread[:12]


def header(r: Run, batch: dict[str, Any], hot: list[str] | tuple[str, ...] = ()) -> Line:
    coord = r.coordinator()
    ctl = r.timeline(coord)
    cstate = (("未知", "dim") if ctl.running is None else
              ("运行中", "run") if ctl.running else ("空闲", "ok")) if ctl else ("", "dim")
    age = r.heartbeat_age()
    if not r.watch:  # e.g. a run coordinated from another run's watcher
        wstate = ("—（本 run 无）", "dim")
    else:
        wstate = (f"✓ {v.dur(age)}", "ok") if age is not None and age < 120 else \
            (f"⚠ {v.dur(age)}" if age is not None else "⚠ 无心跳", "fail")
    gaps = r.watch.get("registration_gaps") or {}
    if gaps:
        wstate = (f"⚠ 漏登记 {len(gaps)}", "fail")
    rkey, rtext = v.remote_state(batch)
    wake = v.wake_status(r.watch, bool(ctl and ctl.running))
    return [("Host DAG", "accent"), (f"  run {run_label(r.path).split('/')[0]}  协调者 {coordinator_label(coord)} ", ""), cstate,
            ("  观察器 ", ""), wstate, *([("  ", ""), (wake, "run")] if wake else []),
            ("  ", ""), (rtext, {"ok": "ok", "warn": "fail"}.get(rkey, "dim")),
            # Reported, never repaired: the hot-path manifest says these are symlinks.
            *([("  ", ""), (f"⚠ 软链断开 {len(hot)}：{'、'.join(hot)}", "fail")] if hot else []),
            (f"  {time.strftime('%H:%M')}", "dim")]


def meter(n: int, total: int, style: str, cols: int = 24) -> list[Seg]:
    f = round(n / total * cols) if total else 0
    return [("█" * f, style), ("░" * (cols - f), "dim")]


COLS = (("票", 9), ("名称", 22), ("状态", 14), ("用时", 7), ("轮次", 11), ("等待", 16), ("依赖", 10), ("模型", 16), ("会话", 12),
        ("响应", 13), ("最后活动", 34))


def ticket_cols(batch: dict[str, Any]) -> tuple[tuple[str, int], ...]:
    """The id column fits the longest id (KV18-03-FIX-SNAP-ORPHAN cut to 9 looked like KV18-03);
    the name column gives the width back."""
    longest = max((v.width(tid) for tid in batch.get("tickets") or {}), default=7)
    grow = max(0, min(longest + 1, 26) - COLS[0][1])
    cols = [(COLS[0][0], COLS[0][1] + grow), (COLS[1][0], max(12, COLS[1][1] - grow)), *COLS[2:]]
    waits = [v.width(t[v.LOCK_WAIT_MARK]) + 2 for t in (batch.get("tickets") or {}).values()
             if isinstance(t, dict) and t.get(v.LOCK_WAIT_MARK)]
    if waits:
        cols[5] = (cols[5][0], max(cols[5][1], *waits))
    return tuple(cols)


def last_response(t: dict[str, Any], tl: v.Timeline | None) -> float:
    """Latest sign of life for a ticket: its executor/review session's last event or its last update."""
    stamps = [v.parse_ts(t.get(k)) for k in ("updated_at", "accepted_at")]
    stamps.append(tl.last_ts if tl is not None else 0.0)
    return max(stamps)


def response_text(ts: float, now: float | None = None) -> str:
    if not ts:
        return "—"
    age = (now or time.time()) - ts
    if age >= 86400:
        return time.strftime("%m-%d %H:%M", time.localtime(ts))
    return f"{time.strftime('%H:%M', time.localtime(ts))} {v.dur(age)}前"


def undone_deps(t: dict[str, Any], tickets: dict[str, Any]) -> list[str]:
    return [d for d in t.get("deps") or [] if d not in tickets
            or v.ts.derive_state(tickets[d], tickets) not in v.ts.DONE]


CAPACITY_NAMES = {"local-memory": "本机内存", "host-memory": "本机内存", "host-io": "本机磁盘 I/O"}


def wait_short(t: dict[str, Any], key: str, tickets: dict[str, Any] | None = None,
               freeze_owner: str | None = None) -> str:
    """Name what the ticket waits on: the ticket(s), or the resource and its holder.

    freeze_owner is the owner of control/locks/board-freeze.json ("" when there is none, None when
    unknown). A ticket queued behind the freeze reads as such, and as waiting on the coordinator once
    the freeze is gone (2026-10-27: two reviews showed "等前置" for 25 minutes after it cleared)."""
    tickets = tickets or {}
    if t.get(v.LOCK_WAIT_MARK):
        return t[v.LOCK_WAIT_MARK]
    if freeze_owner is not None and str(t.get("blocker") or "").startswith("board-freeze") \
            and v.ts.derive_state(t) in v.ts.WAITING:
        return f"冻结排队（{freeze_owner} 窗口）" if freeze_owner else "⚠ 冻结已解除，待派"
    req = t.get("page_request") or {}
    if req.get("status") in ("pending", "claimed"):
        if req["status"] == "claimed":
            return "页面运行中"
        queue = [x["page_request"] for x in tickets.values()
                 if isinstance(x, dict) and (x.get("page_request") or {}).get("status") == "pending"]
        order = sorted(queue, key=lambda r: (-int(r.get("priority") or 0), r.get("approved_at") or ""))
        pos = next((i + 1 for i, r in enumerate(order) if r.get("id") == req.get("id")), None)
        return f"页面队列 #{pos}" if pos else "页面队列"
    if t.get("wait_on"):
        kind, _, detail = str(t["wait_on"]).partition(":")
        if kind == "ticket":
            target = detail or t.get("wait_holder")
            return f"票 {target}" if target else "未写明哪张票"
        if kind == "capacity" and detail:
            name = next((label + detail[len(k):] for k, label in CAPACITY_NAMES.items() if detail.startswith(k)),
                        f"{v.WAIT_KINDS['capacity']} {detail}")
            return name + (f" {t['wait_holder']}" if t.get("wait_holder") else "")
        if v.milestone_label(t["wait_on"]):
            return v.milestone_label(t["wait_on"])
        if v.user_decision_label(t["wait_on"]):
            return v.user_decision_label(t["wait_on"])
        return v.WAIT_KINDS.get(kind, kind) + (f" {t['wait_holder']}" if t.get("wait_holder") else "")
    if key == "wait":
        if not t.get("deps"):
            return "未写明等什么"  # waiting state with neither deps nor wait_on
        pending = undone_deps(t, tickets)
        if not pending:
            return "前置都已验收"
        return f"票 {pending[0]}" + (f" 等{len(pending)}张" if len(pending) > 1 else "")
    return "—"


def wait_stale(r: "Run", t: dict[str, Any], tickets: dict[str, Any]) -> str:
    """Why a declared wait looks over: its ticket is accepted, the page is free, the advisor answered."""
    kind, _, detail = str(t.get("wait_on") or "").partition(":")
    target = detail or (t.get("wait_holder") if kind == "ticket" else None)
    if kind == "ticket" and target in tickets and v.ts.derive_state(tickets[target], tickets) in v.ts.DONE:
        return f"{target} 已验收，不该再等"
    if not t.get("wait_on") and t.get("deps") and not undone_deps(t, tickets):
        return "前置都已验收，可以派了"
    if not t.get("wait_on") and not t.get("deps") and t.get("state") in ("WAITING_DEPENDENCY", "WAITING_RESOURCE"):
        return "没写明在等什么（无前置、无 wait_on），见说明"
    if kind == "decision" and detail and not detail.startswith("PAGE-WINDOW-LIB:"):  # library owner's action
        answer = r.advisor_answer(detail)
        if v.is_final_answer(answer):
            return f"顾问 {time.strftime('%H:%M', time.localtime(float(answer.get('at') or 0)))} 已裁定，等协调者执行"
    m = re.search(r"WAIT-UNTIL:(\S+)", t.get("blocker") or "")
    if m and 0 < v.parse_ts(m.group(1)) < time.time() - 300:
        return f"WAIT-UNTIL {m.group(1)[5:16].replace('T', ' ')} 已过期"
    if kind == "page-window":
        page = v.load_json(r.path / "control/api-page-validation-pause.json") or {}
        if not page.get("owner") and page.get("status") == "RESUMED":
            return "页面空闲，等的是排期"
    return ""


def wait_detail(r: "Run", t: dict[str, Any], tickets: dict[str, Any]) -> Line:
    """What the ticket waits on and who holds it right now, read live (registry, advisor log, tickets)."""
    kind, _, detail = str(t.get("wait_on") or "").partition(":")
    if kind == "ticket" and not detail:
        detail = t.get("wait_holder") or ""
    holder = t.get("wait_holder") if t.get("wait_holder") != detail else None  # "等 X · 占用 X" says nothing
    line: Line = [(v.wait_label(f"{kind}:{detail}" if detail else kind, holder) if kind else "—", "run")]
    if not kind:
        pending = undone_deps(t, tickets)
        line = [(f"等前置票 {', '.join(pending)}" if pending else
                 "前置都已验收" if t.get("deps") else "没有前置票，也没写 wait_on", "run")]
    since = v.parse_ts(t.get("updated_at"))
    if kind == "page-window":
        page = v.load_json(r.path / "control/api-page-validation-pause.json") or {}
        if page.get("owner"):
            at = float(page.get("at") or 0)
            line += [(f"   当前占用 {page['owner']}（{page.get('status')}，{response_text(at)}起）", "hold")]
        elif page.get("status") == "DRIFT_REVIEW":
            line += [(f"   页面被 {(page.get('review') or {}).get('owner')} 的漂移审查挡住，等顾问", "hold")]
        else:
            line += [("   页面现在空闲（RESUMED）——可以开窗，等的是协调者排期", "ask")]
    elif kind == "decision":
        tid = t.get("_id") or ""
        log = r.path / "advisor/log.jsonl"
        latest = None
        try:
            for rec in jsonl_rows(log):
                key = str(rec.get("key") or "")
                # The wait names the advisor question it waits on (often asked by another ticket);
                # without one, fall back to questions that carry this ticket's id.
                if (detail and (key == detail or key.endswith(detail) or detail.endswith(key))) or (
                        not detail and tid and tid in key):
                    latest = rec
        except (OSError, ValueError):
            pass
        if detail.startswith("PAGE-WINDOW-LIB:"):
            # Page-library work (implement / land) is the library owner's action, not an advisor
            # question; it lands only while the page is free, so show who holds the page now.
            page = v.load_json(r.path / "control/api-page-validation-pause.json") or {}
            held = (f"页面现被 {page['owner']} 占用（{page.get('status')}），空闲后落地" if page.get("owner")
                    else "页面空闲，等库负责人落地")
            line += [(f"   页面库负责人（advisor-session）实施/落地中 · {held}", "hold")]
        elif latest is None:
            line += [("   顾问日志里没有本票的提问", "fail")]
        elif v.is_provisional_answer(latest):
            line += [(f"   暂行，待 Fable：{memory_text(str(latest.get('title') or ''))[:40]}", "hold")]
        elif v.is_final_answer(latest):
            when = float(latest.get("at") or 0)
            late = " ——已裁定，等协调者执行" if when >= since - 60 else ""
            line += [(f"   顾问 {response_text(when)} {latest.get('verdict')}：{memory_text(str(latest.get('title') or ''))[:40]}{late}",
                      "ask" if late else "dim")]
        else:
            line += [(f"   顾问处理中：{memory_text(str(latest.get('title') or ''))[:40]}", "run")]
    elif kind == "ticket" and detail in tickets:
        other = tickets[detail]
        home = next((n for n, b in r.batches().items() if detail in (b.get("tickets") or {})), "")
        line += [(f"   {home + ' 批的 ' if home and detail not in (r.batches().get(t.get('_batch') or '', {}).get('tickets') or {}) else ''}"
                  f"{detail} 当前 {v.ts.derive_state(other, tickets)}，{response_text(v.parse_ts(other.get('updated_at')))}更新", "dim")]
    elif kind == "ticket":
        line += [(f"   找不到票 {detail}", "fail")]
    stale = wait_stale(r, t, tickets)
    if stale:
        line += [(f"   ⚠ {stale}", "ask")]
    return line


def cell(text: str, cols: int) -> str:
    """Pad to a column, keeping one space before the next column; cut with … when too long."""
    if v.width(text) > cols - 1:
        text = v.fit(text, cols - 2) + "…"
    return v.pad(text, cols - 1) + " "


def short_thread(thread: str | None) -> str:
    """Last 8 hex digits: Codex thread ids start with a timestamp, so prefixes collide."""
    match = re.search(r"-review-(spec|standards)-(\d+)$", thread or "")
    if match:
        return f"{'sp' if match[1] == 'spec' else 'std'}-{match[2][-6:]}"
    return thread[-8:] if thread else "—"


# h cycles the board rows: live work only (default) → plus queued → everything → live work only.
FOLD_ALL, FOLD_CLOSED, FOLD_LIVE = 0, 1, 2
LIVE_KEYS = {"run", "cand", "ask"}  # executing or under review, plus decisions that need someone now


def open_defect_line(batch: dict[str, Any], everything: dict[str, Any], cols: int = 0) -> Line:
    """One row: this batch's open defects, highest priority first, unknown priority last as 未定.
    With a width the row stops at a whole ticket id and ends with "… +N" (never cuts an id)."""
    rows = v.open_defects(batch, everything)
    if not rows:
        return [("本版未闭环缺陷 ", "dim"), ("0 张", "ok")]
    line: Line = [("本版未闭环缺陷 ", "fail"), (f"{len(rows)} 张 · ", "fail")]
    used = sum(v.width(text) for text, _ in line)
    limit = cols - 1 if cols else 0
    for i, row in enumerate(rows):
        rank = row["priority"]
        style = "fail" if rank is not None and rank <= 1 else ("hold" if rank is None else "dim")
        item = [(" · ", "dim")] if i else []
        item.append((f"{row['priority_label']} {row['ticket']}", style))
        need = sum(v.width(text) for text, _ in item)
        rest = len(rows) - i - 1
        tail = v.width(f" · … +{rest}") if rest else 0
        if limit and used + need + tail > limit:
            line.append((f"{' · ' if i else ''}… +{len(rows) - i}", "dim"))
            return line
        line += item
        used += need
    return line


def moved_only(old, new) -> bool:
    """Two board read requests that differ only in the selected row (index 2)."""
    if not isinstance(old, (list, tuple)) or len(old) != len(new):
        return False
    return all(i == 2 or list(a) == list(b) if isinstance(a, (list, tuple)) else i == 2 or a == b
               for i, (a, b) in enumerate(zip(old, new)))


def reselect(lines: list[Line], row0: int, old: int, new: int, count: int) -> list[Line]:
    """Copy of board lines with the cursor highlight moved from ticket row `old` to `new`."""
    def unsel(row):
        return [(t, "" if st == "sel" else st[:-4] if str(st).endswith("+sel") else st) for t, st in row]
    def sel(row):
        return [(t, f"{st}+sel" if st else "sel") for t, st in unsel(row)]
    out = list(lines)
    if 0 <= old < count and row0 + old < len(out):
        out[row0 + old] = unsel(out[row0 + old])
    if 0 <= new < count and row0 + new < len(out):
        out[row0 + new] = sel(out[row0 + new])
    return out


def board_lines(r: Run, name: str, sel: int, flt: str = "", show_all: bool = False,
                hide_ok: int = FOLD_ALL, hot: list[str] | tuple[str, ...] = (),
                cols: int = 0, meta: dict[str, int] | None = None) -> tuple[list[Line], list[str]]:
    """Board lines and the ticket ids in row order (for selection); hide_ok is the fold mode
    (True = FOLD_CLOSED: fold closed tickets; FOLD_LIVE: keep only live work). cols is the
    screen width (0 = unknown); rows that list ids use it to stop at a whole id."""
    names = v.visible_batch_names(r.run, show_all, keep=name)
    hidden = len(r.batches()) - len(names)
    batch = r.batches()[name]
    everything = r.all_tickets()
    p = v.progress(batch, everything)
    bg = background_running(r)
    lines: list[Line] = [header(r, batch, hot), []]
    tabs: Line = [("批次 ", "dim")]
    for n in names:
        label = f" {'✔ ' if v.batch_done(r.batches()[n]) else ''}{n} "
        tabs += [(label, "sel") if n == name else (label, "dim"), (" ", "")]
    if hidden:
        tabs.append((f"（另有 {hidden} 个已结束，b 展开）", "dim"))
    lines.append(tabs)
    span = v.batch_span(batch, time.time())
    lines.append([("名称 ", "dim"), (v.batch_title(batch) or "—", "accent"),
                  *([("   用时 ", "dim"), (v.dur_long(span[1] - span[0]), "")] if span else []),
                  (f"   {v.shorten(batch.get('dag') or '', r.aliases())}", "dim")])
    lines.append([("已实现 ", ""), (f"{p.implemented}/{p.total} ", ""), *meter(p.implemented, p.total, "cand"),
                  ("  交过候选，含已验收", "dim")])
    tip = (batch.get("accepted_tip") or "")[:7]
    lines.append([("已验收 ", ""), (f"{p.accepted}/{p.total} ", ""), *meter(p.accepted, p.total, "ok"),
                  (f"  已进集成分支 tip {tip or '—'}", "dim")])
    c, stages = bg_counts(batch, everything, p, bg)
    summary: Line = [(f"需决定 {c['ask']}", "ask"), (" · ", "dim"), (f"外部阻塞 {c['hold']}", "hold"),
                     (" · ", "dim"), (f"执行中 {c['run']}", "run"), (" · ", "dim"),
                     (f"可执行 {c['ready']}", ""), (" · ", "dim"), (f"等资源 {c['res']}", "hold"),
                     (" · ", "dim")]
    for i, key in enumerate(v.WAIT_STAGES):
        if i:
            summary.append((" · ", "dim"))
        summary.append((f"{v.WAIT_STAGE_LABEL[key]} {stages[key]}", v.WAIT_STAGE_STYLE[key]))
    summary += ([(f" · 失败 {c['fail']}", "fail")] if c["fail"] else []) + \
               ([(f" · 作废 {c['sup']}", "sup")] if c["sup"] else []) + \
               ([(f" · 转遗留 {c['left']}", "left")] if c["left"] else []) + \
               ([(f" · 移至其他批次 {c['move']}", "move")] if c["move"] else []) + \
               ([("（不计入总数）", "dim")] if p.superseded else [])
    lines.append(summary)
    lines.append(open_defect_line(batch, everything, cols))
    lines.append(flow_line(r, batch))
    lines.append([])
    cols = ticket_cols(batch)
    lines.append([(cell(h, w), "dim") for h, w in cols])
    order = [tid for tid in v.ticket_order(batch) if not flt or flt.lower() in tid.lower()
             or flt in state_seg(batch, tid, everything, bg)[0] or flt in ((batch.get("tickets") or {})[tid].get("title") or "")]
    folded = 0
    if hide_ok == FOLD_LIVE:
        kept = [tid for tid in order if display_state(batch, tid, everything, bg)[0] in LIVE_KEYS]
        folded, order = len(order) - len(kept), kept
    elif hide_ok:
        # Fold every closed ticket (accepted, integrated, canceled/superseded, failed); keep live work in view.
        kept = [tid for tid in order
                if v.ts.derive_state((batch.get("tickets") or {})[tid], everything) not in v.ts.TERMINAL]
        folded, order = len(order) - len(kept), kept
    tickets = batch.get("tickets") or {}
    now = time.time()
    freeze = r.freeze_owner()  # one read per refresh: the file sits on the USB disk
    if meta is not None:
        meta["row0"] = len(lines)  # ticket i is drawn on line row0 + i (lets the UI move the cursor locally)
    for i, tid in enumerate(order):
        t = tickets[tid]
        text, key = state_seg(batch, tid, everything, bg)
        phase = display_state(batch, tid, everything, bg)[1]
        # Parsing a session log costs ~0.1 s; only live tickets need it, the rest reuse a loaded one.
        if t.get("status") != "CLAIMED":
            tl = None
        elif key in {"run", "cand"}:
            tl = r.timeline(v.effective_executor(t))
        else:
            tl = r.cached_timeline(t.get("executor"))
        deps = ",".join(d.rsplit("-", 1)[-1] for d in t.get("deps") or []) or "—"
        wait = wait_short(t, key, everything, freeze)
        if wait != "—" and wait_stale(r, t, everything):
            wait = "⚠ " + wait
        budget = v.round_budget(t)
        span = v.ticket_span(t, now)
        cells = [(tid, ""), (t.get("title") or "—", "" if t.get("title") else "dim"), (text, key),
                 (v.dur_long(span[1] - span[0]), "" if key == "run" else "dim") if span else ("—", "dim"),
                 (budget[0], "fail") if budget and budget[1] else (budget[0], "") if budget else ("—", "dim"),
                 (wait, "hold" if wait != "—" else "dim"), (deps, ""),
                 (model_text(t, tl), "" if t.get("model") else "dim"), (session_text(t, phase), ""),
                 (response_text(last_response(t, tl)), "dim"),
                 (phase if phase.startswith("后台运行中") else last_text(t, key, tl), "")]
        row = [(cell(x, w), st) for (x, st), (_, w) in zip(cells, cols)]
        if i == sel:
            row = [(s, f"{st}+sel" if st else "sel") for s, st in row]
        lines.append(row)
    if not order:
        lines.append([("（没有票" + ("匹配过滤条件" if flt else "") + "）", "dim")])
    if folded and hide_ok == FOLD_LIVE:
        lines.append([(f"（只显示执行中/审查中/需决定，已收起 {folded} 张；h 显示排队的）", "dim")])
    elif folded:
        lines.append([(f"（已收起 {folded} 张已结束：已验收/作废/失败，h 展开）", "dim")])
    return lines, order


def flow_line(r: Run, batch: dict[str, Any]) -> Line:
    """Flow v2 summary: leftover rows of this batch, the batch closure (when recorded), the cumulative candidate."""
    # The ledger is append-only: count each item once at its latest status, and only open items as 遗留.
    latest: dict[Any, dict[str, Any]] = {}
    for i, row in enumerate(v.batch_leftovers(batch, r.leftovers)):
        latest[row.get("id") or ("row", i)] = row
    status = [str(row.get("status") or "").upper() for row in latest.values()]
    closed = sum(s in {"CLOSED", "DROPPED"} for s in status)
    scheduled = status.count("SCHEDULED")
    line: Line = [("新流程 ", "dim"), (f"遗留 {len(status) - closed} 条", "left")]
    if scheduled:
        line.append((f"（其中 {scheduled} 条已排进修复票）", "dim"))
    if closed:
        line.append((f" · 已关 {closed} 条", "dim"))
    closure = v.batch_closure(batch)
    if closure:
        line += [(" · ", "dim"), ("收口 ", "dim"), (closure, "ok" if closure == "验收通过" else "ask")]
    cand = v.candidate_set(r.path)
    line.append((" · ", "dim"))
    if cand is None:
        line.append(("累计候选未建", "dim"))
    elif cand.get("error"):
        line.append((f"累计候选 ⚠ {cand['error']}", "fail"))
    else:
        repos = " ".join(f"{n} {sha}" for n, sha in cand["repos"]) or "—"
        line += [("累计候选 ", "dim"), (cand["branch"] or "—", "accent"), (f"  {repos}", "")]
    return line


def displayed_reviews(r: Run, ticket: dict) -> dict:
    """Fallback only to explicitly named review directories; never scan old attempts."""
    reviews = ticket.get('reviews') or {}
    if reviews:
        return reviews
    found = {}
    for value in re.findall(r'[^\s`\"\'<>，。；;,()]+', str(ticket.get('blocker') or '')):
        path = Path(value.rstrip(':：'))
        if not path.name.startswith('review-'):
            continue
        path = path if path.is_absolute() else r.path / path
        dispatch = path / 'dispatch.out'
        try:
            if dispatch.stat().st_size > 1024 * 1024:
                continue
            text = dispatch.read_text()
        except OSError:
            continue
        # CLI receipts can include wrapper prose before the JSON object.
        match = re.search(r'"(?:threadId|thread_id|thread)"\s*:\s*"([^"\s]+)"', text)
        if match:
            found[path.name] = {'thread': match[1], 'evidence': str(dispatch)}
    return found


def session_tabs(r: Run, ticket: dict) -> list[str]:
    """Tab order in the detail view: executor, spec, then any other review session that has a thread.

    11-20 user: policy-v3 reviews only on the spec axis; an always-empty `standards —` tab was noise, so it
    (like any other axis) shows only when a session is registered on it."""
    extra = [k for k, rec in displayed_reviews(r, ticket).items()
             if k != "spec" and isinstance(rec, dict) and rec.get("thread")]
    extra.sort(key=lambda k: (v.REVIEW_AXES.index(k) if k in v.REVIEW_AXES else len(v.REVIEW_AXES), k))
    return ["exec", "spec", *extra]


def detail_lines(r: Run, name: str, tid: str, tab: str) -> list[Line]:
    batch = r.batches()[name]
    tickets = batch.get("tickets") or {}
    t = tickets[tid]
    everything = r.all_tickets()
    text, key = state_seg(batch, tid, everything, background_running(r))
    tl = r.timeline(v.effective_executor(t))
    succ = [x for x in v.ticket_order(batch) if tid in (tickets[x].get("deps") or [])]
    deps: Line = []
    for d in t.get("deps") or []:
        dk = v.display_state({"tickets": everything}, d, everything)[0] if d in everything else "fail"
        deps += [(f"{d} ", ""), (v.DISPLAY[dk][0], dk), ("  ", "")]
    reviews = displayed_reviews(r, t)
    unreg = v.dispatched_unit(t)
    rv: Line = [("执行 ", "dim"), (short_thread(v.effective_executor(t)), "sel" if tab == "exec" else ""),
                *([("（新一轮，未登记）", "fail")] if unreg else [])]
    for axis in session_tabs(r, t)[1:]:
        rec = reviews.get(axis) or {}
        verdict = rec.get("verdict")
        rv += [(f"   {axis} ", "dim"), (short_thread(rec.get("thread")), "sel" if tab == axis else ""),
               (" ✓" if verdict == "PASS" else " ✗" if verdict == "FAIL" else "", "ok" if verdict == "PASS" else "fail")]

    def kv(k: str, val: Line | str) -> Line:
        return [(v.pad(k, 8), "dim")] + ([(val, "")] if isinstance(val, str) else val)

    lines: list[Line] = [
        [(tid, "accent"), (f"  {t.get('title') or ''}   ", ""), (text, key),
         (f"  {v.ts.derive_state(t, everything)}{'' if v.ts.explicit_state(t) else '（推导）'}"
          f"  r{t.get('round') or 1} · 第 {v.ts.attempt_of(t)} 次执行", "dim"),
         *([(f"  {budget[0]}", "fail" if budget[1] else "dim")] if (budget := v.round_budget(t)) else [])],
        [],
        kv("范围", t.get("scope") or t.get("title") or "—"),
        kv("依赖", (deps or [("—", "")]) + [(f"   后继 {', '.join(succ) or '—'}", "dim")]),
        kv("模型", model_text(t, tl)),
        kv("基线", f"{(t.get('base') or '—')[:8]}   候选 {(t.get('candidate') or '—（未交）')[:10]}"
                 f"   分支 {t.get('branch') or '—'}"),
        kv("会话", rv),
        kv("证据", v.shorten(t.get("evidence") or "—", r.aliases())),
    ]
    lines.append(kv("响应", response_text(last_response(t, tl))))
    req = t.get("page_request") or {}
    if req:
        lines.append(kv("页面请求", f"{req.get('status')}  {req.get('window') or req.get('id')}  优先级 {req.get('priority', 0)}"
                                  f"  批准 {str(req.get('approved_at') or '')[11:16]}"
                                  + (f"  结果 {req.get('outcome')}" if req.get("outcome") else "")
                                  + (f"  {req.get('reason')}" if req.get("reason") else "")))
    if t.get("pending_action"):
        act = t["pending_action"]
        lines.append(kv("待办", [(f"{act.get('kind')}  {str(act.get('at') or '')[11:16]}  "
                                  f"{act.get('outcome') or act.get('verdict') or act.get('accepted') or ''}", "ask")]))
    if t.get("wait_on") or key in {"wait", "res"}:
        lines.append(kv("等待", t.get(v.LOCK_WAIT_MARK) or
                        wait_detail(r, {**t, "_id": tid, "_batch": name}, r.all_tickets())))
    if t.get("blocker"):
        lines.append(kv("说明", [(memory_text(t["blocker"]), key)]))
    role_label = {"exec": "执行者", "spec": "规格审查", "standards": "规范审查"}.get(tab, tab)
    if v.ts.derive_state(t, everything) == v.ts.REVIEWING and tab != "exec":
        # Tab picks which session the log shows: a review axis shows that review, exec falls through below.
        current = [(kind, rec) for kind, rec in reviews.items()
                   if kind == tab and isinstance(rec, dict) and rec.get("thread")]
        if not current:
            lines += [[], [(f"{role_label}：本轮没有登记审查会话（Tab 切换会话）", "dim")]]
        for kind, rec in current:
            review_tl = r.timeline(rec["thread"])
            tier = rec.get("tier") or t.get("review_tier") or t.get("tier") \
                or "档位未登记"
            lines += [[], [(f"{kind} 审查 · {tier} · {model_text(rec, review_tl)} · thread {rec['thread']}"
                            "（最近 20 条，Tab 切换会话，f 全屏）", "dim")]]
            if review_tl is None:
                lines.append([("（没有找到这个审查会话的记录）", "dim")])
            else:
                lines += [[seg] for seg in v.event_lines(review_tl, aliases=r.aliases())[-20:]]
        return lines
    lines += [[], [(f"{role_label}过程（最近 8 条，Tab 切换会话，f 全屏）", "dim")]]
    thread = v.effective_executor(t) if tab == "exec" else (reviews.get(tab) or {}).get("thread")
    stl = r.timeline(thread)
    if stl is None:
        lines.append([("（没有找到这个会话的记录）", "dim")])
    else:
        lines += [[seg] for seg in v.event_lines(stl, aliases=r.aliases())[-8:]]
    return lines


def peek_lines(label: str, thread: str, tl: v.Timeline | None, t: dict[str, Any], tools_only: bool | str = False,
               aliases: list[tuple[str, str]] | None = None, *, mode: str | None = None) -> list[Line]:
    al = aliases or []
    state = v.peek_filter(tools_only, mode)
    lines: list[Line] = [[(label, "accent"), (f"  thread {thread}  {model_text(t, tl)}", ""),
                          (f"  · {v.PEEK_FILTER_LABEL[state]}", "run")]]
    if tl is None:
        where = ("Claude 会话（~/.local/state/host-dag-claude-host/" + thread + ".json）" if thread.startswith("claude-")
                 else "rollout-*-" + thread + ".jsonl")
        return lines + [[("找不到会话记录 " + where, "fail")]]
    lines.append([("状态   ", "dim"), (tl.status_line(), "run" if tl.running else "")])
    if tl.truncated:
        lines.append([('仅解析最近 4MiB；完整历史保留在原日志', 'dim')])
    lines.append([("工作树 ", "dim"), (v.shorten(t.get("worktree") or tl.cwd or "—", al), "")])
    lines.append([("─" * 70, "dim")])
    events = v.event_lines(tl, mode=state, aliases=al)
    if not events:
        empty = {v.FILTER_MSG: "无消息", v.FILTER_TOOL: "无工具调用"}.get(state, "无事件")
        lines.append([(empty, "dim")])
    lines += [[seg] for seg in events]
    lines.append([("─" * 70, "dim")])
    lines.append([(f"记录 {v.shorten(str(tl.path), al)}", "dim")])
    return lines


def direct_prereqs(t: dict[str, Any]) -> list[str]:
    """Declared edges only: never include prerequisites of a prerequisite."""
    kind, _, detail = str(t.get("wait_on") or "").partition(":")
    target = (detail or t.get("wait_holder")) if kind == "ticket" else None
    return list(dict.fromkeys(list(t.get("deps") or []) + ([target] if target else [])))


def graph_prereqs(t: dict[str, Any], everything: dict[str, Any]) -> list[str]:
    """Edges for the dependency graph: only a ticket that is still waiting hangs under its
    prerequisites. A dispatched (running/reviewing) ticket already had them met; keeping those edges
    loops back (11-11: r18 kept deps on its three fixes, which now wait for r18 to accept them,
    so 90+ tickets waiting on r18 were drawn flat instead of under it)."""
    if v.ts.derive_state(t, everything) in v.ts.WAITING:
        return direct_prereqs(t)
    return []


def dependency_cycles(prereqs: dict[str, list[str]]) -> list[list[str]]:
    """Each distinct cycle among the given edges once, as [a, b, ..., a]."""
    color: dict[str, int] = {}
    found: list[list[str]] = []
    seen_sets: set[frozenset[str]] = set()
    for start in prereqs:
        if color.get(start):
            continue
        stack: list[tuple[str, int]] = [(start, 0)]
        path: list[str] = []
        while stack:
            node, i = stack.pop()
            if i == 0:
                color[node] = 1
                path.append(node)
            deps = prereqs.get(node, [])
            if i < len(deps):
                stack.append((node, i + 1))
                nxt = deps[i]
                if color.get(nxt) == 1:
                    loop = path[path.index(nxt):] + [nxt]
                    if frozenset(loop) not in seen_sets:
                        seen_sets.add(frozenset(loop))
                        found.append(loop)
                elif not color.get(nxt):
                    stack.append((nxt, 0))
            else:
                color[node] = 2
                path.pop()
    return found


def dependency_state_seg(batch: dict[str, Any], tid: str, everything: dict[str, Any]) -> Seg:
    seg = state_seg(batch, tid, everything)
    t = everything[tid]
    if seg[1] == "wait" or (seg[1] == "res" and str(t.get("wait_on") or "").partition(":")[0] == "ticket"):
        pending = [d for d in direct_prereqs(t) if d not in everything
                   or v.ts.derive_state(everything[d], everything) not in v.ts.DONE]
        if pending:
            return f"{v.DISPLAY[seg[1]][0]} 等 {','.join(pending)}", seg[1]
    return seg


def tree_view(r: Run, name: str) -> list[Line]:
    batch = r.batches()[name]
    everything = r.all_tickets()
    p = v.progress(batch, everything)
    head: Line = [(f"{name} 依赖树   ", "accent")]
    for key, n in bg_counts(batch, everything, p, background_running(r))[0].items():
        if n:
            head += [(f"{v.DISPLAY[key][0]} {n}  ", key)]
    lines: list[Line] = [head, [("上面的先做，下面的等上面", "dim")], []]
    for text, tid in v.tree_lines(batch):
        if text.endswith("↑ 见上"):
            lines.append([(text[:-4], ""), ("↑ 见上", "dim")])
        else:
            lines.append([(text + " ", ""), dependency_state_seg(batch, tid, everything)])
    return lines


def graph_view(r: Run) -> list[Line]:
    """Open tickets without unfinished prerequisites are roots; their successors hang below.
    Missing prerequisites remain visible as diagnostic roots. Shared successors expand once."""
    batch_of = {tid: name for name, b in r.batches().items() for tid in (b.get("tickets") or {})}
    everything = r.all_tickets()

    def done(tid: str) -> bool:
        name = batch_of.get(tid)
        return name is not None and v.display_state(r.batches()[name], tid, everything)[0] in ("ok", "sup", "left", "move")

    open_ids = [tid for tid in batch_of if not done(tid)]
    prereqs = {tid: [d for d in graph_prereqs(everything[tid], everything) if not done(d)] for tid in open_ids}
    children: dict[str, list[str]] = {tid: [] for tid in open_ids}
    for tid, deps in prereqs.items():
        for dep in deps:
            children.setdefault(dep, []).append(tid)
    roots = [tid for tid in children if not prereqs.get(tid)]
    lines: list[Line] = [[("全局依赖图（未完成的票，跨批次）   ", "accent"), (f"{len(open_ids)} 张未完成", "dim")],
                         [("上面的先做，下面的等上面", "dim")]]
    for loop in dependency_cycles(prereqs):
        # A cycle hides its whole subtree from the roots (11-11: 90+ tickets waiting on r18 looked
        # unattached). Name it so the data gets fixed instead of the picture being misread.
        lines.append([("⚠ 循环依赖：", "fail"), (" → ".join(loop), "fail"),
                      ("（环上的票及其后继平铺在下面，需改 deps/wait_on）", "dim")])
    lines.append([])
    freeze = r.freeze_owner()  # one read per refresh, not one per node
    seen: set[str] = set()

    def walk(tid: str, prefix: str, last: bool, root: bool) -> None:
        branch = "" if root else ("└─ " if last else "├─ ")
        name = batch_of.get(tid)
        if tid in seen:
            lines.append([(f"{prefix}{branch}{tid} ", ""), ("↑ 见上", "dim")])
            return
        seen.add(tid)
        if name is None:
            lines.append([(f"{prefix}{branch}{tid} ", ""), ("不在本 run", "fail")])
        else:
            t = everything[tid]
            kind = str(t.get("wait_on") or "").partition(":")[0]
            res = f"  等 {wait_short(t, '', everything, freeze)}" if kind and kind != "ticket" else ""
            lines.append([(f"{prefix}{branch}{tid} ", ""), (f"[{name}] ", "dim"),
                          dependency_state_seg(r.batches()[name], tid, everything), (res, "res" if res else "")])
        kids = children[tid]
        nxt = "" if root else prefix + ("   " if last else "│  ")
        for i, kid in enumerate(kids):
            walk(kid, nxt, i == len(kids) - 1, False)

    for tid in roots:
        walk(tid, "", True, True)
    for tid in open_ids:  # anything only reachable through a cycle
        if tid not in seen:
            walk(tid, "", True, True)
    return lines


def graph_export_text(r: Run, generated: str) -> str:
    """Plain-text snapshot of the whole board: summary, live tickets, open dependency tree, non-ticket
    waits, then every ticket with its declared prerequisites. Read-only; run.json stays the authority."""
    tickets = r.all_tickets()
    batches = r.batches()
    batch_of = {tid: name for name, b in batches.items() for tid in (b.get("tickets") or {})}
    bg = background_running(r)

    def label(tid: str) -> str:
        return state_seg(batches[batch_of[tid]], tid, tickets, bg)[0].strip()

    def shown(tid: str) -> str:
        return f"{tid} [{label(tid)}] {str(tickets[tid].get('title') or '')[:46]}".rstrip()

    def finished(tid: str) -> bool:
        return v.display_state(batches[batch_of[tid]], tid, tickets)[0] in ("ok", "sup", "left", "move")

    open_ids = [tid for tid in batch_of if not finished(tid)]
    states: dict[str, int] = {}
    per_batch: dict[str, int] = {}
    for tid in open_ids:
        states[label(tid)] = states.get(label(tid), 0) + 1
        per_batch[batch_of[tid]] = per_batch.get(batch_of[tid], 0) + 1
    rule = "-" * 60
    out = ["Host DAG 全局依赖图（只读快照）", f"生成时间：{generated}", f"数据源：{r.path / 'run.json'}",
           "权威数据仍是 run.json，这份文本不会随看板更新。",
           "只含登记的依赖（deps 和 wait_on=ticket:*），写在说明文字里的前置不在图上。", "",
           "一、概要", rule, f"批次 {len(batches)} 个，票 {len(batch_of)} 张；未结束 {len(open_ids)} 张。",
           "未结束按状态：" + "，".join(f"{k} {n}" for k, n in sorted(states.items(), key=lambda x: -x[1])),
           "未结束按批次：" + "，".join(f"{k} {n}" for k, n in sorted(per_batch.items(), key=lambda x: -x[1])), "",
           "二、当前在动的票", rule]
    moving = [tid for tid in sorted(open_ids) if v.ts.derive_state(tickets[tid], tickets) in (v.ts.RUNNING, v.ts.REVIEWING)
              or display_state(batches[batch_of[tid]], tid, tickets, bg)[1].startswith("后台运行中")]
    out += [f"{shown(tid)}  更新 {str(tickets[tid].get('updated_at') or '')[:16]}" for tid in moving] or ["无"]
    out += ["", "三、未结束票的依赖树（上面的先做，下面的等上面）", rule]
    out += [ansi(line, False) for line in graph_view(r)[3:]]
    out += ["", "四、非票类等待（裁定、资源等）", rule]
    waits = [f"{shown(tid)}  等 {tickets[tid]['wait_on']}" for tid in sorted(open_ids)
             if str(tickets[tid].get("wait_on") or "").partition(":")[0] not in ("", "ticket")]
    out += waits or ["无"]
    out += ["", "五、全部票及其前置（含已结束，按批次）", rule]
    for name, batch in batches.items():
        rows = batch.get("tickets") or {}
        out.append(f"[{name}] {str(batch.get('title') or '')[:70]}（{len(rows)} 张）")
        for tid in sorted(rows):
            prereqs = direct_prereqs(tickets[tid])
            out.append(f"  {tid} [{label(tid)}]" + (" ← " + ", ".join(prereqs) if prereqs else ""))
    return "\n".join(out) + "\n"


def graph_default_path(now: _dt.datetime) -> Path:
    """Human-readable local date and time in the name, in the desktop's download directory."""
    try:
        found = subprocess.run(["xdg-user-dir", "DOWNLOAD"], capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        found = ""
    folder = Path(found) if found and Path(found) != Path.home() else Path.home() / "Downloads"
    return folder / f"HostDAG看板全局依赖图-{now.strftime('%Y年%m月%d日-%H时%M分%S秒')}.txt"


def cmd_graph(args: argparse.Namespace) -> int:
    now = _dt.datetime.now()
    text = graph_export_text(Run(find_run(args.run)), now.astimezone().strftime("%Y年%m月%d日 %H:%M:%S %Z"))
    if args.output == "-":
        sys.stdout.write(text)
        return 0
    target = Path(args.output).expanduser() if args.output else graph_default_path(now)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(target.name + ".tmp")
    temp.write_text(text, encoding="utf-8")
    temp.replace(target)
    print(target)
    return 0


ACTIVITY_INFRA_UNITS = ("host-dag-watch", "host-dag-chatgpt-chrome", "host-dag-telegram-bridge", "codex-app-server")
ACTIVITY_DAG_UNIT = re.compile(os.environ.get("HOST_DAG_ACTIVITY_UNIT_REGEX") or r"^(host-dag|dag|r\d+|prod|tool|ops|v\d+[a-z]*|net|bk|plat|left|post|rel)-", re.IGNORECASE)
ACTIVITY_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def activity_recent_sessions(now: float, stale_seconds: float) -> list[tuple[str, str, float]]:
    """(provider, session id, last write) for local Codex and Claude transcripts written recently."""
    found: list[tuple[str, str, float]] = []
    day = 86400.0
    home = Path.home()
    roots = [("Codex", home / ".codex" / "sessions" / time.strftime("%Y/%m/%d", time.localtime(now - d * day)), "rollout-*.jsonl")
             for d in (0, 1)]
    claude_proj = os.environ.get("HOST_DAG_CLAUDE_PROJECT_DIR")
    if claude_proj:
        roots.append(("Claude", Path(claude_proj).expanduser(), "*.jsonl"))
    elif (home / ".claude" / "projects").is_dir():
        for proj_dir in (home / ".claude" / "projects").iterdir():
            if proj_dir.is_dir():
                roots.append(("Claude", proj_dir, "*.jsonl"))
    for provider, folder, pattern in roots:
        for path in folder.glob(pattern) if folder.is_dir() else ():
            with contextlib.suppress(OSError):
                written = path.stat().st_mtime
                if now - written <= stale_seconds:
                    match = ACTIVITY_UUID.search(path.name)
                    if match:
                        found.append((provider, match.group(0), written))
    return found


def activity_units() -> tuple[list[str], list[str], bool]:
    """(advisor question units, other background units, coordinator turn active) from systemd."""
    try:
        listed = subprocess.run(["systemctl", "--user", "list-units", "--type=service,scope", "--state=active",
                                 "--no-legend", "--plain", "--no-pager"],
                                capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return [], [], False
    advisors: list[str] = []
    background: list[str] = []
    turn = False
    for row in listed.splitlines():
        parts = row.split(None, 4)
        if len(parts) < 5:
            continue
        unit, description = parts[0], parts[4]
        name = unit.rsplit(".", 1)[0]
        if name.startswith("host-dag-claude-turn-"):
            turn = True
        elif name.startswith("host-dag-advisor-"):
            advisors.append(name.removeprefix("host-dag-advisor-"))
        elif (description.startswith("[systemd-run]") and ACTIVITY_DAG_UNIT.match(name)
              and not name.startswith(ACTIVITY_INFRA_UNITS)):
            background.append(name)
    return sorted(advisors), sorted(background), turn


def activity_locks(path: Path) -> list[str]:
    held: list[str] = []
    locks = path / "control" / "locks"
    for folder in sorted(locks.glob("*.holders")) if locks.is_dir() else ():
        if folder.is_dir() and any(folder.iterdir()):
            held.append(folder.name.removesuffix(".holders"))
    return held


def activity_text(r: Run, now: float, stale_seconds: float, sessions: list[tuple[str, str, float]],
                  advisors: list[str], background: list[str], coordinator_turn: bool, locks: list[str]) -> str:
    """One verdict, then the evidence behind it. An empty board column alone never means stuck."""
    tickets = r.all_tickets()
    batches = r.batches()
    batch_of = {tid: name for name, b in batches.items() for tid in (b.get("tickets") or {})}
    owner: dict[str, str] = {}
    for tid, t in tickets.items():
        for session in ACTIVITY_UUID.findall(f"{t.get('executor') or ''} {json.dumps(t.get('reviews') or '')}"):
            owner[session] = tid
    bg = background_running(r)
    board = [tid for tid in sorted(batch_of) if (v.ts.derive_state(tickets[tid], tickets) in (v.ts.RUNNING, v.ts.REVIEWING)
             and tickets[tid].get("status") not in ("ACCEPTED", "SUPERSEDED"))
             or display_state(batches[batch_of[tid]], tid, tickets, bg)[1].startswith("后台运行中")]
    parent = ACTIVITY_UUID.search(str(r.watch.get("parent_thread") or ""))
    parent_id = parent.group(0) if parent else ""
    executors = [(tid, p, sid, w) for p, sid, w in sessions for tid in [owner.get(sid)] if tid and sid != parent_id]
    others = [(p, sid, w) for p, sid, w in sessions if sid not in owner and sid != parent_id]
    coordinator_recent = any(sid == parent_id for _, sid, _ in sessions)

    def ago(when: float) -> str:
        minutes = max(0, int((now - when) // 60))
        return f"{minutes} 分钟前" if minutes < 120 else f"{minutes // 60} 小时前"

    if executors or background:
        verdict = "在干活"
    elif advisors:
        verdict = "在等裁定"
    elif coordinator_turn or coordinator_recent:
        verdict = "协调者在处理"
    else:
        verdict = "可能卡住：没有执行者、裁定、后台任务或协调者活动"
    minutes = int(stale_seconds // 60)
    out = [f"判断：{verdict}", f"（活跃 = 最近 {minutes} 分钟有写入；长时间静默的命令可能被漏判，只读，不改看板）", "",
           f"看板进行中：{len(board)} 张" + (" — " + "，".join(board) if board else "")]
    out.append(f"执行者会话：{len(executors)} 个")
    for tid, provider, sid, written in sorted(executors, key=lambda x: -x[3]):
        extra = "" if tid in board else "（票不在进行中，看板上看不到）"
        out.append(f"  {tid}{extra}  {provider} {sid[:13]}  {ago(written)}写入")
    out.append(f"Fable 裁定进行中：{len(advisors)} 个" + (" — " + "，".join(advisors) if advisors else ""))
    out.append(f"后台任务：{len(background)} 个" + (" — " + "，".join(background) if background else ""))
    out.append("协调者：" + ("回合进行中" if coordinator_turn else "最近有写入" if coordinator_recent else "空闲")
               + f"（{r.watch.get('parent_thread') or '未知'}）")
    out.append("持锁环境：" + ("，".join(locks) if locks else "无"))
    if others:
        out.append(f"其他会话（未绑定票，不计入判断）：{len(others)} 个")
    waits = [tid for tid in sorted(batch_of) if str(tickets[tid].get("wait_on") or "").partition(":")[0] not in ("", "ticket")
             and not v.ts.derive_state(tickets[tid], tickets) in v.ts.TERMINAL]
    if waits:
        out.append("非票类等待：" + "；".join(f"{tid} 等 {tickets[tid]['wait_on']}" for tid in waits[:8]))
    changes = [str(t.get("updated_at")) for t in tickets.values() if t.get("updated_at")]
    if changes:
        out.append(f"看板最后更新：{max(changes)[:16]}")
    return "\n".join(out) + "\n"


def cmd_activity(args: argparse.Namespace) -> int:
    path = find_run(args.run)
    now = time.time()
    stale = max(1.0, args.stale_minutes) * 60
    advisors, background, turn = activity_units()
    sys.stdout.write(activity_text(Run(path), now, stale, activity_recent_sessions(now, stale), advisors, background,
                                   turn, activity_locks(path)))
    return 0


def alert_when(a: dict[str, Any]) -> str:
    try:
        stamp = float(a.get("at") or 0)
    except (TypeError, ValueError):
        stamp = 0.0
    return time.strftime("%m-%d %H:%M:%S", time.localtime(stamp)) if stamp else "时间未知"


def alert_rows(r: Run) -> list[dict[str, Any]]:
    """Every alerts.jsonl record, oldest first.

    No desktop filter: `desktop` only says the entry was meant to pop, not that it did, so it
    becomes a marker. The incremental JSONL index keeps reading to appended bytes only.
    """
    try:
        return jsonl_rows(r.path / "alerts.jsonl")
    except OSError:
        return []


def alert_lines(rows: list[dict[str, Any]], sel: int = 0, expanded: bool = False, cols: int = 0,
                *, missing: bool = False) -> list[Line]:
    """The `a` view: one row per alert, ↑↓ picks, Enter unfolds the whole entry above the list."""
    if missing:
        return [[("没有 alerts.jsonl", "dim")]]
    if not rows:
        return [[("没有告警 · Esc 返回", "accent")]]
    sel = max(0, min(sel, len(rows) - 1))
    a = rows[sel]
    if expanded:
        head: Line = [("告警 · " + alert_when(a), "accent"),
                      *([(" 桌面", "hold")] if a.get("desktop") else []),
                      (f"   {sel + 1}/{len(rows)}   ↑↓ PgUp/PgDn 滚动  Esc 收起", "dim")]
        lines: list[Line] = [head, [(str(a.get("key") or "—"), "dim")], [],
                             [(str(a.get("title") or ""), "accent")], []]
        for paragraph in str(a.get("body") or "").splitlines() or [""]:
            line: Line = [("  " + paragraph if paragraph else "", "")]
            lines += v.wrap(line, cols, 2, 10_000) if cols else [line]
        return lines
    out: list[Line] = [[(f"告警 {len(rows)} 条（全部；标记 = 本应弹窗，不代表弹过）", "accent"),
                        ("   ↑↓ 选择  Enter 展开全文  Esc 返回", "dim")]]
    for i, row in enumerate(rows):
        line: Line = [("▸ " if i == sel else "  ", "accent"), (alert_when(row) + " ", "dim"),
                      *([("桌面 ", "hold")] if row.get("desktop") else []),
                      (str(row.get("title") or ""), "")]
        out.append([(text, f"{style}+sel" if style else "sel") for text, style in line] if i == sel else line)
    return out


def timing_line(timings: dict[str, float], fresh_at: float = 0.0) -> Line:
    """Stage timings of the last read (stat / 读 / 解析 / 渲染); `t` shows or hides this row."""
    def ms(name: str) -> str:
        return f"{(timings.get(name) or 0.0) * 1000:.1f}ms"
    return [("耗时 stat ", "dim"), (ms("stat") + " ", ""), ("读 ", "dim"), (ms("read") + " ", ""),
            ("解析 ", "dim"), (ms("parse") + " ", ""), ("渲染 ", "dim"), (ms("render") + " ", ""),
            ("   上次成功刷新 " + clock_text(fresh_at), "dim")]


RUN_ID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

HOT_AUTHORITY = ("run.json", "watch.json")


def hot_run_id(run_path: Path, run: dict[str, Any] | None = None) -> str:
    """The hot-run state directory name: run.json's task_thread, else a UUID run directory name."""
    thread = str((run or {}).get("task_thread") or "")
    if thread:
        return thread
    return run_path.parent.name if RUN_ID_RE.match(run_path.parent.name) else ""


def hot_manifest(run_path: Path, run_id: str, *, home: Path | None = None) -> dict[str, Any]:
    if not run_id:
        return {}
    path = (home or Path.home()) / ".local/state/host-dag-hot-runs" / run_id / "segment1/segment1-migration.json"
    try:
        state = read_json(path)
    except (OSError, ValueError):
        return {}
    return state if isinstance(state, dict) else {}


def hot_link_warnings(run_path: Path, state: dict[str, Any], *, expected_device: int | None = None,
                      names: tuple[str, ...] = HOT_AUTHORITY) -> list[str]:
    """Authority files the manifest calls `linked` that are not symlinks or not on the expected device.

    Read-only on purpose: the board reports a broken hot link and never repairs it. Entries the
    next ticket detaches (`detached` / `detached_at`) are skipped.
    """
    if expected_device is None and state.get("ssd"):
        try:
            expected_device = os.stat(Path(str(state["ssd"]))).st_dev
        except OSError:
            expected_device = None
    linked = {}
    for entry in state.get("entries") or []:
        if not isinstance(entry, dict) or entry.get("phase") != "linked":
            continue
        if "detached" in entry or "detached_at" in entry:
            continue
        linked[str(entry.get("path") or "")] = entry
    out = []
    for name in names:
        if name not in linked:
            continue
        path = run_path / name
        try:
            info = os.lstat(path)
        except OSError:
            out.append(name + " 缺失")
            continue
        if not stat.S_ISLNK(info.st_mode):
            out.append(name + " 非软链")
            continue
        if expected_device is not None:
            try:
                target = os.stat(path)
            except OSError:
                out.append(name + " 目标缺失")
                continue
            if target.st_dev != expected_device:
                out.append(name + " 不在预期设备")
    return out


def hot_link_check(run_path: Path, run_id: str | Any, *, home: Path | None = None,
                   expected_device: int | None = None) -> list[str]:
    """One read-only manifest check; a missing manifest or run id means there is nothing to report."""
    run_id = run_id() if callable(run_id) else run_id
    state = hot_manifest(run_path, str(run_id), home=home)
    return hot_link_warnings(run_path, state, expected_device=expected_device) if state else []


class HotLinkWatch:
    """Background lstat/manifest check behind the title-bar warning; never on the input thread."""

    def __init__(self, run_path: Path, run_id: Any, *, interval: float = 15.0, home: Path | None = None) -> None:
        self.run_path = run_path
        self.run_id = run_id
        self.interval = interval
        self.home = home
        self.warnings: list[str] = []
        self.finished = 0.0
        self.worker = None

    def poll(self) -> list[str]:
        if (self.worker is None or not self.worker.is_alive()) and time.time() - self.finished >= self.interval:
            def scan() -> None:
                try:
                    self.warnings = hot_link_check(self.run_path, self.run_id, home=self.home)
                except Exception:  # a read-only probe must never turn into a board error
                    self.warnings = []
                finally:
                    self.finished = time.time()
            self.worker = threading.Thread(target=scan, name="host-view-hot-links", daemon=True)
            self.worker.start()
        return self.warnings


# ---------------------------------------------------------------- read-only observer


def observer_stamp(value: Any) -> float:
    try:
        stamp = float(value) if isinstance(value, (int, float)) else v.parse_ts(str(value or ""))
        return stamp if math.isfinite(stamp) else 0.0
    except (ValueError, TypeError, OverflowError):
        return 0.0


def unconsumed_events(tickets: dict[str, Any], artifacts: list[dict[str, Any]], now: float) -> list[dict[str, Any]]:
    """Pure mtime > ticket.updated_at predicate; unknown state time is not proof of an unread event.

    Oldest events come first. Exactly ten minutes is not overdue. This is a reconciliation
    signal, not an assertion that the coordinator has never read the file.
    """
    out = []
    for artifact in artifacts:
        updated = observer_stamp((tickets.get(artifact["ticket"]) or {}).get("updated_at"))
        mtime = observer_stamp(artifact.get("mtime"))
        if updated and mtime > updated:
            wait = max(0.0, now - mtime)
            out.append({**artifact, "wait": wait, "overdue": wait > 600})
    return sorted(out, key=lambda a: (-a["wait"], a["ticket"], a["path"]))


def group_sibling_events(events: list[dict[str, Any]], threshold: int = 3) -> list[dict[str, Any]]:
    """Collapse per-module outputs (e.g. full/<test>/result.json) into one row per parent scan."""
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in events:
        groups.setdefault((event["ticket"], event["kind"], str(Path(event["path"]).parent.parent)), []).append(event)
    out = []
    for (_, _, parent), members in groups.items():
        if len(members) < threshold:
            out.extend(members)
            continue
        oldest = max(members, key=lambda e: e["wait"])
        out.append({**oldest, "path": parent, "count": len(members),
                    "overdue": any(e["overdue"] for e in members)})
    return sorted(out, key=lambda a: (-a["wait"], a["ticket"], a["path"]))


def observer_artifacts(root: Path, tickets: dict[str, Any]) -> list[dict[str, Any]]:
    """Read stats only, including CLI evidence files and nested review/attempt directories.

    Find conventional evidence/<topic>/<batch>/<ticket> roots without traversing unrelated
    build trees: scan at most ticket/round/attempt/review. Deeper explicit evidence
    remains a separate root and can also live outside the run. No artifact JSON is executed/read.
    """
    roots: dict[str, set[Path]] = {tid: set() for tid in tickets}
    direct: dict[str, set[Path]] = {tid: set() for tid in tickets}
    for tid, ticket in tickets.items():
        records = [ticket, *((ticket.get("reviews") or {}).values())]
        for rec in records:
            if not isinstance(rec, dict):
                continue
            for key in ("evidence", "verdict_path", "verdict_file", "verdict"):
                value = rec.get(key)
                if not isinstance(value, str) or not value:
                    continue
                if key == "verdict" and not value.endswith(".json"):
                    continue  # PASS/FAIL are verdict values, not paths.
                path = Path(value).expanduser()
                if not path.is_absolute():
                    path = root / path
                is_file = bool(path.suffix) or path.is_file()
                if is_file:
                    path = path.parent
                if path.name == "cli":
                    path = path.parent
                # A shared control/*.md receipt must not cause a scan of all control history.
                if is_file and root / "evidence" not in path.parents and tid not in path.parts:
                    direct[tid].add(path)
                else:
                    roots[tid].add(path)
    for pattern in ("*", "*/*", "*/*/*"):
        for path in (root / "evidence").glob(pattern):
            if path.name in roots and path.is_dir():
                roots[path.name].add(path)
    out = []
    for tid, folders in roots.items():
        seen: set[Path] = set()
        # Keep explicit roots: the bounded ticket scan may not reach deeper referenced evidence.
        for folder in folders | direct[tid]:
            for base, dirs, files in os.walk(folder):
                dirs[:] = [d for d in dirs if folder not in direct[tid]
                           and len(Path(base).relative_to(folder).parts) < 3
                           and d not in {".git", "node_modules", "source", "scratch", "artifacts", "windows", "window",
                                         "screenshots", "logs", "vendor", ".venv", "rootfs", "tools", "checks",
                                         "reads", "upload", "reference", "candidate", "build", "__pycache__"}
                           and (d not in tickets or d == tid)]
                for name in ("result.json", "verdict.json"):
                    path = Path(base) / name
                    if name not in files or path in seen:
                        continue
                    seen.add(path)
                    try:
                        out.append({"ticket": tid, "kind": name.removesuffix(".json"),
                                    "path": str(path), "mtime": path.stat().st_mtime})
                    except OSError:
                        pass
    return out


def observer_json(path: Path) -> dict[str, Any]:
    try:
        obj = read_json(path)
        return obj if isinstance(obj, dict) else {}
    except (OSError, ValueError):
        return {}


def observer_time(stamp: float) -> str:
    return time.strftime("%m-%d %H:%M:%S", time.localtime(stamp)) if stamp else "未知"


def observer_units(root: Path) -> list[dict[str, str]]:
    configured = str(observer_json(root / "watch-unit.json").get("unit") or "")
    watcher = configured if configured.endswith(".service") else configured + ".service" if configured else "host-dag-host-watch.service"
    names = [watcher, "host-dag-watch-watchdog.timer", "host-dag-dag-nudge.timer", "host-dag-telegram-bridge.service"]
    props = "Id,LoadState,ActiveState,SubState,WatchdogTimestamp,LastTriggerUSec,ActiveEnterTimestamp,ExecStart"
    try:
        raw = subprocess.run(["systemctl", "--user", "show", *names, "host-dag-*watch*.service", "-p", props],
                             capture_output=True, text=True, timeout=2).stdout
    except (OSError, subprocess.SubprocessError):
        raw = ""
    units = {}
    for block in raw.split("\n\n"):
        fields = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        if fields.get("Id"):
            units[fields["Id"]] = fields
    if not configured:
        names[0] = next((name for name, fields in units.items()
                         if "host-dag-watch.py" in fields.get("ExecStart", "")
                         and str(root / "watch.json") in fields["ExecStart"]), watcher)
    try:
        beat = float((root / "watch.json.heartbeat").read_text().strip())
        beat = beat if math.isfinite(beat) else 0.0
    except (OSError, ValueError):
        beat = 0.0
    out = []
    for index, name in enumerate(names):
        fields = units.get(name, {})
        state = fields.get("ActiveState", "unknown") if fields.get("LoadState") == "loaded" else "missing"
        heartbeat = "心跳未知"
        if index == 0 and beat:
            heartbeat = f"心跳 {observer_time(beat)}"
        elif fields.get("WatchdogTimestamp"):
            heartbeat = "心跳 " + fields["WatchdogTimestamp"]
        elif fields.get("LastTriggerUSec"):
            heartbeat = "最近触发 " + fields["LastTriggerUSec"]
        elif fields.get("ActiveEnterTimestamp"):
            heartbeat += " · 启动 " + fields["ActiveEnterTimestamp"]
        out.append({"unit": name, "state": state, "sub": fields.get("SubState", ""), "heartbeat": heartbeat})
    return out


def advisor_send_state(text: str) -> str:
    """Transport evidence only; a receipt says nothing about business execution."""
    try:
        reply = json.loads(text)
    except ValueError:
        reply = None
    if isinstance(reply, dict):
        error = reply.get("error")
        if error:
            return "pending" if isinstance(error, dict) and error.get("code") == "THREAD_BUSY" else "historical-failure"
        turn = reply.get("turn")
        if reply.get("status") == "inProgress" or isinstance(turn, dict) and turn.get("status") == "inProgress":
            return "sent"
    if text.strip() in ("busy", "THREAD_BUSY", "queued"):
        return "pending"
    if re.search(r"Traceback|\b\w+Error:|^TIMEOUT\s*$", text, re.M):
        return "historical-failure"
    return "unknown"


def advisor_delivery_links(root: Path, index_path: Path) -> tuple[dict, str]:
    """Read-only, content-bound evidence links. Never a queue or send instruction."""
    root = root.resolve()
    links: dict[str, dict] = {}
    try:
        if not index_path.resolve().is_relative_to(root):
            raise ValueError("evidence index outside run")
        if index_path.stat().st_size > 1 << 20:
            raise ValueError("evidence index too large")
        index = json.loads(index_path.read_text())
        if not isinstance(index, dict) or index.get("version") != 1 or not isinstance(index.get("links"), list):
            raise ValueError("invalid evidence index")
    except FileNotFoundError:
        return links, ""
    except (OSError, ValueError) as exc:
        return links, f"证据索引不明：{exc}"

    def reference(ref):
        if not isinstance(ref, dict) or not isinstance(ref.get("path"), str) or not ref["path"]:
            raise ValueError("invalid evidence reference")
        path = (root / ref["path"]).resolve()
        if not path.is_relative_to(root):
            raise ValueError("evidence path outside run")
        return path

    warning = ""
    for link in index["links"]:
        source = None
        try:
            if not isinstance(link, dict) or not isinstance(link.get("evidence"), dict):
                raise ValueError("invalid evidence link")
            refs = link["evidence"]
            source = reference(refs.get("source_log"))
            if str(source) in links:
                raise ValueError("ambiguous evidence links")
            if link.get("relation") not in ("redelivered", "withdrawn"):
                raise ValueError("unknown evidence relation")
            verified = {}
            for role in ("source_note", "source_log", "via_note", "via_log"):
                path = reference(refs.get(role))
                if path.stat().st_size > 1 << 20:
                    raise ValueError("evidence file too large")
                raw = path.read_bytes()
                if hashlib.sha256(raw).hexdigest() != refs[role].get("sha256"):
                    raise ValueError("evidence content changed")
                verified[role] = (path, raw)
            if any(verified[note][0].name != "note.md" or verified[log][0].name != "send.log" or
                   verified[note][0].parent != verified[log][0].parent
                   for note, log in (("source_note", "source_log"), ("via_note", "via_log"))):
                raise ValueError("note/send evidence mismatch")
            if source == verified["via_log"][0] or advisor_send_state(verified["via_log"][1].decode("utf-8", "replace")) != "sent":
                raise ValueError("linked delivery has no success receipt")
            links[str(source)] = {"status": link["relation"], "reason": "内容绑定的补发/撤回证据",
                                  "evidence": [str(index_path), *(str(p) for p, _ in verified.values())]}
        except (OSError, ValueError) as exc:
            row = {"status": "unknown", "reason": f"关联证据不明：{exc}", "evidence": [str(index_path)]}
            if source is not None:
                links[str(source)] = row
            else:
                warning = row["reason"]
    return links, warning


def advisor_queue(root: Path, now: float, evidence_index: Path | None = None) -> list[dict[str, Any]]:
    root = root.resolve()
    links, warning = advisor_delivery_links(root, evidence_index or root / "control/advisor-delivery-evidence.json")
    out = []
    # Missing indexed logs remain visible; absence is not proof of delivery.
    paths = set((root / "control").glob("advisor-*/send.log")) | {Path(source) for source in links}
    for path in paths:
        try:
            if not path.resolve().is_relative_to(root):
                raise OSError("send log outside run")
            stat = path.stat()
            status = advisor_send_state(path.read_text(encoding="utf-8", errors="replace")) if stat.st_size <= 1 << 20 else "unknown"
            age = max(0.0, now - stat.st_mtime)
        except OSError:
            status, age = "unknown", 0.0
        if status == "sent":
            continue
        row = {"name": path.parent.name, "wait": age, "status": status,
               "reason": warning, "evidence": [str(path)]}
        row.update(links.get(str(path.resolve()), {}))
        out.append(row)
    if warning:
        out.append({"name": "顾问送达证据索引", "wait": 0.0, "status": "unknown",
                    "reason": warning, "evidence": [str(evidence_index or root / "control/advisor-delivery-evidence.json")]})
    rank = {"pending": 0, "unknown": 1, "historical-failure": 2, "redelivered": 3, "withdrawn": 4}
    return sorted(out, key=lambda a: (rank[a["status"]], -a["wait"], a["name"]))


def observer_tail(path: Path) -> list[dict[str, Any]]:
    """Bound log I/O on each refresh; discard an incomplete first/last JSONL record."""
    try:
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            offset = max(0, fh.tell() - (256 << 10))
            fh.seek(offset)
            if offset:
                fh.readline()
            raw = fh.read().decode("utf-8", "replace")
    except OSError:
        return []
    out = []
    for line in raw.splitlines():
        try:
            row = json.loads(line)
            if isinstance(row, dict):
                out.append(row)
        except ValueError:
            continue
    return out


def observer_recent(root: Path, watch: dict[str, Any], tl: v.Timeline | None) -> list[dict[str, Any]]:
    events = []
    prefixes = ("toolstall", "interrupt", "retr", "parent", "uncertain", "writer-conflict", "redelivered",
                "drift", "resolved-wait", "review-verdict", "idle", "watcher", "codexdaemon")
    for row in observer_tail(root / "alerts.jsonl"):
        if str(row.get("key", "")).startswith(prefixes):
            events.append({"at": observer_stamp(row.get("at")), "text": "watch/self-heal " + str(row.get("title", ""))})
    for row in observer_tail(root / "logs/dag-nudge.jsonl"):
        if row.get("verdict") == "nudge" or row.get("action") == "retry-pending":
            events.append({"at": observer_stamp(row.get("at")), "text": "nudge " + str(row.get("result") or "pending")
                           + " " + ",".join(str(k) for k in row.get("kept") or [])})
    # Delivered wake prompts are retained in the same coordinator timeline as the c view.
    if tl:
        for event in tl.events:
            if event.kind == "start" and event.text.startswith("HOST-DAG-WATCH-MARKER:"):
                events.append({"at": event.ts, "text": "watcher 唤醒 " + event.text})
    delivered = observer_stamp(watch.get("last_delivered_at"))
    if delivered:
        events.append({"at": delivered, "text": "watcher 最近送达"})
    return sorted((e for e in events if e["at"]), key=lambda e: -e["at"])[:10]


def observer_coordinator(tl: v.Timeline | None, thread: str) -> str:
    if tl is None or tl.running is None:
        return "忙闲未知 · 开始 未知 · 结束 未知 · 后台等待：未知"
    start = max([tl.turn_started, *[e.ts for e in tl.events if e.kind == "start"]])
    end = max([0.0, *[e.ts for e in tl.events if e.kind in {"end", "abort"}]])
    if thread.startswith("claude-"):
        turns = v.claude_host_record(thread).get("turns") or []
        if turns and isinstance(turns[-1], dict):
            start = observer_stamp(turns[-1].get("started")) or start
            end = observer_stamp(turns[-1].get("ended")) or (0.0 if tl.running else end)
    waiting = tl.running and (bool(tl.cells) or any(
        e.ts >= start and e.result == "running" and re.search(r"wait|sleep|TaskOutput|poll", e.text, re.I)
        for e in tl.events))
    ended = "本轮未结束" if tl.running else observer_time(end) if end >= start else "未知"
    return (f"{'忙' if tl.running else '闲'} · 开始 {observer_time(start)} · 结束 {ended}"
            f" · 后台等待：{'有' if waiting else '未见'}（按 coord 日志）")


def observer_lines(r: Run, now: float | None = None) -> list[Line]:
    now = time.time() if now is None else now
    tickets = r.all_tickets()
    # Walking hundreds of closed tickets' evidence on a saturated USB disk took minutes; only open ones can be unread.
    live = {tid: t for tid, t in tickets.items() if v.ts.derive_state(t, tickets) not in v.ts.TERMINAL}
    pending = group_sibling_events(unconsumed_events(live, observer_artifacts(r.path, live), now))
    tl = r.timeline(r.coordinator())
    lines: list[Line] = [[("观察器 · 只读 · result/verdict mtime > updated_at；>10 分钟红色", "accent")],
                        [("守护单元", "accent")]]
    for unit in observer_units(r.path):
        lines.append([(f"  {unit['unit']}  {unit['state']}/{unit['sub']}  {unit['heartbeat']}",
                       "ok" if unit["state"] == "active" else "dim")])
    lines.append([(f"未消费事件（{len(pending)}）· 最久在前", "accent")])
    pending_lines = []
    for event in pending:
        path = event["path"].replace(str(r.path), "$RUN")
        many = f" · {event['count']} 个子结果（执行者内部分模块输出）" if event.get("count") else ""
        pending_lines.append([(f"  {event['ticket']} {event['kind']}{many} 等待 {v.dur_long(event['wait'])}  {path}",
                               "fail" if event["overdue"] else "running")])
    lines.extend(pending_lines[:3])
    if len(pending) > 3:
        lines.append([(f"  另 {len(pending) - 3} 条，向下滚动查看完整列表", "dim")])
    if not pending:
        lines.append([("  无（缺失文件 / 未知 updated_at 不作为未消费证据）", "dim")])
    lines.append([("协调者 " + observer_coordinator(tl, r.coordinator()), "accent")])
    queue = advisor_queue(r.path, now)
    labels = {"pending": "待送", "unknown": "不明", "historical-failure": "历史失败", "redelivered": "已补发", "withdrawn": "已撤回"}
    counts = " · ".join(f"{label} {sum(q['status'] == status for q in queue)}" for status, label in labels.items())
    lines.append([("顾问通知队列 / 历史 · " + counts, "accent")])
    queue_lines = []
    # Closed history (redelivered/withdrawn/historical-failure) stays in the counts only; listing it reads as a week-old backlog.
    for q in (q for q in queue if q["status"] in ("pending", "unknown")):
        evidence = ", ".join(p.replace(str(r.path), "$RUN") for p in q["evidence"])
        queue_lines.append([(f"  {q['name']} {labels[q['status']]} · 记录距今 {v.dur_long(q['wait'])}"
                             f" · {q['reason']} · 证据：{evidence}",
                             "running" if q["status"] in ("pending", "unknown") else "dim")])
    lines.extend(queue_lines[:2])
    if len(queue_lines) > 2:
        lines.append([(f"  另 {len(queue_lines) - 2} 条，向下滚动查看完整列表", "dim")])
    if not queue_lines:
        lines.append([("  无待处理（已结束的只计数）" if queue else "  无", "dim")])
    lines.append([("最近 10 条 watcher 唤醒 / self-heal / nudge（最新在前）", "accent")])
    events = observer_recent(r.path, r.watch, tl)
    lines.extend([[(f"  {observer_time(e['at'])} {e['text']}", "")] for e in events])
    if not events:
        lines.append([("  暂无记录", "dim")])
    if len(pending) > 3:
        lines.extend([[('未消费事件 · 完整列表', 'accent')], *pending_lines])
    if len(queue_lines) > 2:
        lines.extend([[('顾问通知队列 · 完整列表', 'accent')], *queue_lines])
    return lines


# ---------------------------------------------------------------- ANSI output for peek


ANSI = {"ok": "32", "fail": "31", "running": "33", "run": "33", "dim": "2", "accent": "36", "cand": "34",
        "ask": "31", "hold": "35", "res": "35", "wait": "2", "sup": "2", "left": "36;2", "move": "36;2", "sel": "7"}


def ansi(line: Line, color: bool) -> str:
    if not color:
        return "".join(s for s, _ in line)
    return "".join(f"\033[{ANSI[st.split('+')[0]]}m{s}\033[0m" if st and st.split("+")[0] in ANSI else s
                   for s, st in line)


def clip(line: Line, cols: int) -> Line:
    """Cut a line to the terminal width so long commands do not wrap; 0 keeps it whole."""
    if not cols:
        return line
    out: Line = []
    left = cols - 1
    for text, st in line:
        if left <= 0:
            break
        part = v.fit(text, left)
        if part != text:
            part = v.fit(part, left - 1) + "…"
        out.append((part, st))
        left -= v.width(part)
    return out


_EVENT_ROW = re.compile(r"^\d\d:\d\d:\d\d  ")


def fit_lines(lines: list[Line], cols: int, max_lines: int) -> list[Line]:
    """Fit lines to the screen: wrap up to max_lines rows (timeline rows continue under the
    text column, output rows under │), or cut to one row with … when max_lines is 1."""
    if not cols:
        return lines
    out: list[Line] = []
    for line in lines:
        if max_lines <= 1:
            out.append(clip(line, cols))
            continue
        first = line[0][0] if line else ""
        indent = v.EVENT_INDENT if _EVENT_ROW.match(first) else \
            len(v.DETAIL_PREFIX) if first.startswith(v.DETAIL_PREFIX) else 2
        out.extend(v.wrap(line, cols, indent, max_lines))
    return out


def cmd_peek(args: argparse.Namespace) -> int:
    r = Run(find_run(args.run))
    label, thread, t = r.resolve(args.target, args.role)
    if not thread:
        raise v.ViewError(f"{label}: no thread recorded yet")
    tl = r.timeline(thread)
    color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
    lines = peek_lines(label, thread, tl, t, tools_only=args.tools, aliases=r.aliases())
    body = lines[4:-2] if tl else []
    if args.lines and len(body) > args.lines:
        body = body[-args.lines:]
        while body and body[0][0][0].startswith("          │"):
            body = body[1:]
        lines = lines[:4] + body + lines[-2:]
    cols = shutil.get_terminal_size().columns if sys.stdout.isatty() else 0
    print("\n".join(ansi(x, color) for x in fit_lines(lines, cols, args.wrap)))
    if not args.follow or tl is None:
        return 0
    try:
        follow(tl, args.tools, color, aliases=r.aliases(), max_lines=args.wrap)
    except KeyboardInterrupt:
        pass
    return 0


def follow(tl: v.Timeline, tools_only: bool, color: bool, ticks: int = -1, interval: float = 1.0,
           aliases: list[tuple[str, str]] | None = None, max_lines: int = 3) -> None:
    """Print new timeline lines as they land, and a ↳ line when a printed running command finishes."""
    cols = shutil.get_terminal_size().columns if sys.stdout.isatty() else 0
    al = aliases or []

    def emit(line: Line) -> None:
        for row in fit_lines([line], cols, max_lines):
            print(ansi(row, color), flush=True)

    shown = len(tl.events)
    pending = {i for i, ev in enumerate(tl.events) if ev.result == "running"}
    while ticks:
        ticks -= 1
        time.sleep(interval)
        tl.refresh()
        for i in sorted(pending):
            ev = tl.events[i]
            if ev.result in {"ok", "fail"}:
                pending.discard(i)
                mark = f"✓ {v.dur(ev.done_ts - ev.ts)}" if ev.result == "ok" else \
                    (f"✗ exit {ev.code}" if ev.code is not None else "✗ 失败")
                emit([(f"{v.clock(ev.done_ts)}  ↳ {v.short_cmd(v.shorten(ev.text, al), 48)}  {mark}", ev.result)])
                for d in ev.detail:
                    emit([(f"{v.DETAIL_PREFIX}{v.shorten(d, al)}", "dim")])
        if len(tl.events) > shown:
            sub = v.Timeline(tl.path)
            sub.events, sub.running = tl.events[shown:], tl.running
            for seg in v.event_lines(sub, tools_only, aliases=al):
                emit([seg])
            pending |= {shown + i for i, ev in enumerate(sub.events) if ev.result == "running"}
            shown = len(tl.events)


# ---------------------------------------------------------------- curses board


def run_choices(current: Path | None) -> list[Path]:
    """Registered runs plus the current one, existing and de-duplicated, current first."""
    out: list[Path] = []
    for p in ([current] if current else []) + v.known_runs():
        p = public_path(p)
        if p not in out and (p / "run.json").is_file():
            out.append(p)
    return out


def run_summary(path: Path) -> tuple[str, int, int, str]:
    """(batches text, open batches, total batches, last update HH:MM or MM-DD)."""
    try:
        run = v.load_json(path / "run.json")
    except v.ViewError:
        return "读取失败", 0, 0, "—"
    parts, open_n = [], 0
    for name in v.ordered_batch_names(run):
        p = v.progress(v.batches(run)[name], v.all_tickets(run))
        done = v.batch_done(v.batches(run)[name])
        open_n += not done
        parts.append(f"{'✔' if done else '▶'}{name} {p.accepted}/{p.total}")
    mtime = (path / "run.json").stat().st_mtime
    stamp = time.strftime("%H:%M" if time.time() - mtime < 86400 else "%m-%d", time.localtime(mtime))
    return ("  ".join(parts) or "无批次"), open_n, len(parts), stamp


def run_label(path: Path) -> str:
    """`01a0b493…/local-dev-dag` for runs under a `runs/` root, else the ~-shortened path."""
    parts = path.parts
    if "runs" in parts and parts.index("runs") + 1 < len(parts):
        rest = list(parts[parts.index("runs") + 1:])
        rest[0] = rest[0][:8] + ("…" if len(rest[0]) > 8 else "")
        return "/".join(rest)
    return v.shorten(str(path), [(str(Path.home()), "~")])


def runs_lines(runs: list[Path], current: Path, sel: int) -> list[Line]:
    lines: list[Line] = [[("Run 列表", "accent"), ("  ↑↓ 选择  Enter 切换  Esc 返回", "dim")], []]
    for i, path in enumerate(runs):
        text, open_n, total, stamp = run_summary(path)
        mark = "●" if path == current else " "
        row: Line = [(f"{mark} ", "accent"), (cell(run_label(path), 44), ""),
                     (cell(stamp, 7), "dim"), (text, "run" if open_n else ("ok" if total else "dim"))]
        lines.append([(t, f"{st}+sel" if st else "sel") for t, st in row] if i == sel else row)
    if not runs:
        lines.append([("没有登记的 run；用 host-dag runs --add <目录> 或 --scan <根目录> 登记", "dim")])
    return lines


def cmd_runs(args: argparse.Namespace) -> int:
    if args.scan:
        for path in v.scan_runs(Path(args.scan).expanduser()):
            if v.register_run(path):
                print(f"登记 {path}")
    for path in args.add or []:
        path = Path(path).expanduser()
        if not (path / "run.json").is_file():
            raise v.ViewError(f"no run.json in {path}")
        v.register_run(path)
    try:
        current = public_path(find_run(args.run))
    except v.ViewError:
        current = None
    color = sys.stdout.isatty() and not os.environ.get("NO_COLOR")
    runs = run_choices(current)
    for line in runs_lines(runs, current or Path("/"), -1)[2:]:
        print(ansi(line, color))
    return 0


def cmd_board(args: argparse.Namespace) -> int:
    os.environ.setdefault("ESCDELAY", "25")  # Esc returns at once instead of after curses' 1 s default
    import curses
    import locale
    locale.setlocale(locale.LC_ALL, "")
    def start(scr):
        # Discovery too can touch USB (registry/run.json). Paint before probing.
        direct = args.run or os.environ.get('HOST_DAG_RUN')
        if direct and (str(direct).startswith('/') or str(direct).startswith('~')):
            path = Path(direct).expanduser()
        else:
            discovery = Snapshot(cache_path(Path.home(), 'discovery-' + str(args.batch or direct or 'default').replace('/', '_')))
            while True:
                value = discovery.get(lambda: str(find_run(args.batch if args.batch and not direct else args.run)), budget=.15)
                if value:
                    path = Path(value)
                    break
                scr.erase()
                scr.addstr(0, 0, 'Host DAG · ' + discovery.status + ' · q 退出')
                scr.refresh()
                scr.timeout(200)
                if scr.getch() == ord('q'):
                    return 0
        return Board(scr, Run(path, load=False), args.batch).loop()
    return curses.wrapper(start)


class Board:
    def __init__(self, scr: Any, r: Run, batch: str | None) -> None:
        import curses
        self.c = curses
        self.scr = scr
        self.r = r
        names = v.ordered_batch_names(r.run)
        self.bname = batch if batch in names else (names[0] if names else "")
        self.runs: list[Path] = []
        self.rsel = 0
        self.view, self.sel, self.tab, self.pfilter, self.flt = "board", 0, "exec", v.FILTER_ALL, ""
        self.show_all = False  # b: list every batch instead of the unfinished plus the 3 latest finished
        self.hide_ok = FOLD_LIVE  # h: live only → plus queued → all (user 11-03)
        self.scroll = 0
        self.board_top = 0  # first ticket row shown when the batch has more tickets than the screen
        self.back = "board"
        self.wrap = True
        self.order: list[str] = []
        self.alerts: list[dict[str, Any]] = []
        self.alert_sel, self.alert_expanded, self.alert_top = -1, False, 0
        self.alert_follow_end = True  # `a` opens on the newest entry, like the old tail view
        self.show_timings = False
        self.timings: dict[str, float] = {}
        self.fresh_at = 0.0
        self.hot_warnings: list[str] = []
        self.hot = HotLinkWatch(r.path, lambda: hot_run_id(r.path, r.run))
        # Scrolling only re-slices the cached rows: a long rollout takes ~0.1 s to wrap, and
        # re-rendering it on every arrow key made held keys queue up. `gen` moves on the 2 s
        # tick, a resize or any non-scroll key, and that is when run.json and the rollout are re-read.
        self.gen, self.loaded = 0, -1
        self.cache: tuple[tuple[Any, ...], list[Line]] | None = None
        self.read_cache = Snapshot(cache_path(r.path, 'board'))
        self.read_key = None
        curses.curs_set(0)
        curses.use_default_colors()
        palette = {"ok": curses.COLOR_GREEN, "fail": curses.COLOR_RED, "running": curses.COLOR_YELLOW,
                   "run": curses.COLOR_YELLOW, "cand": curses.COLOR_BLUE, "ask": curses.COLOR_RED,
                   "hold": curses.COLOR_MAGENTA, "res": curses.COLOR_MAGENTA, "accent": curses.COLOR_CYAN,
                   "left": curses.COLOR_CYAN, "move": curses.COLOR_CYAN}
        self.attr: dict[str, int] = {"": 0, "dim": curses.A_DIM, "wait": curses.A_DIM, "sup": curses.A_DIM,
                                     "ready": 0, "sel": curses.A_REVERSE}
        for i, (k, col) in enumerate(palette.items(), start=1):
            curses.init_pair(i, col, -1)
            self.attr[k] = curses.color_pair(i) | (curses.A_BOLD if k == "accent" else 0)
        scr.timeout(2000)

    def style(self, st: str) -> int:
        base, _, extra = st.partition("+")
        return self.attr.get(base, 0) | (self.c.A_REVERSE if extra == "sel" else 0)

    def name(self) -> str:
        names = v.ordered_batch_names(self.r.run)
        if self.bname not in names:
            self.bname = names[0] if names else ''
        return self.bname

    def cur_tid(self) -> str:
        # 11-18: the background render can swap in a shorter order between draw and key (Enter crashed).
        if not self.order:
            return ""
        return self.order[min(max(self.sel, 0), len(self.order) - 1)]

    def step_batch(self, delta: int) -> None:
        names = v.visible_batch_names(self.r.run, self.show_all, keep=self.name())
        self.bname = names[(names.index(self.name()) + delta) % len(names)]
        self.sel = 0

    def switch_run(self, path: Path) -> None:
        r = Run(path, load=False)
        self.r, self.view, self.sel = r, "board", 0
        self.bname = ''
        self.alerts, self.alert_sel, self.alert_expanded = [], -1, False
        self.alert_follow_end = True
        self.hot_warnings = []
        self.hot = HotLinkWatch(r.path, lambda: hot_run_id(r.path, r.run))
        self.read_cache = Snapshot(cache_path(path, 'board'))
        self.read_key = None

    def width(self) -> int:
        scr = getattr(self, "scr", None)
        return scr.getmaxyx()[1] if scr is not None else 0

    def lines(self) -> list[Line]:
        if self.view == "runs":
            return runs_lines(self.runs, public_path(self.r.path), self.rsel)
        n = self.name()
        if self.view == "board":
            meta: dict[str, int] = {}
            lines, self.order = board_lines(self.r, n, self.sel, self.flt, self.show_all, self.hide_ok,
                                            self.hot_warnings, cols=self.width(), meta=meta)
            if self.order and self.sel >= len(self.order):
                self.sel = len(self.order) - 1
                lines, self.order = board_lines(self.r, n, self.sel, self.flt, self.show_all, self.hide_ok,
                                                self.hot_warnings, cols=self.width(), meta=meta)
            self.board_row0 = meta.get("row0")
            return lines
        if self.view == "tree":
            return tree_view(self.r, n)
        if self.view == "graph":
            return graph_view(self.r)
        if self.view == "alerts":
            return self.alert_view()
        if self.view == "observer":
            return self.r.observer_view()
        if self.view == "coord":
            coord = self.r.coordinator()
            return peek_lines("协调者", coord or "—", self.r.timeline(coord), {}, aliases=self.r.aliases(),
                              mode=self.pfilter)
        tid = self.cur_tid()
        if not tid:
            self.view = "board"
            return self.lines()
        if self.view == "detail":
            return detail_lines(self.r, n, tid, self.tab)
        t = (self.r.batches()[n].get("tickets") or {})[tid]
        thread = t.get("executor") if self.tab == "exec" else (displayed_reviews(self.r, t).get(self.tab) or {}).get("thread")
        label = tid if self.tab == "exec" else f"{tid} {self.tab} 审查"
        return peek_lines(label, thread or "—", self.r.timeline(thread), t, aliases=self.r.aliases(),
                          mode=self.pfilter)

    def alert_view(self) -> list[Line]:
        """Read every alert once per refresh; `alert_sel < 0` means "follow the newest"."""
        missing = not (self.r.path / "alerts.jsonl").is_file()
        self.alerts = alert_rows(self.r)
        if self.alert_follow_end or self.alert_sel < 0:
            self.alert_sel = max(0, len(self.alerts) - 1)
        self.alert_sel = max(0, min(self.alert_sel, len(self.alerts) - 1))
        return alert_lines(self.alerts, self.alert_sel, self.alert_expanded,
                           self.scr.getmaxyx()[1], missing=missing)

    def rendered(self) -> list[Line]:
        """Lines as drawn: timeline views wrap up to 3 rows (w toggles one row with …)."""
        key = (self.gen, id(self.r), self.view, self.bname, self.sel, self.tab, self.pfilter, self.wrap,
               self.flt, self.rsel, self.show_all, self.hide_ok, self.alert_sel, self.alert_expanded,
               tuple(self.hot_warnings), self.read_cache.status, self.scr.getmaxyx()[1])
        if self.cache and self.cache[0] == key:
            return self.cache[1]
        lines = self.read_lines()
        if self.read_cache.status:
            lines = [[(self.read_cache.status, 'hold')], *lines]
        if self.view in {'peek', 'coord', 'detail'}:
            lines = fit_lines(lines, self.scr.getmaxyx()[1], 3 if self.wrap else 1)
        self.cache = (key, lines)
        return lines

    def read_lines(self) -> list[Line]:
        # The entire read/render runs off the curses input thread, including
        # stat, lock observations, rollout discovery and optional detail files.
        # One worker prevents a stalled disk from accumulating reader threads.
        request = (self.view, self.bname, self.sel, self.tab, self.pfilter, self.wrap,
                   self.flt, self.rsel, self.show_all, self.hide_ok, self.alert_sel, self.alert_expanded,
                   tuple(self.hot_warnings), self.scr.getmaxyx()[1])
        shadow = copy.copy(self)
        cache, path = self.read_cache, self.r.path
        def load():
            if not hasattr(cache, 'reader'):
                cache.reader = Run(path, load=False)
            reader = cache.reader
            reader.json.reset_timings()
            reader.reload()
            shadow.r = reader
            if not shadow.r.batches():
                raise v.ViewError('run.json has no batches')
            if shadow.view == 'runs':
                shadow.runs = run_choices(shadow.r.path)
            started = time.perf_counter()
            lines = shadow.lines()
            rendered_request = list(request)
            rendered_request[1] = shadow.bname
            return {'request': rendered_request, 'lines': lines, 'run': shadow.r.run,
                    'watch': shadow.r.watch, 'leftovers': shadow.r.leftovers,
                    'order': shadow.order, 'batch': shadow.bname, 'runs': [str(p) for p in shadow.runs],
                    'alerts': shadow.alerts if shadow.view == 'alerts' else [],
                    'row0': getattr(shadow, 'board_row0', None) if shadow.view == 'board' else None,
                    'alert_sel': shadow.alert_sel, 'alert_follow_end': shadow.alert_follow_end,
                    'timings': {**reader.json.timings, 'render': time.perf_counter() - started}}
        saved = self.read_cache.get(load, budget=.15, store=self.view != 'alerts')
        # A saved board may be shown immediately on first launch; never show a
        # different detail selection while its read is pending.
        usable = saved and (saved.get('request') == list(request) or
                            (not self.r.batches() and self.view == 'board' and
                             saved.get('request', [''])[0] == 'board' and
                             (not self.bname or saved.get('batch') == self.bname)))
        if usable:
            self.r.run, self.r.watch = saved['run'], saved['watch']
            self.r.leftovers = saved['leftovers']
            self.order, self.bname = saved['order'], saved['batch']
            self.runs = [Path(p) for p in saved.get('runs', [])]
            self.alerts = saved.get('alerts') or self.alerts
            self.alert_sel = saved.get('alert_sel', self.alert_sel)
            self.alert_follow_end = saved.get('alert_follow_end', self.alert_follow_end)
            self.timings = saved.get('timings') or self.timings
            lines = saved['lines']
        elif (saved and self.view == 'board' and isinstance(saved.get('row0'), int)
              and moved_only(saved.get('request'), request)):
            # Only the cursor moved (11-11 user: ↑↓ on a large batch flashed 正在读取 for seconds while
            # the worker re-read and re-rendered every row). Re-highlight the last board now; the
            # worker's fresh render replaces it as soon as it lands.
            self.r.run, self.r.watch = saved['run'], saved['watch']
            self.r.leftovers = saved['leftovers']
            self.order, self.bname = saved['order'], saved['batch']
            self.timings = saved.get('timings') or self.timings
            lines = reselect(saved['lines'], saved['row0'], saved['request'][2], self.sel, len(self.order))
        else:
            lines = [[('Host DAG · 正在读取', 'accent')]]
        self.fresh_at = self.read_cache.fresh_at
        return lines

    def keys_hint(self) -> str:
        return {
            "board": "↑↓ PgUp/PgDn 选票  Enter 详情  p peek  c 协调者  o 观察器  d 依赖树  D 全局依赖  a 告警  r 切换 run  / 过滤  ←/→ 批次  b 全部批次  h 执行中→加排队→全部  t 计时  q 退出   [只读]",
            "observer": "↑↓ PgUp/PgDn 滚动  Esc/o 返回   [只读]",
            "runs": "↑↓ 选择  Enter 切换  Esc 返回   [只读]",
            "detail": "↑↓ PgUp/PgDn 滚动  Tab 切换 执行/spec/std 会话  f 全屏跟踪  e 打开证据目录  w 折行/单行  Esc 返回   [只读]",
            "peek": f"↑↓ PgUp/PgDn 滚动  End 跟到最新  Tab 全部/消息/工具（当前 {v.PEEK_FILTER_LABEL[self.pfilter]}）  w 折行/单行  Esc 返回   [只读]",
            "coord": f"↑↓ PgUp/PgDn 滚动  End 跟到最新  Tab 全部/消息/工具（当前 {v.PEEK_FILTER_LABEL[self.pfilter]}）  w 折行/单行  Esc 返回   [只读]",
            "tree": "↑↓ PgUp/PgDn 滚动  Esc 返回   [只读]",
            "alerts": "↑↓ 选择  Enter 展开全文  Esc 收起/返回   [只读]",
            "graph": "↑↓ PgUp/PgDn 滚动  Esc 返回   [只读]",
        }[self.view]

    def draw(self) -> None:
        scr = self.scr
        scr.erase()
        h, w = scr.getmaxyx()
        lines = self.rendered()
        if self.view == "board" and self.show_timings and lines:
            lines = [lines[0], timing_line(self.timings, self.fresh_at), *lines[1:]]
        body_h = h - 1
        if self.view == "alerts" and not self.alert_expanded:
            # The alert list can hold thousands of rows: scroll it so the picked entry stays visible.
            rows, room = max(0, len(lines) - 1), max(1, body_h - 1)
            self.alert_top = min(self.alert_top, self.alert_sel, max(0, rows - room))
            if self.alert_sel >= self.alert_top + room:
                self.alert_top = self.alert_sel - room + 1
            shown = lines[:1] + lines[1 + self.alert_top: 1 + self.alert_top + room]
        else:
            if self.view in self.SCROLL_VIEWS:
                top = max(0, len(lines) - body_h) if self.scroll < 0 else min(self.scroll, max(0, len(lines) - body_h))
            else:
                top = 0
            shown = lines[top: top + body_h]
        more = ""
        n = len(self.order)
        head = len(lines) - n
        if self.view == "board" and n and head < body_h and len(lines) > body_h:
            # Keep the batch header and column titles; scroll the ticket rows so the selection stays visible.
            room = body_h - head
            self.board_top = min(self.board_top, self.sel, max(0, n - room))
            if self.sel >= self.board_top + room:
                self.board_top = self.sel - room + 1
            shown = lines[:head] + lines[head + self.board_top: head + self.board_top + room]
            more = f"{self.sel + 1}/{n}  "
        for y, line in enumerate(shown):
            x = 0
            for text, st in line:
                if x >= w - 1:
                    break
                text = v.fit(text, w - 1 - x)
                try:
                    scr.addstr(y, x, text, self.style(st))
                except self.c.error:
                    pass
                x += v.width(text)
        hint = self.keys_hint() if not self.flt or self.view != "board" else f"过滤: {self.flt}   Esc 清除"
        if self.view == "board":
            hint = more + hint
        try:
            scr.addstr(h - 1, 0, v.fit(hint, w - 1), self.attr["dim"])
        except self.c.error:
            pass
        scr.refresh()

    def prompt(self, label: str) -> str:
        h, w = self.scr.getmaxyx()
        self.c.echo()
        self.c.curs_set(1)
        self.scr.timeout(-1)
        self.scr.addstr(h - 1, 0, " " * (w - 1))
        self.scr.addstr(h - 1, 0, label)
        try:
            text = self.scr.getstr(h - 1, len(label), 40).decode("utf-8", "replace")
        finally:
            self.c.noecho()
            self.c.curs_set(0)
            self.scr.timeout(2000)
        return text.strip()

    def open_evidence(self) -> None:
        t = (self.r.batches()[self.name()].get("tickets") or {}).get(self.cur_tid(), {})
        path = t.get("evidence")
        if path and os.path.exists(path):
            subprocess.Popen(["xdg-open", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True)

    SCROLL_VIEWS = {"peek", "coord", "alerts", "graph", "tree", "observer", "detail"}

    def loop(self) -> int:
        while True:
            if self.loaded != self.gen:
                self.loaded = self.gen
            # Read-only background probe: a broken hot link only turns the title bar red.
            self.hot_warnings = self.hot.poll()
            self.draw()
            k = self.scr.getch()
            if k == -1:
                self.gen += 1
                continue
            keys = [k]  # take every key already queued so a held arrow costs one redraw
            self.scr.nodelay(True)
            try:
                while (more := self.scr.getch()) != -1:
                    keys.append(more)
            finally:
                self.scr.timeout(2000)
            for k in keys:
                if self.handle(k):
                    return 0

    def default_tab(self, tid: str) -> str:
        """A ticket under review opens on its registered review session; otherwise on the executor."""
        everything = self.r.all_tickets()
        t = everything.get(tid) or {}
        if v.ts.derive_state(t, everything) == v.ts.REVIEWING:
            reviews = displayed_reviews(self.r, t)
            for axis in session_tabs(self.r, t)[1:]:
                if (reviews.get(axis) or {}).get("thread"):
                    return axis
        return "exec"

    def handle(self, k: int) -> bool:
        """Apply one key; True means quit."""
        c = self.c
        scroll_keys = {c.KEY_UP, c.KEY_DOWN, c.KEY_PPAGE, c.KEY_NPAGE, c.KEY_END}
        if not (self.view in self.SCROLL_VIEWS and k in scroll_keys):
            self.gen += 1
        n = len(self.order)
        if self.view == "board":
            if k in (ord("q"),):
                return True
            if k in (c.KEY_DOWN, ord("j")) and n:
                self.sel = (self.sel + 1) % n
            elif k in (c.KEY_UP, ord("k")) and n:
                self.sel = (self.sel - 1) % n
            elif k in (c.KEY_NPAGE, c.KEY_PPAGE) and n:
                page = max(1, self.scr.getmaxyx()[0] - 12)
                self.sel = min(n - 1, self.sel + page) if k == c.KEY_NPAGE else max(0, self.sel - page)
            elif k in (c.KEY_HOME, c.KEY_END) and n:
                self.sel = 0 if k == c.KEY_HOME else n - 1
            elif k in (c.KEY_RIGHT, c.KEY_LEFT) and self.r.batches():
                self.step_batch(1 if k == c.KEY_RIGHT else -1)
            elif k in (10, 13, c.KEY_ENTER) and n:
                self.view, self.tab = "detail", self.default_tab(self.cur_tid())
            elif k == ord("p") and n:
                self.view, self.tab, self.scroll, self.pfilter = "peek", "exec", -1, v.FILTER_ALL
            elif k == ord("c"):
                self.view, self.scroll, self.pfilter = "coord", -1, v.FILTER_ALL
            elif k == ord("o"):
                self.view, self.scroll = "observer", 0
            elif k == ord("d"):
                self.view, self.scroll = "tree", 0  # 11-11 user: one batch's tree is taller than the screen
            elif k == ord("D"):
                self.view, self.scroll = "graph", 0
            elif k == ord("r"):
                self.runs = []
                self.view, self.rsel = "runs", 0
            elif k == ord("a"):
                self.view, self.scroll = "alerts", -1
                self.alert_expanded, self.alert_sel, self.alert_top = False, -1, 0
                self.alert_follow_end = True
            elif k == ord("b"):
                self.show_all = not self.show_all
            elif k == ord("t"):
                self.show_timings = not self.show_timings
            elif k == ord("h"):
                self.hide_ok = {FOLD_LIVE: FOLD_CLOSED, FOLD_CLOSED: FOLD_ALL}.get(self.hide_ok, FOLD_LIVE)
                self.sel = 0
            elif k == ord("/"):
                self.flt, self.sel = self.prompt("过滤: "), 0
            elif k == 27 and self.flt:
                self.flt = ""
            return False
        if self.view == "runs":
            if k in (c.KEY_DOWN, ord("j")) and self.runs:
                self.rsel = (self.rsel + 1) % len(self.runs)
            elif k in (c.KEY_UP, ord("k")) and self.runs:
                self.rsel = (self.rsel - 1) % len(self.runs)
            elif k in (10, 13, c.KEY_ENTER) and self.runs:
                self.switch_run(self.runs[self.rsel])
            elif k in (27, ord("q")):
                self.view = "board"
            return False
        if self.view == "alerts":
            # ↑↓ picks an entry, Enter unfolds its full text (then ↑↓ scroll it), Esc steps back.
            if k == 27 and self.alert_expanded:
                self.alert_expanded, self.scroll = False, 0
                return False
            if k in (10, 13, c.KEY_ENTER) and self.alerts:
                self.alert_expanded, self.scroll = not self.alert_expanded, 0
                return False
            if not self.alert_expanded and self.alerts and k in (c.KEY_UP, c.KEY_DOWN, ord("k"), ord("j")):
                step = -1 if k in (c.KEY_UP, ord("k")) else 1
                self.alert_sel = max(0, min(len(self.alerts) - 1, self.alert_sel + step))
                self.alert_follow_end = False
                return False
        if k in (27, ord("q")) or (k == ord("o") and self.view == "observer"):
            self.view = self.back if self.view == "peek" else "board"
            self.back = "board"
            return False
        if k == ord("w") and self.view in {"peek", "coord", "detail"}:
            self.wrap = not self.wrap
            return False
        if self.view == "detail":
            if k == 9:
                axes = session_tabs(self.r, self.r.all_tickets().get(self.cur_tid()) or {}) \
                    if self.order else ["exec", *v.REVIEW_AXES]
                self.tab = axes[(axes.index(self.tab) + 1) % len(axes)] if self.tab in axes else "exec"
            elif k == ord("f"):
                self.view, self.scroll, self.back, self.pfilter = "peek", -1, "detail", v.FILTER_ALL
            elif k == ord("e"):
                self.open_evidence()
        if self.view in self.SCROLL_VIEWS:
            h = self.scr.getmaxyx()[0]
            total = len(self.rendered())
            cur = max(0, total - (h - 1)) if self.scroll < 0 else self.scroll
            step = {c.KEY_UP: -1, c.KEY_DOWN: 1, c.KEY_PPAGE: -(h - 2), c.KEY_NPAGE: h - 2}.get(k)
            if step is not None:
                nxt = max(0, cur + step)
                self.scroll = -1 if nxt >= total - (h - 1) else nxt
            elif k == c.KEY_END:
                self.scroll = -1
            elif k == 9 and self.view in {"peek", "coord"}:
                self.pfilter = v.next_peek_filter(self.pfilter)
        return False


# ---------------------------------------------------------------- coordinator writers


def csv(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [x.strip() for x in value.split(",") if x.strip()]


def nullable(value: str | None) -> str | None:
    return None if value is None or value.lower() == "none" else value


def cmd_batch_set(args: argparse.Namespace) -> int:
    fields: dict[str, Any] = {"dag": args.dag, "title": args.title, "integration_ref": args.integration_ref,
                              "accepted_tip": args.accepted_tip, "repo": args.repo}
    remote = {k: val for k, val in (("ref", args.remote_ref), ("readback", args.remote_readback)) if val}
    if remote:
        fields["remote"] = remote
    run_path = find_run(args.run) / "run.json"
    batch = v.update_run(run_path, lambda run: v.apply_batch_update(run, args.batch, fields))
    v.register_run(run_path.parent)  # so `host-dag runs` and the board's run list find it
    given = [k for k, val in fields.items() if val is not None and val != {}]
    finish = core_mirror(run_path.parent, "batch set", None,
                         lambda sm: sm.mirror_batch_set(run_path.parent, args.batch, given),
                         {"batch": args.batch, "fields": fields})
    print(json.dumps({k: val for k, val in batch.items() if k != "tickets"}, ensure_ascii=False))
    finish()
    return 0


# ---------------------------------------------------------------- stage B4: core mirror and core commands


MIRROR_WARN = "host-dag: core mirror skipped"


class _MirrorInterrupted(BaseException):
    """SIGTERM while waiting for the mirror: stop waiting, still print the legacy result."""


@contextlib.contextmanager
def _signals_stop_waiting():
    """While host-dag waits for the mirror, SIGTERM (and SIGINT, as KeyboardInterrupt) end the wait
    instead of the process, so the legacy result is still printed with its usual exit code."""
    previous = None
    if threading.current_thread() is threading.main_thread():
        def stop(signum, frame):
            raise _MirrorInterrupted(f"signal {signum}")
        try:
            previous = signal.signal(signal.SIGTERM, stop)
        except (ValueError, OSError):
            previous = None
    try:
        yield
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)


def _mirror_budget() -> float:
    try:
        return max(0.1, float(os.environ.get("HOST_DAG_DAG_MIRROR_BUDGET") or 2.0))
    except ValueError:
        return 2.0


def _mirror_grace() -> float:
    """Seconds a timed-out mirror may still finish AFTER the legacy result is printed (0: abandon it at
    the budget, as before core-mirror). What does not finish stays in the retry queue."""
    try:
        return max(0.0, float(os.environ.get("HOST_DAG_DAG_MIRROR_GRACE") or 5.0))
    except ValueError:
        return 5.0


def _new_base() -> str:
    """hostdag_dag_translate.new_base() without importing the core before a core is known."""
    import uuid  # noqa: PLC0415
    return "hv-" + uuid.uuid4().hex[:16]


def _core_db_exists(run_dir: Path) -> bool:
    """The only run-disk touch before host-dag knows a core exists (a test hook for a slow disk)."""
    return (run_dir / "sm" / "state.db").is_file()


def core_mirror(run_dir: Path, command: str, ticket: str | None, call, record: dict[str, Any], *,
                retry=None, base: str | None = None):
    """Shadow mirror of a legacy write that already succeeded, run BEFORE its result is printed (so the
    one possible warning precedes stdout in every buffering mode). Everything that touches the run disk,
    even the state.db stat, runs in a daemon worker bounded by HOST_DAG_DAG_MIRROR_BUDGET (default 2 s).
    It never changes this command's output or exit code and prints at most one stderr line.

    Until the worker has seen <run>/sm/state.db, host-dag knows of no core: if the stat does not answer
    within the budget, fails, or says there is none, or the wait is interrupted, it returns silently
    (no warning, no spool, no signal handler; re-review I2) and the worker, if it ever gets past the
    stat, does nothing. Only once a core is known are SIGTERM/SIGINT turned into "stop waiting", and a
    write the core then missed (failure, timeout, interruption) is spooled on the internal disk
    (hostdag_dag_cli.spool_lost), not under the possibly stalled run dir.

    core-mirror: with ``retry`` (ticket set: what replaying needs) and the op ``base`` fixed
    here, a missed write is also queued (hostdag_dag_cli.queue_retry) and the worker first drains older
    queued writes. Returns ``finish``, which the caller runs after printing: a timed-out worker gets
    HOST_DAG_DAG_MIRROR_GRACE seconds (default 5) to end, and if it did, its queue entry is resolved."""
    def nothing() -> None:
        return None
    if os.environ.get("HOST_DAG_DAG_MIRROR") == "0":
        return nothing
    lock = threading.Lock()
    decided = threading.Event()
    state: dict[str, Any] = {"go": False, "gave_up": False}

    def body() -> None:
        try:
            present = _core_db_exists(run_dir)
        except BaseException:  # noqa: BLE001 - an unreadable run dir is "no core known"
            present = False
        with lock:
            if not present or state["gave_up"]:
                decided.set()
                return
            state["go"] = True
            decided.set()
        try:
            import hostdag_dag_cli as sm  # noqa: PLC0415 - only when a core database exists
            if retry is not None:
                try:  # older writes first, so the core sees them in order
                    state["drained"] = sm.drain_retry_queue(run_dir, deadline=started + budget + _mirror_grace())
                except Exception:  # noqa: BLE001 - draining is best effort; this write still runs
                    pass
            state["result"] = call(sm)
        except BaseException as exc:  # noqa: BLE001
            state["error"] = exc

    def give_up_unless_core() -> bool:
        with lock:
            if not state["go"]:
                state["gave_up"] = True
                return True
            return False

    budget = _mirror_budget()
    started = time.monotonic()
    worker = threading.Thread(target=body, name="host-dag-core-mirror", daemon=True)
    status, detail = "", ""
    try:
        worker.start()
        decided.wait(budget)
    except BaseException as exc:  # noqa: BLE001 - KeyboardInterrupt while finding out whether a core exists
        if give_up_unless_core():
            return nothing  # no core known: the legacy path only
        status, detail = "interrupted", f"{type(exc).__name__}: {exc}"  # core known: not swallowed (N-b)
    if not status and give_up_unless_core():
        return nothing
    if not status:
        try:
            with _signals_stop_waiting():
                worker.join(max(0.0, budget - (time.monotonic() - started)))
            if worker.is_alive():
                status, detail = "timeout", f"still running after {budget:g}s; outcome unknown"
            elif "error" in state:
                status, detail = "error", f"{type(state['error']).__name__}: {state['error']}"
            elif "result" in state and state["result"].warn:
                status, detail = state["result"].status, state["result"].detail
            else:
                return nothing
        except BaseException as exc:  # noqa: BLE001 - KeyboardInterrupt / SIGTERM while waiting
            status, detail = "interrupted", f"{type(exc).__name__}: {exc}"
    queued = None
    try:
        import hostdag_dag_cli as sm  # noqa: PLC0415 - local disk
        if retry is not None and base is not None and status != "mode":
            queued = sm.queue_retry(run_dir, base=base, status=status, detail=detail, **retry(sm))
    except BaseException:  # noqa: BLE001
        queued = None
    try:
        tail = f"; queued for retry as {base}" if queued is not None else ""
        print(f"{MIRROR_WARN} ({status}: {detail}); run.json was updated{tail}", file=sys.stderr)
        import hostdag_dag_cli as sm  # noqa: PLC0415 - local disk
        sm.spool_lost(run_dir, command=command, ticket=ticket, status=status, detail=detail, record=record)
    except BaseException:  # noqa: BLE001
        pass
    if status != "timeout" or queued is None:
        return nothing

    def finish() -> None:
        """After the result is printed: let the slow worker end (bounded), then resolve its entry."""
        try:
            with contextlib.suppress(Exception):
                sys.stdout.flush()
                sys.stderr.flush()
            with _signals_stop_waiting():
                worker.join(_mirror_grace())
            result = state.get("result")
            if worker.is_alive() or result is None or result.status not in sm.RETRY_DONE:
                return  # the queue keeps it; the next mirror or `host-dag sm retry` replays the same base
            queued.unlink(missing_ok=True)
            sm._retry_log(run_dir, {"event": "resolved", "base": base, "ticket": ticket, "status": result.status,
                                    "detail": "the timed-out mirror finished in the exit grace"})
        except BaseException:  # noqa: BLE001 - SIGTERM / Ctrl-C: exit now, the entry stays queued
            pass
    return finish


def _sm():
    import hostdag_dag_cli as sm  # noqa: PLC0415
    return sm


def core_command(args: argparse.Namespace, run) -> int:
    """Run one core-only command: JSON on stdout; exit 1 when the core refuses, 2 when it cannot run here."""
    sm = _sm()
    import hostdag_dag_core as core  # noqa: PLC0415
    try:
        out = run(sm, find_run(args.run))
    except sm.CliError as exc:
        print(f"host-dag {args.cmd}: {exc}", file=sys.stderr)
        return 2
    except core.BusyError as exc:
        print(f"host-dag {args.cmd}: core busy, nothing accepted ({exc}); retry", file=sys.stderr)
        return 2
    except core.StorageError as exc:
        print(f"host-dag {args.cmd}: core storage error, nothing accepted ({exc})", file=sys.stderr)
        return 2
    except core.CoreError as exc:
        print(f"host-dag {args.cmd}: refused by the core: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(sm.dump(out))
    return 0


def cmd_exec_request(args: argparse.Namespace) -> int:
    return core_command(args, lambda sm, run: sm.exec_request(
        run, args.ticket, role=args.role, mode=args.mode, provider=args.provider, model=args.model,
        effort=args.effort, contract=args.contract, session=args.session, resume_of=args.resume_of,
        axis=args.axis, override_reason=args.override_reason, receipt=args.quota_receipt))


def cmd_question(args: argparse.Namespace) -> int:
    if args.op == "ask":
        return core_command(args, lambda sm, run: sm.question_ask(
            run, args.ticket, to=args.to, key=args.key, title=args.title, question_file=args.question_file))
    return core_command(args, lambda sm, run: sm.question_link(run, args.ticket, args.key, title=args.title))


def cmd_decide(args: argparse.Namespace) -> int:
    return core_command(args, lambda sm, run: sm.decide(
        run, args.ticket, response=args.response, expected_revision=args.expected_revision,
        action_id=args.action_id, question_id=args.question_id, reason=args.reason, verdict=args.verdict,
        actor="user" if args.as_user else "coordinator"))


def cmd_decisions(args: argparse.Namespace) -> int:
    if not args.pending:
        print("host-dag decisions: only --pending is supported", file=sys.stderr)
        return 2
    return core_command(args, lambda sm, run: sm.decisions_pending(run, args.ticket))


def cmd_wait(args: argparse.Namespace) -> int:
    if args.op == "add":
        return core_command(args, lambda sm, run: sm.wait_add(run, args.ticket, kind=args.kind, ref=args.ref,
                                                              note=args.note, wait_id=args.wait_id))
    if args.op == "update":
        return core_command(args, lambda sm, run: sm.wait_update(run, args.ticket, wait_id=args.wait_id,
                                                                 ref=args.ref, reason=args.reason, note=args.note))
    return core_command(args, lambda sm, run: sm.wait_waive(run, args.ticket, wait_id=args.wait_id, reason=args.reason,
                                                            actor="user" if args.as_user else "coordinator"))


def cmd_pagewindow(args: argparse.Namespace) -> int:
    if args.op == "enqueue":
        return core_command(args, lambda sm, run: sm.pagewindow_enqueue(
            run, args.ticket, spec=args.spec, priority=args.priority, needs_guest=args.needs_guest,
            est_seconds=args.est_seconds))
    return core_command(args, lambda sm, run: sm.pagewindow_policy(run, args.ticket, priority=args.priority,
                                                                   reason=args.reason, entry_id=args.entry_id))


def cmd_authority(args: argparse.Namespace) -> int:
    return core_command(args, lambda sm, run: sm.authority_switch(run, mode=args.mode, epoch=args.epoch,
                                                                  force=args.force, yes=args.yes, reason=args.reason))


def cmd_sm(args: argparse.Namespace) -> int:
    if args.op == "quiesce":
        return core_command(args, lambda sm, run: sm.sm_quiesce(run, reason=args.reason, timeout=args.timeout))
    if args.op == "resume":
        return core_command(args, lambda sm, run: sm.sm_resume(run))
    if args.op == "reconcile":
        return core_command(args, lambda sm, run: sm.reconcile_closures(
            run, apply=args.apply, tickets=csv(args.tickets) or (), reason=args.reason or sm.RECONCILE_REASON))
    if args.op == "retry":
        return core_command(args, lambda sm, run: sm.drain_retry_queue(run, dry_run=args.dry_run))
    return core_command(args, lambda sm, run: sm.sm_status(run))


def cmd_ticket_set(args: argparse.Namespace) -> int:
    fields = ticket_fields(args)
    if fields.get("withdraw_release_disposition") is not None and any(
            getattr(args, key, None) is not None for key in B4_TICKET_FLAGS):
        raise v.ViewError("--withdraw-release-disposition cannot be combined with other ticket edits")
    run_path = find_run(args.run) / "run.json"
    vector = getattr(args, "release_candidate_vector", None)
    dry_run = getattr(args, "dry_run", False)
    if (vector is not None or dry_run) and (args.degrade_result or args.lint_action
            or args.rollback_lint_action or args.release_done_wait or args.parent_registration
            or fields.get("withdraw_release_disposition") is not None):
        raise v.ViewError("candidate vector and dry-run require an ordinary ticket update")
    if vector is not None and fields.get("state") not in v.ts.DONE and fields.get("status") not in v.ts.DONE:
        raise v.ViewError("--release-candidate-vector requires an ACCEPTED or INTEGRATED update")
    if args.degrade_result:
        if not args.expected_revision or any(value not in (None,False) for value in fields.values()) or args.lint_action or args.rollback_lint_action:
            raise v.ViewError('--degrade-result needs only --expected-revision')
        from hostdag_lint_actions import finish_degrade
        try:result=finish_degrade(run_path,args.batch,args.ticket,args.degrade_result,args.expected_revision)
        except (ValueError,OSError,KeyError) as exc:raise v.ViewError(str(exc)) from exc
        print(json.dumps(result));return 0
    if args.parent_registration is not None and (args.lint_action != "wait-on-decided" or args.rollback_lint_action):
        raise v.ViewError("--parent-registration requires only --lint-action wait-on-decided")
    if args.lint_action or args.rollback_lint_action:
        if (not args.expected_revision or args.release_done_wait or
                (args.lint_action and args.rollback_lint_action) or
                any(value not in (None, False) for value in fields.values()) or
                any(getattr(args, key, None) is not None for key in B4_TICKET_FLAGS)):
            raise v.ViewError("lint action needs --expected-revision and no other ticket edits")
        from hostdag_lint_actions import execute
        print(json.dumps(execute(run_path, args.batch, args.ticket, args.lint_action,
                                 args.expected_revision, args.rollback_lint_action,
                                 parent_registration=args.parent_registration), ensure_ascii=False))
        return 0
    if args.release_done_wait:
        if not args.expected_revision or any(value not in (None, False) for value in fields.values()) or any(
                getattr(args, key, None) is not None for key in B4_TICKET_FLAGS):
            raise v.ViewError("--release-done-wait needs --expected-revision and no other ticket edits")
        # This narrow legacy action never enters the frozen Stage B mirror.
        with v.RunLock(run_path):
            mtime = run_path.stat().st_mtime
            run = v.load_json(run_path)
            revision = v.done_wait_revision(run, args.batch, args.ticket)
            receipt = {"ticket": args.ticket, "batch": args.batch, "result": "skipped",
                       "expected_revision": args.expected_revision}
            from hostdag_lint_actions import lint_module
            if args.ticket in lint_module().core_tickets(run):
                receipt["reason"] = "core-chain: parent only"
                revision = None
            if revision is not None and revision == args.expected_revision:
                t = run["batches"][args.batch]["tickets"][args.ticket]
                before = copy.deepcopy(t)
                receipt["wait_on"] = t["wait_on"]
                state = v.ts.WAITING_DEPENDENCY if v.ts.dependency_status(t, v.all_tickets(run))[0] else v.ts.CREATED
                after = v.apply_ticket_update(run, args.batch, args.ticket,
                                              {"state": state, "wait_on": "", "wait_holder": ""},
                                              run_dir=run_path.parent)
                from hostdag_lint_actions import commit
                commit(run_path, run, mtime, args.batch, args.ticket, before, after, "wait-on-done")
                receipt.update(result="released", state=after["state"], updated_at=after["updated_at"])
            else:
                receipt["reason"] = "condition or revision changed"
        print(json.dumps(receipt, ensure_ascii=False))
        return 0
    if args.expected_revision is not None:
        raise v.ViewError("--expected-revision requires --release-done-wait")
    seen: dict[str, Any] = {}

    def mutate(run: dict[str, Any]) -> dict[str, Any]:
        # Fail closed (TOOL-GUARD-FAIL-CLOSED): a guard that crashes refuses the write as INFRA_ERROR.
        try:
            return guarded_mutate(run)
        except v.ViewError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise v.infra_refusal([f"guard crashed: {type(exc).__name__}: {exc}"]) from exc

    def guarded_mutate(run: dict[str, Any]) -> dict[str, Any]:
        try:  # what the ticket looked like before this write, for the B4 core mirror only
            seen["before"] = copy.deepcopy(((run.get("batches") or {}).get(args.batch) or {})
                                           .get("tickets", {}).get(args.ticket))
        except Exception:  # noqa: BLE001
            seen["before"] = None
        if seen["before"] is None and not fields["title"]:
            raise v.ViewError(f"new ticket {args.ticket} needs --title (the short Chinese name the board lists)")
        effective={**(seen['before'] or {}),**{k:v for k,v in fields.items() if v is not None}}
        if v.critical_tool(effective) and (fields.get('degrade_plan') is not None or fields.get('critical_path') is True or fields.get('kind')=='tool'):
            from hostdag_lint_actions import degrade_schema
            try:degrade_schema(run_path.parent,effective)
            except (ValueError,OSError,TypeError,KeyError) as exc:raise v.ViewError('DEGRADE_PLAN_INVALID: '+str(exc)) from exc
        # Evaluate the established Git/integration guards before adjunct checks.
        # update_run persists only after every check below succeeds.
        updated = v.apply_ticket_update(run, args.batch, args.ticket, fields, run_dir=run_path.parent)
        from hostdag_fix_regression import validate_update
        try:
            validate_update(run, args.ticket, seen["before"] or {}, fields, run_path.parent)
        except ValueError as exc:
            raise v.ViewError(str(exc)) from exc
        from hostdag_adjunct_lock_policy import validate_update as validate_adjunct_lock
        try:
            kwargs = {"release_candidate_vector": vector} if vector is not None else {}
            proof = validate_adjunct_lock(run, args.ticket, seen["before"] or {}, fields, run_path.parent, **kwargs)
        except ValueError as exc:
            raise v.ViewError(str(exc)) from exc
        if proof is not None:
            updated["adjunct_release_vector"] = proof
        elif (v.ts.derive_state(updated) not in v.ts.DONE
              or updated.get("candidate") != (seen["before"] or {}).get("candidate")):
            updated.pop("adjunct_release_vector", None)
        from hostdag_dispatch_guard import validate_review_registration, DispatchError
        try:
            validate_review_registration(run,args.ticket,fields,args.batch)
        except DispatchError as exc:
            raise v.ViewError(exc.message) from exc
        return updated
    if dry_run:
        preview = mutate(v.load_json(run_path))
        print(json.dumps({args.ticket: preview}, ensure_ascii=False))
        return 0
    t = v.update_run(run_path, mutate)
    after = copy.deepcopy(t)
    base, built = _new_base(), {}

    def build():  # in the worker: may read the --approve-window spec on the run disk
        built["req"] = ticket_set_request(args, fields)
        return built["req"]
    finish = core_mirror(
        run_path.parent, "ticket set", args.ticket, lambda sm: sm.mirror_ticket_set(
            run_path.parent, build, seen.get("before"), after, base=base),
        {"batch": args.batch, "fields": fields, "after_flag": getattr(args, "after", None), "by": getattr(args, "by", None),
         "affects_downstream": getattr(args, "affects_downstream", None), "before": seen.get("before"), "after": after},
        # main thread: never re-read the spec here (the replay hashes it again, in its own worker)
        retry=lambda sm: {"request": sm.request_json(built.get("req") or ticket_set_request(args, fields, digest=False)),
                          "before": seen.get("before"), "after": after}, base=base)
    print(json.dumps({args.ticket: t}, ensure_ascii=False))
    finish()
    return 0


def ticket_fields(args: argparse.Namespace) -> dict[str, Any]:
    """The fields of one `ticket set`, as apply_ticket_update takes them."""
    fields: dict[str, Any] = {
        "kind": v.normalize_ticket_kind(args.kind) if getattr(args, "kind", None) is not None else None,
        "title": args.title, "deps": csv(args.deps), "round": args.round, "executor": args.executor, "owner": args.owner,
        "model": args.model, "effort": args.effort, "candidate": args.candidate, "base": args.base,
        "candidate_decision_ref": args.candidate_decision_ref,
        "release_group": getattr(args, "release_group", None),
        "requires_for_release": csv(getattr(args, "requires_for_release", None)),
        "production_write_fence": getattr(args, "production_write_fence", None),
        "coverage_migration_plan": str(args.coverage_migration_plan) if getattr(args, "coverage_migration_plan", None) is not None else None,
        "branch": args.branch, "worktree": args.worktree, "evidence": args.evidence, "scope": args.scope,
        "blocker": args.blocker, "accepted_at": args.accepted_at,
        "wait_on": args.wait_on, "wait_holder": args.wait_holder,
        "state": args.state, "attempt": args.attempt, "accept_scope": args.accept_scope,
        "window_spec": args.approve_window, "window_priority": args.window_priority,
    }
    for key in ('root_ticket','chain_id','review_token','review_from'):
        if getattr(args,key,None) is not None:fields[key]=getattr(args,key)
    if getattr(args, "critical_path", None) is not None:
        fields["critical_path"] = args.critical_path == "true"
    for key in ("degrade_plan", "degrade_decision", "freeze_exempt"):
        raw = getattr(args, key, None)
        if raw is not None:
            try:
                fields[key] = json.loads(raw)
                if not isinstance(fields[key], dict) or not fields[key]:
                    raise ValueError(f"{key} must be a nonempty JSON object")
            except (ValueError, TypeError) as exc:
                raise v.ViewError(str(exc)) from exc
    if getattr(args, "defect_class", None) is not None:
        fields["defect_class"] = args.defect_class
    if getattr(args, "regression", None) is not None:
        try:
            fields["regression"] = json.loads(args.regression)
            if not isinstance(fields["regression"], dict):
                raise ValueError("regression must be a JSON object")
        except (ValueError, TypeError) as exc:
            raise v.ViewError(str(exc)) from exc
    if getattr(args, "release_disposition", None) is not None:
        try:
            raw_disposition = json.loads(args.release_disposition)
        except (ValueError, TypeError) as exc:
            raise v.ViewError(str(exc)) from exc
        try:
            from release_coverage import validate_disposition
        except ImportError:
            v._warn_missing_delivery("release_coverage")
            fields["release_disposition"] = raw_disposition
        else:
            try:
                fields["release_disposition"] = validate_disposition(raw_disposition)
            except (ValueError, TypeError) as exc:
                raise v.ViewError(str(exc)) from exc
    if getattr(args, "withdraw_release_disposition", None) is not None:
        fields["withdraw_release_disposition"] = args.withdraw_release_disposition
    if getattr(args, "integrate_head", None) is not None:
        fields["integrate_heads"] = args.integrate_head
    if getattr(args, "expected_updated_at", None) is not None:
        fields["expected_updated_at"] = args.expected_updated_at
    if args.status is not None:
        fields["status"] = nullable(args.status)
    if args.outcome is not None:
        fields["outcome"] = nullable(args.outcome)
    reviews: dict[str, dict[str, Any]] = {}
    for spec in args.review or []:
        axis, _, rest = spec.partition("=")
        thread, _, verdict = rest.partition(":")
        reviews[axis] = {"thread": thread or None, "verdict": verdict or None}
    if reviews:
        fields["reviews"] = reviews
    fields["clear_reviews"] = args.clear_reviews
    return fields


def ticket_set_request(args: argparse.Namespace, fields: dict[str, Any], *, digest: bool = True):
    import hostdag_dag_translate as tr  # noqa: PLC0415
    affects = csv(getattr(args, "affects_downstream", None))
    want_digest, digest = digest, None
    if args.approve_window and want_digest:
        import hostdag_dag_cli as sm  # noqa: PLC0415
        digest = sm.spec_digest(args.approve_window)
    # The legacy write already validated these controls under run.json.lock;
    # they are board-only inputs, not core state-machine fields.
    translated = {k: val for k, val in fields.items()
                  if k not in ("expected_updated_at", "withdraw_release_disposition")}
    return tr.TicketSet(args.batch, args.ticket, translated, after=getattr(args, "after", None),
                        affects_downstream=tuple(affects) if affects is not None else None, by=getattr(args, "by", None),
                        spec_digest=digest, run_dir=str(find_run(args.run)))


LEGACY_COMMANDS = ("peek", "board", "runs", "batch", "ticket")
B4_TICKET_FLAGS = ("after", "affects_downstream", "by")


# The legacy usage text below relies on PRIVATE argparse API (argparse._SubParsersAction,
# HelpFormatter._metavar_formatter, ArgumentParser._check_value), which Python may change in any
# release. The guard is the byte comparison of the real old and new scripts in a subprocess on every
# interpreter (tests/test_hostdag_host_view_b4.py ArgparseGuardTests, plus LiveCompatTests in-process):
# keep both; an interpreter upgrade that breaks these overrides must fail there first.
def _legacy_view(action: argparse.Action) -> bool:
    return isinstance(action, argparse._SubParsersAction) and action.dest == "cmd"


class LegacyUsageFormatter(argparse.RawDescriptionHelpFormatter):
    """Top-level usage/help shows only the legacy commands, exactly as before B4 (review P2-1); the B4
    core commands are reachable but unlisted (each has its own --help)."""

    def _metavar_formatter(self, action, default_metavar):
        if _legacy_view(action) and action.metavar is None:
            legacy = "{%s}" % ",".join(c for c in action.choices if c in LEGACY_COMMANDS)
            return lambda size: (legacy,) * size
        return super()._metavar_formatter(action, default_metavar)


class HostDagParser(argparse.ArgumentParser):
    def _check_value(self, action, value):
        """An unknown command is reported with the legacy choice list (same text as before B4)."""
        if _legacy_view(action) and value not in action.choices:
            full = action.choices
            action.choices = {k: p for k, p in full.items() if k in LEGACY_COMMANDS}
            try:
                super()._check_value(action, value)
            finally:
                action.choices = full
        super()._check_value(action, value)


class _ProbeError(Exception):
    pass


class _ProbeParser(HostDagParser):
    def error(self, message):  # noqa: D401 - raise instead of print + exit, to try another parser
        raise _ProbeError(message)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Review P3-3: the hidden B4 `ticket set` flags (--after, --affects-downstream, --by) exist only
    when the run has sm/state.db. Parse as before B4 first; only when that fails, and the full parser
    succeeds with a B4 flag on a run that has a core database, take the B4 parse. Otherwise the legacy
    parser reports the error itself: same exit code 2, same text as before."""
    argv = sys.argv[1:] if argv is None else list(argv)
    try:
        return parser(hidden=False, cls=_ProbeParser).parse_args(argv)
    except _ProbeError:
        pass
    try:
        full = parser(hidden=True, cls=_ProbeParser).parse_args(argv)
    except _ProbeError:
        full = None
    if full is not None and getattr(full, "func", None) is cmd_ticket_set \
            and any(getattr(full, k, None) is not None for k in B4_TICKET_FLAGS):
        try:
            if (find_run(full.run) / "sm" / "state.db").is_file():
                return full
        except v.ViewError:
            pass
    return parser(hidden=False).parse_args(argv)


def parser(hidden: bool = True, cls: type[argparse.ArgumentParser] = HostDagParser) -> argparse.ArgumentParser:
    ap = cls(prog="host-dag", description=__doc__, formatter_class=LegacyUsageFormatter)
    ap.add_argument("--run", help="run directory, or a key from `host-dag runs`: run id prefix, directory name or batch name")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pk = sub.add_parser("peek", help="show one executor's timeline")
    pk.add_argument("target", help="ticket id or Codex thread id")
    pk.add_argument("--role", choices=["exec", *v.REVIEW_AXES], default="exec")
    pk.add_argument("-f", "--follow", action="store_true")
    pk.add_argument("--tools", action="store_true", help="tool calls only")
    pk.add_argument("-n", "--lines", type=int, default=0, help="last N timeline lines")
    pk.add_argument("--wrap", type=int, default=3, help="wrap long lines to at most N rows; 1 cuts them with …")
    pk.set_defaults(func=cmd_peek)

    bd = sub.add_parser("board", help="read-only TUI panel", description=(
        "Read-only TUI panel. Flow v2 (control/flow-rules-20261022.md): fix tickets no longer open a page"
        " window (新流程下，修复票不再开页面窗口); windows only run batch acceptance and the fixed smoke spec."
        " SUPERSEDED tickets named in control/leftover-ledger.jsonl show as 转遗留, the rest as 作废."))
    bd.add_argument("--batch")
    bd.set_defaults(func=cmd_board)

    gr = sub.add_parser("graph", help="export the board's dependency graph as text (read-only)")
    gr.add_argument("-o", "--output", help="output file, or - for stdout; default: dated file in the download directory")
    gr.set_defaults(func=cmd_graph)

    ac = sub.add_parser("activity", help="is anything actually working, or is the DAG stuck (read-only)")
    ac.add_argument("--stale-minutes", type=float, default=10, help="a session counts as active if written within this time")
    ac.set_defaults(func=cmd_activity)

    rn = sub.add_parser("runs", help="list registered runs and their batches")
    rn.add_argument("--add", action="append", help="register a run directory")
    rn.add_argument("--scan", help="find runs with batches under this root (skips evidence trees) and register them")
    rn.set_defaults(func=cmd_runs)

    bs = sub.add_parser("batch", help="coordinator: record batch metadata").add_subparsers(dest="op", required=True)
    b = bs.add_parser("set")
    b.add_argument("--batch", required=True)
    b.add_argument("--dag")
    b.add_argument("--title", help="short Chinese name shown on the board (default: the DAG doc's H1)")
    b.add_argument("--integration-ref")
    b.add_argument("--accepted-tip")
    b.add_argument("--remote-ref")
    b.add_argument("--remote-readback")
    b.add_argument("--repo", help="git repo used to count how far the remote is behind")
    b.set_defaults(func=cmd_batch_set)

    ts = sub.add_parser("ticket", help="coordinator: record ticket state").add_subparsers(dest="op", required=True)
    t = ts.add_parser("set")
    t.add_argument("--batch", required=True)
    t.add_argument("--ticket", required=True)
    t.add_argument("--title")
    t.add_argument("--kind", help="ticket kind: tool|doc|product|ops|runtime-only (case-insensitive; stored lowercase)")
    t.add_argument("--freeze-exempt", help="L1265 JSON exemption bound to a registered failed release step")
    t.add_argument("--root-ticket", help="stable root work ticket for review budget")
    t.add_argument("--chain-id", help="stable review work chain (root_work_id)")
    t.add_argument("--review-token", help="delivered review reservation token (rv_register)")
    t.add_argument("--review-from", help="BATCH/TICKET owning the delivered token for an explicit merged-review reference")
    t.add_argument("--critical-path", choices=("true", "false"), help="critical-path prerequisite (§16)")
    t.add_argument("--degrade-plan", help="structured option a/b/c/NONE execution contract; free-text fallback_ref invalid")
    t.add_argument("--degrade-decision", help="nonempty JSON object recording §16 degradation decision")
    t.add_argument("--defect-class", help="registered defect slug")
    t.add_argument("--regression", help="JSON check_cmd/red_ref/green_ref; recurrence: why_existing_check_missed")
    t.add_argument("--scope")
    t.add_argument("--owner", help="parent responsible for this ticket")
    t.add_argument("--deps", help="comma-separated ticket ids")
    t.add_argument("--release-group", help="atomic release group; independent of construction deps")
    t.add_argument("--requires-for-release", help="comma-separated release requirements (batch/ticket for ambiguous ids)")
    t.add_argument("--status", help=f"{'|'.join(v.STATUSES)}|none")
    t.add_argument("--outcome", help=f"{'|'.join(v.OUTCOMES)}|none")
    t.add_argument("--state", help=f"{'|'.join(v.ts.STATES)}; also sets the matching status/outcome")
    t.add_argument("--attempt", help="executions actually dispatched (N or +1); admission bounces do not count")
    t.add_argument("--accept-scope", help="FULL|SCOPED|REDUCED, with --state ACCEPTED/INTEGRATED")
    t.add_argument("--round", type=int)
    t.add_argument("--executor", help="executor thread id")
    t.add_argument("--expected-updated-at", help=argparse.SUPPRESS)
    t.add_argument("--degrade-result", help=argparse.SUPPRESS)
    t.add_argument("--lint-action", choices=("review-cap-degrade-due", "freeze-gate-clear", "freeze-gate-stale-exempt", "deps-all-done", "verdict-not-registered", "wait-on-decided", "queued-but-unit-failed"), help=argparse.SUPPRESS)
    t.add_argument("--rollback-lint-action", help=argparse.SUPPRESS)
    t.add_argument("--parent-registration", help="absolute parent registration path; only with --lint-action wait-on-decided")
    t.add_argument("--release-done-wait", action="store_true", help=argparse.SUPPRESS)
    t.add_argument("--expected-revision", help=argparse.SUPPRESS)
    t.add_argument("--model")
    t.add_argument("--effort")
    t.add_argument("--review", action="append", help="AXIS=THREAD[:PASS|FAIL], axis spec|standards")
    t.add_argument("--clear-reviews", action="store_true", help="drop review records (new round)")
    t.add_argument("--candidate")
    t.add_argument("--production-write-fence", action="store_true", default=None,
                   help="enable production fence protection; monotonic audited metadata")
    t.add_argument("--coverage-migration-plan", type=Path, help="snapshot-bound Fable-approved single-ticket migration plan")
    t.add_argument("--candidate-decision-ref", help="run-relative Fable candidate attribution, advisor/log.jsonl#Ln")
    t.add_argument("--release-candidate-vector", type=Path, help=argparse.SUPPRESS)
    t.add_argument("--dry-run", action="store_true", help=argparse.SUPPRESS)
    t.add_argument("--integrate-head", action="append", metavar="REPO:REF",
                   help="INTEGRATED target override; resolved with release-heads.json repository mapping")
    t.add_argument("--release-disposition", help="JSON release disposition (deferred|superseded|not_applicable)")
    t.add_argument("--withdraw-release-disposition", metavar="DECISION_REF",
                   help="withdraw an accepted ticket's release disposition with advisor/log.jsonl#Ln")
    t.add_argument("--base")
    t.add_argument("--branch")
    t.add_argument("--worktree")
    t.add_argument("--evidence")
    t.add_argument("--blocker", help="decision needed / external condition / supersession note")
    t.add_argument("--wait-on", help="resource a WAITING_RESOURCE ticket waits on (required with that state): "
                                      + ", ".join(v.WAIT_KINDS) + "; kind or kind:detail; empty clears")
    t.add_argument("--wait-holder", help="ticket or owner currently holding that resource; empty clears")
    t.add_argument("--accepted-at", help="ISO time of an earlier acceptance when backfilling (with --status ACCEPTED)")
    t.add_argument("--approve-window", metavar="SPEC", help="approve this page-window spec (grant already in it); the "
                   "watcher runs it when the page is free. Empty withdraws a request that has not started")
    t.add_argument("--window-priority", type=int, help="higher runs first among approved page requests (default 0)")
    # B4 core-mirror-only flags (the board ignores them): --after (what a new CREATED ticket is deferred
    # until: ticket id, ticket:X or batch:Y:bound|completed), --affects-downstream (comma-separated
    # tickets a SCOPED/REDUCED acceptance affects; '' = none, said explicitly), --by (the superseding
    # ticket). Hidden from usage/help so the legacy usage text, which argparse prints on every error,
    # stays byte-identical. parse_args() accepts them only on a run with sm/state.db (review P3-3).
    if hidden:
        t.add_argument("--after", help=argparse.SUPPRESS)
        t.add_argument("--affects-downstream", help=argparse.SUPPRESS)
        t.add_argument("--by", help=argparse.SUPPRESS)
    t.set_defaults(func=cmd_ticket_set)

    # stage B4 core-only commands (need <run>/sm/state.db and the pinned interpreter)
    ex = sub.add_parser("exec", description="core: request an execution").add_subparsers(dest="op", required=True)
    e = ex.add_parser("request", help="execution.request: new_attempt / resume / adopt (reviewer: --role reviewer)")
    e.add_argument("--ticket", required=True)
    e.add_argument("--role", choices=["executor", "reviewer"], default="executor")
    e.add_argument("--mode", choices=["new_attempt", "resume", "adopt"], default="new_attempt")
    e.add_argument("--axis", choices=list(v.REVIEW_AXES))
    e.add_argument("--session", help="existing thread/session (adopt; optional for resume)")
    e.add_argument("--resume-of", help="execution_id being resumed")
    e.add_argument("--provider")
    e.add_argument("--model")
    e.add_argument("--effort")
    e.add_argument("--contract", help="contract file the session gets")
    e.add_argument("--quota-receipt", help="JSON of `host-dag-quota.py select` (status OK) for provider/model/effort")
    e.add_argument("--override-reason", help="new round in REVIEW without a P0/P1 FAIL")
    e.set_defaults(func=cmd_exec_request)

    qs = sub.add_parser("question", description="core: ask or link a question").add_subparsers(dest="op", required=True)
    q = qs.add_parser("ask")
    q.add_argument("--ticket", required=True)
    q.add_argument("--to", choices=["advisor", "user"], required=True)
    q.add_argument("--key", required=True)
    q.add_argument("--title", required=True)
    q.add_argument("--question-file")
    q.set_defaults(func=cmd_question)
    q = qs.add_parser("link", help="register an advisor question already in advisor/log.jsonl (exact key or alias)")
    q.add_argument("--ticket", required=True)
    q.add_argument("--key", required=True)
    q.add_argument("--title")
    q.set_defaults(func=cmd_question)

    d = sub.add_parser("decide", description="core: respond to a pending decision (hold/escalate/acknowledge/answer)")
    d.add_argument("--ticket", required=True)
    d.add_argument("--response", required=True)
    d.add_argument("--expected-revision", type=int, required=True, help="the ticket revision you decided on")
    d.add_argument("--action-id")
    d.add_argument("--question-id", help="a user question, instead of --action-id")
    d.add_argument("--reason")
    d.add_argument("--verdict", help="with --response answer")
    d.add_argument("--as-user", action="store_true", help="the user answers (default: coordinator)")
    d.set_defaults(func=cmd_decide)

    dl = sub.add_parser("decisions", description="core: decisions still waiting for a response")
    dl.add_argument("--pending", action="store_true", required=True)
    dl.add_argument("--ticket")
    dl.set_defaults(func=cmd_decisions)

    ws = sub.add_parser("wait", description="core: coordinator waits").add_subparsers(dest="op", required=True)
    w = ws.add_parser("add")
    w.add_argument("--ticket", required=True)
    w.add_argument("--kind", choices=["ticket", "batch", "time", "external"], required=True)
    w.add_argument("--ref", required=True, help="ticket id | <batch>:bound|completed | ISO time | text")
    w.add_argument("--note")
    w.add_argument("--wait-id")
    w.set_defaults(func=cmd_wait)
    w = ws.add_parser("update", help="reschedule a time wait / reword an external wait")
    w.add_argument("--ticket", required=True)
    w.add_argument("--wait-id", required=True)
    w.add_argument("--ref", required=True)
    w.add_argument("--reason", required=True)
    w.add_argument("--note")
    w.set_defaults(func=cmd_wait)
    w = ws.add_parser("waive")
    w.add_argument("--ticket", required=True)
    w.add_argument("--wait-id", required=True)
    w.add_argument("--reason", required=True)
    w.add_argument("--as-user", action="store_true", help="external waits are waived by the user only")
    w.set_defaults(func=cmd_wait)

    ps = sub.add_parser("pagewindow", description="core: page-window queue").add_subparsers(dest="op", required=True)
    pw = ps.add_parser("enqueue", help="queue an approved spec (grant written, offline check passes, spec_digest pinned)")
    pw.add_argument("--ticket", required=True)
    pw.add_argument("--spec", required=True)
    pw.add_argument("--priority", type=int)
    pw.add_argument("--needs-guest", action="store_true")
    pw.add_argument("--est-seconds", type=int)
    pw.set_defaults(func=cmd_pagewindow)
    pw = ps.add_parser("policy", help="change a queued window's priority, with the reason")
    pw.add_argument("--ticket", required=True)
    pw.add_argument("--priority", type=int, required=True)
    pw.add_argument("--reason", required=True)
    pw.add_argument("--entry-id")
    pw.set_defaults(func=cmd_pagewindow)

    au = sub.add_parser("authority", description="core: authority switch (user)").add_subparsers(dest="op", required=True)
    a = au.add_parser("switch", help="to core only with --force and a passing 24 h shadow zero-diff gate")
    a.add_argument("--mode", choices=["shadow", "legacy", "freeze", "core"], required=True)
    a.add_argument("--epoch", type=int)
    a.add_argument("--reason", default="")
    a.add_argument("--force", action="store_true")
    a.add_argument("--yes", action="store_true")
    a.set_defaults(func=cmd_authority)

    ss = sub.add_parser("sm", description="core database: quiesce before unplugging the run disk, resume, status")
    ssub = ss.add_subparsers(dest="op", required=True)
    s = ssub.add_parser("quiesce", help="mark, wait for the controller cycle, checkpoint the WAL, close")
    s.add_argument("--reason", default="")
    s.add_argument("--timeout", type=float, default=60.0)
    s.set_defaults(func=cmd_sm)
    ssub.add_parser("resume", help="quick_check and remove the QUIESCED marker").set_defaults(func=cmd_sm)
    ssub.add_parser("status", help="read only: marker, heartbeat, interpreter, recent mirror results").set_defaults(
        func=cmd_sm)
    s = ssub.add_parser("reconcile", help="backfill accepted/integrated closures run.json has and the core lost "
                                          "(dry run unless --apply)")
    s.add_argument("--apply", action="store_true", help="write them (one transaction, actor seed, audited)")
    s.add_argument("--tickets", help="only these tickets (comma separated)")
    s.add_argument("--reason", default="")
    s.set_defaults(func=cmd_sm)
    s = ssub.add_parser("retry", help="replay mirror writes queued after a timeout/busy/error (same op ids)")
    s.add_argument("--dry-run", action="store_true", help="list the queue only")
    s.set_defaults(func=cmd_sm)
    return ap


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return args.func(args)
    except v.ViewError as exc:
        print(f"host-dag: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

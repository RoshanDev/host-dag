#!/usr/bin/env python3
"""Observe Codex Host threads and Grok ACP sessions; wake one parent with bounded event packets."""

from __future__ import annotations

import argparse
import atexit
import datetime as dt
import hashlib
import json
import logging
import math
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

try:
    from hostdag_grok_watch import (
        apply_run_tickets,
        event_kind as grok_event_kind,
        grok_fingerprint,
        observe_record as observe_grok_record,
        tickets_from_run,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from hostdag_grok_watch import (
        apply_run_tickets,
        event_kind as grok_event_kind,
        grok_fingerprint,
        observe_record as observe_grok_record,
        tickets_from_run,
    )
from hostdag_json import read_json
import hostdag_dag_view as dagview  # noqa: E402
import hostdag_ticket_state as ts  # noqa: E402  (path set up by the import above)
import hostdag_watch_registration as registrations  # noqa: E402
import hostdag_dispatch_guard as dispatch_guard  # noqa: E402
import hostdag_resource_locks as resource_locks  # noqa: E402
from hostdag_self_heal import (  # noqa: E402  (path set up by the import above)
    alert,
    is_transient,
    retry_due,
    retry_message_file,
    server_identity,
    stall,
    state_alerts,
    turn_activity,
)
from hostdag_cli_watch import (  # noqa: E402  (path set up by the import above)
    CLI_PROVIDERS,
    unit_active as cli_unit_active,
    cli_fingerprint,
    event_kind as cli_event_kind,
    observe_record as observe_cli_record,
)


DEFAULT_POLL_INTERVAL = 30.0
DEFAULT_DIRECTION_INTERVAL = 3600.0
DEFAULT_HOST_TIMEOUT = 30.0
# Failed polling cycles, not individual calls (one cycle can recover a send). A `watch` process counts
# only its own cycles: reset_host_timeout_streak zeroes the persisted value when the process starts.
HOST_TIMEOUT_FAILURE_LIMIT = 3
# Terminal host-backed records (2026-11-04: 130 delivered terminal threads were read every cycle; one
# slow app-server read per cycle tripped the limit above). A record that just turned terminal is still
# read for a grace window -- a coordinator may send its thread a new turn -- but only as an *optional*
# read. Once past grace with its event delivered it is skipped; one such record per poll is re-read at
# most every DEFAULT_TERMINAL_RECHECK_SECONDS to notice a late reused thread. Optional-read failures go
# to last_host_error and are never counted; an optional timeout backs all optional reads off.
DEFAULT_TERMINAL_GRACE_SECONDS = 1800.0
DEFAULT_TERMINAL_RECHECK_SECONDS = 1800.0
TERMINAL_RECHECK_BACKOFF_SECONDS = 900.0
DEFAULT_EVENT_LIMIT = 8
DEFAULT_SETTLE_SECONDS = 0.0
DEFAULT_MAX_HOLD_SECONDS = 600.0
DEFAULT_SIBLING_HOLD_SECONDS = 1800.0
REVIEW_MARK = "-review-"
MAX_PACKET_CHARS = 4000
SUMMARY_CHARS = 240
MESSAGE_PAGE_LIMIT = 25
MAX_MESSAGE_PAGES = 20
KNOWN_RUNNING = {"running"}
KNOWN_COMPLETED = {"completed"}
KNOWN_FAILED = {"failed", "cancelled"}
KNOWN_ATTENTION = {"error", "attention"}
MARKER_PREFIX = "HOST-DAG-WATCH-MARKER:"
CLOSED_TICKET_STATES = {"ACCEPTED", "INTEGRATED", "SUPERSEDED", "CLOSED", "DONE", "CANCELLED", "CANCELED"}
SEND_REPLAY_STATES = {"intent", "uncertain"}
# Failures and ambiguous states need a decision now; routine outcomes may settle.
URGENT_KINDS = {"failed", "attention", "unknown_status", "rejected", "unanswered", "cancelled"}
# Attention notices that report routine progress, not a decision (2026-10-20: they were 40% of wakes).
ROUTINE_ATTENTION = {"idle", "window-started"}


def urgent(event: dict[str, Any], state: dict[str, Any]) -> bool:
    """Routine attention notices count as non-urgent only when min_wake_gap_seconds is set; without it
    every attention event stays urgent, exactly as before."""
    routine = bool(state.get("min_wake_gap_seconds")) and event.get("kind") == "attention" \
        and event.get("status") in ROUTINE_ATTENTION
    return event.get("kind") in URGENT_KINDS and not routine


class BusyError(Exception):
    pass


class HostError(Exception):
    def __init__(self, code: str, message: str, returncode: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.returncode = returncode


class HostCalls(list[list[str]]):
    """Per-poll facts; later host errors must not erase an earlier timeout."""

    def __init__(self) -> None:
        super().__init__()
        self.timeouts: list[dict[str, Any]] = []
        self.successes = 0
        # Optional reads (terminal records): recorded in last_host_error, never counted.
        self.optional_errors: list[dict[str, Any]] = []


def emit(payload: Any, code: int = 0) -> int:
    raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    out = getattr(sys.stdout, "buffer", None)
    if out is not None:
        out.write(raw)
        out.flush()
    else:
        sys.stdout.write(raw.decode("utf-8"))
        sys.stdout.flush()
    return code


def redact(text: str) -> str:
    token = os.environ.get("CODEXHOST_RUNTIME_TOKEN")
    if token:
        text = text.replace(token, "<redacted>")
    return text


def now_value(args: argparse.Namespace) -> float:
    if args.now is not None:
        return float(args.now)
    return time.time()


def lock_path(state_path: Path) -> Path:
    return state_path.with_name(state_path.name + ".lock")


def _pid_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes
        handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, int(pid))
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


class ExclusiveLock:
    """One writer. fcntl/msvcrt when the filesystem supports it; directory lock otherwise."""

    def __init__(self, state_path: Path) -> None:
        self.path = lock_path(state_path)
        self.dir_path = Path(str(state_path) + ".lockd")
        self.fd = -1
        self.mode = ""

    def __enter__(self) -> ExclusiveLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if os.name != "nt":
            self._fcntl()
            return self
        try:
            self._msvcrt()
        except OSError as exc:
            if self.fd >= 0:
                os.close(self.fd)
                self.fd = -1
            if exc.errno == 22:
                self._dirlock()
            else:
                raise BusyError("state file has another writer") from exc
        return self

    def _fcntl(self) -> None:
        import fcntl
        self.fd = os.open(str(self.path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(self.fd)
            self.fd = -1
            raise BusyError("state file has another writer") from exc
        self.mode = "fcntl"

    def _msvcrt(self) -> None:
        import msvcrt
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        self.fd = os.open(str(self.path), flags, 0o644)
        if os.fstat(self.fd).st_size == 0:
            os.write(self.fd, b"\0")
        os.lseek(self.fd, 0, os.SEEK_SET)
        msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
        self.mode = "msvcrt"

    def _dirlock(self) -> None:
        try:
            self.dir_path.mkdir()
        except FileExistsError:
            pid_file = self.dir_path / "pid"
            try:
                pid = int(pid_file.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                raise BusyError("state file has another writer")
            if _pid_alive(pid):
                raise BusyError("state file has another writer")
            try:
                pid_file.unlink()
            except OSError:
                pass
            try:
                self.dir_path.rmdir()
            except OSError as exc:
                raise BusyError("state file has another writer") from exc
            try:
                self.dir_path.mkdir()
            except FileExistsError as exc:
                raise BusyError("state file has another writer") from exc
        (self.dir_path / "pid").write_text(str(os.getpid()), encoding="utf-8")
        self.mode = "dir"
        atexit.register(self._release_dir)

    def _release_dir(self) -> None:
        if self.mode != "dir":
            return
        try:
            (self.dir_path / "pid").unlink()
        except OSError:
            pass
        try:
            self.dir_path.rmdir()
        except OSError:
            pass
        self.mode = ""

    def __exit__(self, *exc: object) -> None:
        if self.mode == "dir":
            self._release_dir()
            return
        if self.fd < 0:
            return
        if self.mode == "msvcrt":
            import msvcrt
            try:
                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
        os.close(self.fd)
        self.fd = -1
        self.mode = ""


def require_abs(path: str, name: str) -> str:
    if not isinstance(path, str) or not path or not os.path.isabs(path):
        raise ValueError(f"{name} must be an absolute path")
    return path


def require_ticket(ticket: str) -> str:
    if not ticket or ticket in {".", ".."} or "/" in ticket or "\\" in ticket:
        raise ValueError("ticket must be a non-empty name without path separators")
    return ticket


def load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"state file not found: {path}")
    data = read_json(path)
    if not isinstance(data, dict):
        raise ValueError("state file must contain an object")
    return data


def save_state(path: Path, state: dict[str, Any]) -> None:
    registrations.atomic(path, state)


def host_cmd(host_cli: str, *args: str) -> list[str]:
    if host_cli.lower().endswith(".py"):
        return [sys.executable, host_cli, *args]
    return [host_cli, *args]


def decode_host_utf8(data: bytes | str | None) -> str:
    if data is None:
        return ""
    if isinstance(data, str):
        return data
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise HostError("HOST_ERROR", "host cli output is not utf-8 json", -1) from exc


def run_host(state: dict[str, Any], args: list[str], called: list[list[str]]) -> dict[str, Any]:
    cli = state["host_cli"]
    # Claude host workers can coexist with a native Codex parent. Their read
    # contract is the same, but the native Codex CLI cannot resolve their IDs.
    if args[:1] == ["thread"] and len(args) > 2 and registrations.claude_thread(args[2]):
        cli = str(Path(__file__).resolve().with_name("host-dag-claude-host.py"))
    cwd = state["host_cwd"]
    timeout = float(state["host_timeout"])
    argv = host_cmd(cli, *args)
    called.append(list(args))
    if not os.path.isdir(cwd):
        raise HostError("RUNTIME_UNREACHABLE", f"host cwd missing: {cwd}", 127)
    started = time.monotonic()

    def failure(code: str, message: str, returncode: int) -> HostError:
        # Record at the call boundary, even when send recovery catches TIMEOUT.
        if code == "TIMEOUT":
            error = {
                "code": code, "message": message, "returncode": returncode,
                "at": time.time(), "call": [cli, *args],
                "elapsed_seconds": time.monotonic() - started,
            }
            state["last_host_error"] = error
            if isinstance(called, HostCalls):
                called.timeouts.append(error)
        return HostError(code, message, returncode)

    try:
        proc = subprocess.run(
            argv, cwd=cwd, capture_output=True, timeout=timeout, shell=False, check=False,
        )
    except FileNotFoundError as exc:
        raise HostError("RUNTIME_UNREACHABLE", f"host cli not found: {cli}", 127) from exc
    except subprocess.TimeoutExpired as exc:
        raise failure("TIMEOUT", "host cli timed out", -1) from exc
    stdout = redact(decode_host_utf8(proc.stdout))
    stderr = redact(decode_host_utf8(proc.stderr))
    body: Any = None
    if stdout.strip():
        try:
            body = json.loads(stdout)
        except json.JSONDecodeError:
            body = None
    if isinstance(body, dict) and isinstance(body.get("error"), dict):
        err = body["error"]
        raise failure(
            redact(str(err.get("code") or "HOST_ERROR")),
            redact(str(err.get("message") or "host error")),
            proc.returncode,
        )
    if proc.returncode != 0:
        raise HostError("HOST_ERROR", stderr or stdout or "host cli failed", proc.returncode)
    if not isinstance(body, dict):
        raise HostError("HOST_ERROR", "host cli returned non-json", proc.returncode)
    if isinstance(called, HostCalls):
        called.successes += 1
    return body


def summarize(text: object) -> str:
    raw = " ".join(str(text or "").split())
    if len(raw) <= SUMMARY_CHARS:
        return raw
    return raw[: SUMMARY_CHARS - 3] + "..."


def fingerprint(payload: dict[str, Any]) -> str:
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    # Native Codex supplies a durable turn identity in result. Legacy Host's
    # top-level turn can be backfilled; keep its existing content deduplication.
    identity = {"turnId": result["turnId"]} if result.get("turnId") else {}
    blob = json.dumps(
        {
            **identity,
            "status": payload.get("status"),
            "availability": result.get("availability"),
            "text": result.get("text"),
        },
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def queue_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    state.setdefault("pending_events", []).append(event)


def make_event(
    ticket: str, rec: dict[str, Any], kind: str, status: object, summary: str, extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "kind": kind,
        "ticket": ticket,
        "status": status,
        "summary": summary,
        "evidence": rec.get("evidence"),
    }
    if rec.get("provider") == "grok":
        event["provider"] = "grok"
        event["session"] = rec.get("session")
    elif rec.get("provider") in CLI_PROVIDERS:
        event["provider"] = rec.get("provider")
        event["run_file"] = rec.get("run_file")
    else:
        event["thread"] = rec.get("thread")
    if extra:
        event.update(extra)
    return event


def format_message(marker: str, events: list[dict[str, Any]]) -> str:
    lines = [
        marker,
        "Host watcher notification. Verify against the original DAG/run receipt for this parent thread.",
        "Do not create a new Goal or worker queue.",
        "Events:",
    ]
    for event in events:
        lines.append(f"- {event.get('ticket')}: {event.get('kind')} {event.get('summary') or ''}".rstrip())
        if event.get("turnId"):
            lines.append(f"  thread={event.get('thread')} turn={event['turnId']} (this turn only)")
        if event.get("evidence"):
            lines.append(f"  evidence: {event['evidence']}")
    return "\n".join(lines)


def fit_packet(marker: str, events: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    packet = [dict(item) for item in events[:limit]]
    while packet and len(format_message(marker, packet)) > MAX_PACKET_CHARS:
        if len(packet) > 1:
            packet.pop()
            continue
        event = dict(packet[0])
        if event.get("evidence"):
            event["evidence"] = ""
            packet[0] = event
            continue
        summary = str(event.get("summary") or "")
        if len(summary) > 8:
            event["summary"] = summary[: max(8, len(summary) // 2)]
            packet[0] = event
            continue
        return []
    return packet


def coalesce_pending(state: dict[str, Any]) -> None:
    """Keep one wake-worthy event per fact so a busy parent is not woken by backlog."""
    send = state.get("pending_send")
    if isinstance(send, dict) and send.get("status") in SEND_REPLAY_STATES:
        return  # recover_send trims by count; the unsent packet must stay byte-stable
    events = list(state.get("pending_events") or [])
    substantive = {e.get("ticket") for e in events if e.get("kind") != "direction"}
    seen: set[Any] = set()
    kept: list[dict[str, Any]] = []
    for event in reversed(events):
        if event.get("kind") == "direction":
            # A newer outcome already brings the coordinator to this ticket.
            key: Any = ("direction", event.get("ticket"))
            if event.get("ticket") in substantive:
                continue
        else:
            key = (event.get("ticket"), event.get("thread"), event.get("turnId"),
                   event.get("kind"), event.get("status"), event.get("summary"))
        if key in seen:
            continue
        seen.add(key)
        kept.append(event)
    state["pending_events"] = list(reversed(kept))


def pause_parent_goal(state: dict[str, Any], now: float, called: list[list[str]]) -> None:
    """Event-driven runs keep the parent Goal paused so Codex never self-continues while idle."""
    if not state.get("pause_parent_goal"):
        return
    try:
        body = run_host(state, ["goal", "pause", state["parent_thread"]], called)
    except HostError as exc:
        # Optimization only: event delivery must not depend on goal support.
        prev = state.get("parent_goal") or {}
        attempts = int(prev.get("attempts") or 0) + 1 if prev.get("error") else 1
        state["parent_goal"] = {"at": now, "error": exc.code, "message": exc.message, "attempts": attempts}
        return
    goal = body.get("goal") if isinstance(body.get("goal"), dict) else {}
    state["parent_goal"] = {"at": now, "status": goal.get("status"), "changed": bool(body.get("changed"))}


def parent_goal_pause_due(state: dict[str, Any], now: float) -> bool:
    """Idle polls pause once, then retry failures with capped backoff (60s doubling to 1h)."""
    last = state.get("parent_goal")
    if not last:
        return True
    if not last.get("error"):
        return False
    backoff = min(60 * 2 ** (int(last.get("attempts") or 1) - 1), 3600)
    return now >= float(last.get("at") or 0) + backoff


def _direction_if_running(state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float) -> None:
    rec["terminal"] = False
    interval = float(state.get("direction_interval") or 0)
    anchor = rec.get("direction_anchor")
    if anchor is None:
        rec["direction_anchor"] = now
        anchor = now
    last = rec.get("last_direction_at")
    start = last if last is not None else anchor
    if now >= float(start) + interval:
        rec["last_direction_at"] = now
        queue_event(state, make_event(
            ticket, rec, "direction", rec.get("last_status") or "running",
            "active task reached direction-check interval",
        ))


def observe_grok_ticket(state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float) -> None:
    snapshot = observe_grok_record(rec, now=now)
    rec["last_status"] = snapshot.get("status")
    if snapshot.get("sessionId"):
        rec["session"] = snapshot.get("sessionId")
    b5_note(ticket, rec, now, snapshot.get("status"))
    kind = grok_event_kind(str(snapshot.get("status") or ""))
    if kind is None:
        _direction_if_running(state, ticket, rec, now)
        return
    rec["terminal"] = kind in {"completed", "failed", "cancelled", "rejected", "unanswered"}
    fp = grok_fingerprint(snapshot)
    if rec.get("last_result_fingerprint") != fp:
        rec["last_result_fingerprint"] = fp
        queue_event(state, make_event(
            ticket, rec, kind, snapshot.get("status"), snapshot.get("summary") or "",
        ))


def observe_cli_ticket(state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float) -> None:
    snapshot = observe_cli_record(rec)
    if rec.get('provider') == 'reasonix' and snapshot.get('credentialFault'):
        from hostdag_reasonix_faults import notify
        notify(state, ticket, snapshot['credentialFault'], now)
    rec["last_status"] = snapshot.get("status")
    b5_note(ticket, rec, now, snapshot.get("status"))
    kind = cli_event_kind(str(snapshot.get("status") or ""))
    if kind is None:
        _direction_if_running(state, ticket, rec, now)
        return
    rec["terminal"] = True
    fp = cli_fingerprint(snapshot)
    if rec.get("last_result_fingerprint") != fp:
        rec["last_result_fingerprint"] = fp
        queue_event(state, make_event(
            ticket, rec, kind, snapshot.get("status"), snapshot.get("summary") or "",
        ))


def observe_ticket(
    state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float, called: list[list[str]],
) -> None:
    if rec.get("provider") == "grok":
        observe_grok_ticket(state, ticket, rec, now)
        return
    if rec.get("provider") in CLI_PROVIDERS:
        observe_cli_ticket(state, ticket, rec, now)
        return
    try:
        payload = run_host(state, ["thread", "read", rec["thread"]], called)
    except HostError as exc:
        if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"}:
            raise
        rec["last_status"] = "error"
        rec["terminal"] = False
        b5_note(ticket, rec, now, "error", error=exc.code)
        fp = hashlib.sha256(exc.code.encode("utf-8")).hexdigest()
        if rec.get("last_result_fingerprint") != fp:
            rec["last_result_fingerprint"] = fp
            queue_event(state, make_event(
                ticket, rec, "unknown_status", exc.code,
                f"{exc.code}: {summarize(exc.message)}; coordinator must judge",
            ))
        return
    status = payload.get("status")
    rec["last_status"] = status
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    server = state.get("server_identity")
    if status in KNOWN_RUNNING and server:
        rec["server_seen"], rec["server_seen_turn"] = server, result.get("turnId")
    fp = fingerprint(payload)
    body = summarize(result.get("text"))
    extra = {"availability": result.get("availability")}
    if result.get("turnId"):
        extra["turnId"] = result["turnId"]

    def b5_observed(healed: bool = False) -> None:
        top = payload.get("turn") if isinstance(payload.get("turn"), dict) else {}
        b5_note(ticket, rec, now, status, turn=result.get("turnId") or top.get("turnId"),
                text=result.get("text"), healed=healed, cwd=payload.get("cwd"))
    # Capacity failures are infrastructure failures, but retry backoff must not hide them
    # from the coordinator. Native Codex exposes structured errors separately from text.
    turn = payload.get("turn") if isinstance(payload.get("turn"), dict) else {}
    errors = [payload.get("nativeError"), payload.get("error"), turn.get("error"), result.get("error"),
              payload.get("codexErrorInfo"), turn.get("codexErrorInfo"), result.get("codexErrorInfo")]
    error_text = json.dumps(errors, ensure_ascii=False, sort_keys=True)
    capacity_text = (error_text + " " + str(result.get("text") or "")).lower()
    capacity = any(token in capacity_text for token in
                   ("serveroverloaded", "server_overloaded", "server overloaded", "selected model is at capacity"))
    startup_failed = status in {"failed", "error"} and not result.get("turnId") \
        and result.get("availability") == "unavailable"
    if capacity or startup_failed:
        b5_observed()
        rec["terminal"] = False
        error_fp = hashlib.sha256((fp + error_text).encode("utf-8")).hexdigest()
        if rec.get("last_result_fingerprint") != error_fp:
            rec["last_result_fingerprint"] = error_fp
            queue_event(state, make_event(ticket, rec, "attention", status,
                        summarize(body or error_text) if any(errors) or body else f"executor startup {status}", extra))
        return
    if status in KNOWN_COMPLETED:
        b5_observed()
        rec["terminal"] = True
        if rec.get("last_result_fingerprint") != fp:
            rec["last_result_fingerprint"] = fp
            queue_event(state, make_event(ticket, rec, "completed", status, body, extra))
        return
    restarted = (bool(server) and rec.get("server_seen") not in (None, server)
                 and result.get("turnId") == rec.get("server_seen_turn"))
    if status in KNOWN_FAILED and heal_failed_worker(state, ticket, rec, status, result, now, called, restarted):
        b5_observed(healed=True)
        return
    b5_observed()
    if status in KNOWN_FAILED:
        rec["terminal"] = True
        if rec.get("last_result_fingerprint") != fp:
            rec["last_result_fingerprint"] = fp
            queue_event(state, make_event(ticket, rec, "failed", status, body or f"session {status}", extra))
        return
    if status in KNOWN_ATTENTION:
        rec["terminal"] = False
        if rec.get("last_result_fingerprint") != fp:
            rec["last_result_fingerprint"] = fp
            queue_event(state, make_event(ticket, rec, "attention", status, body or f"session {status}", extra))
        return
    if status not in KNOWN_RUNNING:
        rec["terminal"] = False
        if rec.get("last_result_fingerprint") != fp:
            rec["last_result_fingerprint"] = fp
            note = body or f"unknown session status {status!r}"
            queue_event(state, make_event(
                ticket, rec, "unknown_status", status,
                f"{note}; coordinator must judge", extra,
            ))
        return
    if heal_running_worker(state, ticket, rec, payload, now, called):
        return
    _direction_if_running(state, ticket, rec, now)


def _minutes(activity: dict[str, Any] | None, now: float) -> int:
    return int((now - float(activity["mtime"])) / 60) if activity else 0


def heal_running_worker(
    state: dict[str, Any], ticket: str, rec: dict[str, Any], payload: dict[str, Any], now: float,
    called: list[list[str]],
) -> bool:
    """Interrupt a worker turn stuck on a model request; the next poll retries it as transient."""
    activity = turn_activity(payload.get("path"))
    kind = stall(activity, now)
    turn = (payload.get("result") or {}).get("turnId")
    if kind == "tool":
        alert(state, f"toolstall:{ticket}:{turn}", f"{ticket} 工具调用 {_minutes(activity, now)} 分钟无输出",
              "执行会话在等一个工具调用（可能是长构建）；watcher 不会中断它，请确认是否正常。", now)
        return False
    if kind != "model" or not turn:
        return False
    try:
        run_host(state, ["thread", "cancel", rec["thread"], "--turn-id", str(turn)], called)
    except HostError as exc:
        if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"}:
            raise
        alert(state, f"cancelfail:{ticket}", f"{ticket} 卡住且中断失败",
              f"执行会话等待模型 {_minutes(activity, now)} 分钟无进展，中断失败：{exc.code}", now)
        return False
    rec["watch_interrupted_turn"] = turn
    rec["last_status"] = "interrupted_by_watch"
    rec["terminal"] = False
    alert(state, f"interrupt:{ticket}:{turn}", f"{ticket} 卡住已中断",
          f"执行会话等待模型 {_minutes(activity, now)} 分钟无进展，watcher 已中断并将自动续跑。", now, desktop=False)
    return True


def heal_failed_worker(
    state: dict[str, Any], ticket: str, rec: dict[str, Any], status: str, result: dict[str, Any], now: float,
    called: list[list[str]], restarted: bool = False,
) -> bool:
    """Resume a worker whose turn died of an infrastructure blip; True when the failure is handled here."""
    turn = result.get("turnId")
    reason = str(result.get("text") or status)
    ours = bool(turn) and rec.get("watch_interrupted_turn") == turn
    if restarted:
        reason = (f"Codex 后台服务重启（{rec.get('server_seen')} → {state.get('server_identity')}），"
                  f"运行中的回合被中断，不是用户取消")
    elif not ours and not (status == "failed" and is_transient(reason)):
        return False
    if rec.get("last_failure_turn") != turn:
        rec["last_failure_turn"] = turn
        rec["last_failure_at"] = now
        if state.get("run_receipt"):
            try:
                rec["retry_dispatch_context"] = dispatch_guard.capture(
                    state["run_receipt"], ticket, rec["thread"], turn)
            except dispatch_guard.DispatchError as exc:
                rec["retry_dispatch_context"] = {"error": exc.message}
    due = retry_due(rec, now)
    if due is None:
        alert(state, f"retries:{ticket}", f"{ticket} 自动续跑已用完",
              f"连续基础设施失败，已交父协调者处理：{reason[:200]}", now)
        return False
    if not due:
        rec["terminal"] = False
        rec["last_status"] = "retry_backoff"
        return True
    context = rec.get("retry_dispatch_context")
    if state.get("run_receipt"):
        try:
            dispatch_guard.check(context or {}, rec["thread"])
        except dispatch_guard.DispatchError as exc:
            rec["retry_dispatch_error"] = {"code": exc.code, "message": exc.message}
            alert(state, f"retryguard:{ticket}:{turn}", f"{ticket} 自动续跑待重新确认",
                  f"{exc.code}: {exc.message}", now, desktop=False)
            return False
    n = int(rec.get("auto_retries") or 0) + 1
    message = retry_message_file(state, ticket, "watcher 中断了卡住的一轮" if ours and not restarted else reason, n)
    args = ["thread", "send", rec["thread"], "--message-file", message]
    if context and not registrations.claude_thread(rec["thread"]):
        context_path = Path(message).with_suffix(".dispatch.json")
        registrations.atomic(context_path, context)
        args.extend(["--dispatch-context", str(context_path)])
    try:
        run_host(state, args, called)
    except HostError as exc:
        if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"}:
            raise
        alert(state, f"retrysend:{ticket}", f"{ticket} 自动续跑发送失败", f"{exc.code}: {exc.message[:200]}", now)
        return False
    rec["auto_retries"] = n
    rec.pop("retry_dispatch_error", None)
    rec["terminal"] = False
    rec["last_status"] = "auto_retried"
    rec.pop("watch_interrupted_turn", None)
    if restarted:
        rec["server_seen"] = state.get("server_identity")
        queue_event(state, make_event(ticket, rec, "server_restart", status, reason + "；watcher 已按原合同自动续跑。"))
    alert(state, f"retry:{ticket}:{n}", f"{ticket} 已自动续跑（第 {n} 次）", reason[:300], now, desktop=False)
    return True


def heal_stuck_parent(
    state: dict[str, Any], parent: dict[str, Any], now: float, called: list[list[str]],
) -> str | None:
    """Interrupt a coordinator turn stuck on a model request; return its new status, or None to keep holding."""
    activity = turn_activity(parent.get("path"))
    kind = stall(activity, now)
    turn = (parent.get("result") or {}).get("turnId")
    minutes = _minutes(activity, now)
    if kind == "tool":
        alert(state, f"parenttool:{turn}", f"父协调者工具调用 {minutes} 分钟无输出",
              "积压通知仍在等待；watcher 不中断工具调用，请确认父协调者是否正常。", now)
        return None
    if kind != "model" or not turn:
        return None
    try:
        run_host(state, ["thread", "cancel", state["parent_thread"], "--turn-id", str(turn)], called)
        after = run_host(state, ["thread", "read", state["parent_thread"]], called)
    except HostError as exc:
        if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"}:
            raise
        alert(state, f"parentcancel:{turn}", "父协调者卡住且中断失败，需要人工处理",
              f"父协调者等待模型 {minutes} 分钟无进展，中断失败：{exc.code}", now)
        return None
    queue_event(state, {
        "kind": "watchdog", "ticket": "WATCHER", "status": "interrupted", "queued_at": now,
        "summary": f"父协调者上一轮等待模型响应 {minutes} 分钟无进展，watcher 已中断该轮（turn {turn}）。"
                   "请核对下列积压事件和上一轮未完成的动作后继续。",
    })
    alert(state, f"parentheal:{turn}", "父协调者卡住已自动中断",
          f"等待模型 {minutes} 分钟无进展，已中断并投递积压通知。", now)
    return after.get("status")


def marker_in_parent_messages(state: dict[str, Any], marker: str, called: list[list[str]]) -> bool:
    cursor: str | None = None
    seen: set[str] = set()
    for _ in range(MAX_MESSAGE_PAGES):
        args = [
            "thread", "read", state["parent_thread"],
            "--view", "messages", "--limit", str(MESSAGE_PAGE_LIMIT),
        ]
        if cursor:
            args.extend(["--cursor", cursor])
        body = run_host(state, args, called)
        if marker in json.dumps(body, ensure_ascii=False):
            return True
        nxt = body.get("nextCursor")
        if not nxt or not isinstance(nxt, str) or nxt in seen:
            return False
        seen.add(nxt)
        cursor = nxt
    return False


def recover_send(state: dict[str, Any], called: list[list[str]]) -> dict[str, Any]:
    send = state.get("pending_send") or {}
    marker = str(send.get("marker") or "")
    try:
        found = bool(marker) and marker_in_parent_messages(state, marker, called)
    except HostError as exc:
        if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"}:
            raise
        found = False
    verdict = None if found else marker_in_parent_rollout(state, marker, send, called)
    if found or verdict == "present":
        n = int(send.get("event_n") or 0)
        send["status"] = "delivered"
        state["pending_send"] = send
        if state.get("min_wake_gap_seconds"):  # a confirmed replay is a delivery too: the gap restarts now
            state["last_delivered_at"] = time.time()
        pending = list(state.get("pending_events") or [])
        state["pending_events"] = pending[n:] if n > 0 else pending
        return {"send": "delivered", "recovered": True}
    if verdict == "absent":
        # The parent's own rollout proves the packet never arrived; the events are still queued.
        state["pending_send"] = None
        return {"send": "redeliver", "recovered": True}
    send["status"] = "uncertain"
    state["pending_send"] = send
    # Keyed by parent, not marker: a redelivery loop mints a new marker every round.
    alert(state, f"uncertain:{state.get('parent_thread')}", "watcher 无法确认通知是否送达，已暂停投递",
          f"父协调者 {state.get('parent_thread')} 的通知 {marker} 状态不明；请检查后清除 pending_send。",
          time.time())
    return {"send": "uncertain", "recovered": False}


ROLLOUT_ABSENT_GRACE_SECONDS = 120


def other_writer(exc: HostError) -> bool:
    """Another client (e.g. the Codex desktop App) holds the parent thread open; nothing was submitted."""
    return "already has an active writer" in exc.message


def marker_in_parent_rollout(
    state: dict[str, Any], marker: str, send: dict[str, Any], called: list[list[str]],
) -> str | None:
    """'present'/'absent' from the parent's local rollout file, or None when it cannot decide.

    Reading messages through the host can fail on very long threads; the rollout file on disk
    is the parent's own record. Absent only counts once the parent is idle and the send is
    old enough that a turn would already have been written.
    """
    if not marker:
        return None
    try:
        parent = run_host(state, ["thread", "read", state["parent_thread"]], called)
    except HostError:
        return None
    path = parent.get("path")
    if not path:
        return None
    try:
        with open(path, "rb") as handle:
            if marker.encode() in handle.read():
                return "present"
    except OSError:
        return None
    saved = float(send.get("saved_at") or 0)
    if parent.get("status") in {"completed", "cancelled", "failed"} and time.time() - saved >= ROLLOUT_ABSENT_GRACE_SECONDS:
        return "absent"
    return None


def _send_packet(state: dict[str, Any], now: float, called: list[list[str]], parent_status: str) -> dict[str, Any]:
    send = state.get("pending_send") if isinstance(state.get("pending_send"), dict) else {}
    marker = str(send.get("marker") or "") if send.get("status") == "busy" else MARKER_PREFIX + str(uuid.uuid4())
    limit = int(state.get("event_limit") or DEFAULT_EVENT_LIMIT)
    packet = fit_packet(marker, list(state.get("pending_events") or []), limit)
    event_n = len(packet)
    if not packet:
        first = (state.get("pending_events") or [{}])[0]
        packet = [{
            "kind": "attention",
            "ticket": first.get("ticket"),
            "thread": first.get("thread"),
            "status": "overflow",
            "summary": "event packet exceeded size cap; coordinator must judge",
            "evidence": "",
        }]
        event_n = 1
    message = format_message(marker, packet)
    if len(message) > MAX_PACKET_CHARS:
        message = (
            f"{marker}\nHost watcher notification. Event packet exceeded size cap; "
            "coordinator must judge.\n"
        )
        event_n = max(event_n, 1)
    state["pending_send"] = {
        "marker": marker,
        "status": "intent",
        "message": message,
        "event_n": event_n,
        "saved_at": now,
    }
    save_state(Path(state["state_path"]), state)
    try:
        run_host(state, ["thread", "send", state["parent_thread"], "--message", message], called)
    except HostError as exc:
        if exc.code == "THREAD_BUSY":
            state["pending_send"]["status"] = "busy"
            return {"send": "thread_busy", "parent_status": parent_status}
        if other_writer(exc):
            state["pending_send"]["status"] = "busy"
            alert(state, f"writer-conflict:{state['parent_thread']}", "父协调者会话被其他客户端占用，通知暂存",
                  f"父协调者 {state['parent_thread']} 正被其他客户端（通常是 Codex 桌面 App）打开，watcher 无法投递。"
                  "请在 App 中关闭该会话或退出 App；占用解除后会自动补发，无需其他操作。",
                  time.time())
            return {"send": "writer_conflict", "parent_status": parent_status}
        if exc.code == "RUNTIME_UNREACHABLE":
            raise
        return recover_send(state, called)
    state["pending_send"] = {"marker": marker, "status": "delivered", "delivered_at": now, "event_n": event_n}
    if state.get("min_wake_gap_seconds"):  # only then: without a gap watch.json stays byte-identical
        state["last_delivered_at"] = now
    state["pending_events"] = (state.get("pending_events") or [])[event_n:]
    leftover = bool(state.get("pending_events"))
    return {"send": "delivered", "parent_status": parent_status, "truncated": leftover}


def review_group(ticket: object) -> str | None:
    """`X-review-standards` and `X-review-spec` are the two axes of X's review."""
    text = str(ticket or "")
    return text.rsplit(REVIEW_MARK, 1)[0] if REVIEW_MARK in text else None


def awaiting_review_axis(state: dict[str, Any], events: list[dict[str, Any]]) -> bool:
    """A finished review axis waits for its still-running sibling instead of waking the parent alone."""
    queued = {e.get("ticket") for e in events}
    groups = {review_group(e.get("ticket")) for e in events if e.get("kind") != "direction"} - {None}
    return any(
        review_group(ticket) in groups and ticket not in queued and not rec.get("terminal")
        for ticket, rec in (state.get("tracked") or {}).items()
    )


def settling(state: dict[str, Any], events: list[dict[str, Any]], now: float) -> bool:
    """Hold review outcomes until the queue is quiet, so paired axes wake the parent once.

    Only review sessions settle: an executor's result goes out on the next poll (2026-10-09, the
    3-minute quiet window delayed a finished ticket with no sibling to wait for).
    """
    if any(urgent(e, state) for e in events):
        return False
    # 2026-10-20 (Codex quota): a non-urgent packet waits until min_wake_gap_seconds have passed since
    # the last delivery, so a busy run wakes the parent at most once per gap; urgent events go at once.
    gap = float(state.get("min_wake_gap_seconds") or 0)
    last = state.get("last_delivered_at")
    if gap > 0 and last is not None and 0 <= now - float(last) < gap:  # a clock moved back holds nothing
        return True
    settle = float(state.get("settle_seconds") or 0)
    if settle <= 0:
        return False
    if not any(review_group(e.get("ticket")) for e in events if e.get("kind") != "direction"):
        return False
    stamps = [float(e["queued_at"]) for e in events if e.get("queued_at") is not None]
    if not stamps:
        return False
    # Review axes routinely finish minutes apart, well past the quiet window.
    sibling_hold = float(state.get("sibling_hold_seconds") or DEFAULT_SIBLING_HOLD_SECONDS)
    if now - min(stamps) < sibling_hold and awaiting_review_axis(state, events):
        return True
    max_hold = float(state.get("max_hold_seconds") or DEFAULT_MAX_HOLD_SECONDS)
    return now - max(stamps) < settle and now - min(stamps) < max_hold


CLAUDE_HOST_STATE = Path.home() / ".local/state/host-dag-claude-host"
LIMIT_TEXT = ("hit your session limit", "usage limit", "rate limit", "hit your limit")
RESET_RE = __import__("re").compile(r"resets\s+(\d{1,2})(?::(\d{2}))?\s*(am|pm)", __import__("re").I)


def _reset_at(text: str, started: float) -> float:
    """Epoch of 'resets 4:10pm' after the turn started (local time); unparsable -> 10 minutes later."""
    m = RESET_RE.search(text or "")
    if not m:
        return started + 600
    hour = int(m.group(1)) % 12 + (12 if m.group(3).lower() == "pm" else 0)
    base = dt.datetime.fromtimestamp(started)
    at = base.replace(hour=hour, minute=int(m.group(2) or 0), second=0, microsecond=0)
    if at.timestamp() < started:
        at += dt.timedelta(days=1)
    return at.timestamp()


def limited_turns(thread: str, since: float) -> list[dict[str, Any]]:
    """Coordinator turns (host-dag-claude-host) after `since` that ended on a usage limit, oldest first."""
    try:
        rec = json.loads((CLAUDE_HOST_STATE / f"{thread}.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    out = []
    for turn in rec.get("turns") or []:
        if float(turn.get("started") or 0) < since:
            continue
        try:
            result = json.loads(Path(turn["out"]).read_text(encoding="utf-8"))
        except (OSError, ValueError, KeyError):
            continue  # still running, or no output
        text = str(result.get("result") or "")
        if result.get("is_error") and (str(result.get("api_error_status")) == "429"
                                       or any(k in text.lower() for k in LIMIT_TEXT)):
            inp = Path(turn["out"]).with_name(Path(turn["out"]).name.replace(".out.json", ".in.txt"))
            out.append({"id": turn["id"], "started": float(turn["started"]), "text": text,
                        "reset_at": _reset_at(text, float(turn["started"])),
                        "input": inp.read_text(encoding="utf-8") if inp.exists() else None})
    return out


def redeliver_limited(state: dict[str, Any], now: float, called: list[list[str]]) -> dict[str, Any] | None:
    """Resend coordinator turns lost to a Claude usage limit, once, after the stated reset.

    Returns {"send": "held_limit"} while the limit is still in force (new packets would be lost too),
    the redelivery result when it resends, or None when there is nothing to do.
    """
    thread = str(state.get("parent_thread") or "")
    if not thread.startswith("claude-"):
        return None
    since = float(state.setdefault("limit_watch_since", now))
    done = set(state.setdefault("redelivered_turns", []))
    lost = [t for t in limited_turns(thread, since) if t["id"] not in done]
    if not lost:
        return None
    reset = max(t["reset_at"] for t in lost) + 60
    if now < reset:
        state["limit_hold_until"] = reset
        return {"send": "held_limit", "until": reset}
    try:
        parent = run_host(state, ["thread", "read", thread], called)
    except HostError:
        return {"send": "held_limit", "until": now + 60}
    if parent.get("status") == "running":
        return {"send": "held_busy", "parent_status": "running"}
    parts = [f"【重发：以下 {len(lost)} 轮因 Claude 额度用尽未被处理（{lost[0]['text'][:80]}），额度已重置，请按原文处理】"]
    for t in lost:
        when = time.strftime("%H:%M", time.localtime(t["started"]))
        parts.append(f"--- 原 {when} 送达 ---\n" + (t["input"] or "（这一轮的原文未保存，请读该时段之后的 alerts.jsonl）"))
    message = "\n\n".join(parts)
    try:
        run_host(state, ["thread", "send", thread, "--message", message], called)
    except HostError as exc:
        if exc.code == "THREAD_BUSY":
            return {"send": "held_busy"}
        raise
    state["redelivered_turns"] = sorted(done | {t["id"] for t in lost})[-200:]
    state.pop("limit_hold_until", None)
    alert(state, f"redelivered:{lost[-1]['id']}", f"已重发 {len(lost)} 轮因额度丢失的协调者消息",
          "；".join(t["id"] for t in lost), now, desktop=False)
    return {"send": "redelivered", "turns": len(lost)}


def deliver_pending(state: dict[str, Any], now: float, called: list[list[str]]) -> dict[str, Any]:
    limited = redeliver_limited(state, now, called)
    if limited is not None:
        return limited  # hold new packets while the limit holds; after a resend, next poll sends them
    send = state.get("pending_send")
    recovered = None
    if isinstance(send, dict) and send.get("status") == "uncertain":
        recovered = recover_send(state, called)  # re-check the rollout each poll; never stall silently
        if recovered.get("send") == "uncertain":
            return recovered
        send = state.get("pending_send")
    if isinstance(send, dict) and send.get("status") == "intent":
        recovered = recover_send(state, called)
        if recovered.get("send") == "uncertain":
            return recovered
        send = state.get("pending_send")
    prune_page_wait_events(state, now)
    pending = state.get("pending_events") or []
    if not pending:
        return recovered or {"send": None}
    if settling(state, pending, now):
        return recovered or {"send": "settling"}
    try:
        parent = run_host(state, ["thread", "read", state["parent_thread"]], called)
    except HostError as exc:
        if exc.code == "RUNTIME_UNREACHABLE":
            raise
        if exc.code == "TIMEOUT":
            if isinstance(send, dict) and send.get("status") == "intent" and send.get("marker"):
                return recover_send(state, called)
            return recovered or {"send": "uncertain"}
        state["last_host_error"] = {"code": exc.code, "message": exc.message, "returncode": exc.returncode}
        return {"send": "unknown_parent"}
    parent_status = parent.get("status")
    if parent_status == "running":
        healed = heal_stuck_parent(state, parent, now, called)
        if healed is None:
            result = {"send": "held_busy", "parent_status": "running"}
            if recovered:
                result["recovered"] = True
            return result
        parent_status = healed
    # A failed or interrupted last turn still accepts a new message (the host's require_sendable).
    if parent_status not in {"completed", "cancelled", "failed"}:
        state["last_host_error"] = {"code": "UNKNOWN_STATUS", "message": f"parent status {parent_status!r}"}
        return {"send": "unknown_parent", "parent_status": parent_status}
    pause_parent_goal(state, now, called)
    sent = _send_packet(state, now, called, parent_status)
    if recovered and sent.get("send") == "delivered":
        sent["recovered"] = True
    elif recovered and sent.get("send") is None:
        return recovered
    return sent


def sync_run_if_needed(state: dict[str, Any], now: float) -> None:
    receipt = state.get("run_receipt")
    if not receipt:
        return
    path = Path(receipt)
    if not path.is_file():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(data, dict):
        return
    apply_run_tickets(state, tickets_from_run(data), now)
    registrations.reconcile(state, data, now)
    gaps = state.get("registration_gaps") or {}
    signature = json.dumps(gaps, sort_keys=True)
    if signature != state.get("registration_gap_notified"):
        state["registration_gap_notified"] = signature
        if gaps:
            queue_event(state, {"kind": "attention", "ticket": "WATCH-REGISTRATION",
                               "status": "registration-gap", "queued_at": now,
                               "summary": "以下活动票缺少可观察身份，请补齐原派工回执：" + ", ".join(gaps),
                               "evidence": str(path)})
    drop_closed_tickets(state, data)


def drop_closed_tickets(state: dict[str, Any], data: dict[str, Any]) -> None:
    """Accepted/superseded tickets need no observation; their late events only re-wake the parent."""
    tickets = data.get("tickets") if isinstance(data.get("tickets"), dict) else {}
    tickets = dict(tickets)
    for body in (data.get("batches") or {}).values():
        tickets.update(body.get("tickets") or {})
    closed = {
        str(name) for name, rec in tickets.items()
        if isinstance(rec, dict) and str(rec.get("state") or rec.get("status") or "").upper() in CLOSED_TICKET_STATES
    }
    tracked = state.setdefault("tracked", {})
    suspended={str(name) for name,rec in tickets.items() if isinstance(rec,dict) and ts.derive_state(rec) in ts.DEGRADE_STATES}
    for name in list(tracked):
        if any(name.startswith(ticket+'-review-') for ticket in suspended):tracked.pop(name,None)
    if not closed:
        return
    for name in list(tracked):
        if name in closed or any(name.startswith(ticket + "-review-") for ticket in closed):
            tracked.pop(name, None)
    send = state.get("pending_send")
    if isinstance(send, dict) and send.get("status") in SEND_REPLAY_STATES:
        return
    state["pending_events"] = [e for e in state.get("pending_events") or []
                               if e.get("ticket") not in closed and not any(
                                   str(e.get("ticket")).startswith(ticket + "-review-") for ticket in closed)]


IDLE_TICKET_SECONDS = 20 * 60
IDLE_RENUDGE_SECONDS = 30 * 60
# A declared wait (WAITING_DEPENDENCY / WAITING_RESOURCE) with no WAIT-UNTIL time is re-checked after this.
WAITING_TIMEOUT_SECONDS = 60 * 60
# Waiting on a person or the outside world, or finished: never nudged.
IDLE_SKIP_STATES = ts.TERMINAL | {ts.NEEDS_USER, ts.EXTERNALLY_BLOCKED}


def _iso_seconds(value: object) -> float | None:
    try:
        return dt.datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def idle_ticket_events(state: dict[str, Any], data: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """Tickets the coordinator forgot: open, dependencies met, and nobody running them.

    User decision 2026-10-14 (KV17-04 sat 20+ minutes with both reviews passed and no
    round dispatched): the watcher nudges the coordinator instead of trusting it to
    remember. REVIEWING (incl. READY_FOR_ACCEPTANCE) is nudged too (someone has to accept it).
    Decided on the ticket state (hostdag_ticket_state): NEEDS_USER, EXTERNALLY_BLOCKED and
    terminal states are skipped; a declared wait is nudged once after its WAIT-UNTIL time,
    or WAITING_TIMEOUT_SECONDS after its last update when it names no time. Merely
    extending the deadline does not re-arm it; actual work or a new candidate does.
    """
    everything = ts.all_tickets(data)
    tracked = state.get("tracked") or {}
    nudged = state.setdefault("idle_nudged", {})
    wait_nudged = state.setdefault("idle_wait_nudged", {})
    events: list[dict[str, Any]] = []
    for batch, body in (data.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        for name, rec in tickets.items():
            if not isinstance(rec, dict):
                continue
            current = ts.derive_state(rec, everything)
            wait_key = f"{batch}/{name}"
            blocker = str(rec.get("blocker") or "")
            declared_wait = current in ts.WAITING or blocker.startswith("WAIT-UNTIL:")
            live = any(key == name or key.startswith(name + "-")
                       for key, watched in tracked.items()
                       if isinstance(watched, dict) and not watched.get("terminal"))
            # Rewording a blocker or rolling its deadline forward is not progress.
            # Re-arm only for a new work identity or after actual work resumes.
            wait_identity = [current, rec.get("round"), rec.get("attempt"),
                             rec.get("candidate"), rec.get("executor")]
            if not declared_wait or live:
                wait_nudged.pop(wait_key, None)
            if current in ts.DEGRADE_STATES:
                execution=rec.get('degrade_execution') or {}
                identity=[current,rec.get('updated_at'),execution.get('status')]
                if state.setdefault('degrade_nudged',{}).get(wait_key)!=identity:
                    state['degrade_nudged'][wait_key]=identity
                    events.append({'kind':'attention','ticket':name,'status':'degrade','queued_at':now,'evidence':rec.get('evidence'),'summary':f'{name} {current}：{blocker}；回执 {execution.get("receipt_path","未登记")}；由协调者处理，不自动派发'})
                continue
            if current in IDLE_SKIP_STATES:
                continue
            deps = rec.get("deps") or []
            # A ticket already dispatched ahead of its dependencies (KV18-10 started in parallel with KV18-07
            # on user authorisation) is the coordinator's to keep moving; it sat 15 hours un-nudged on 10-16.
            started = bool(rec.get("round")) or int(rec.get("attempt") or 0) > 0
            if (not started or ts.explicit_state(rec) == ts.CREATED) and ts.dependency_status(rec, everything)[0]:
                continue
            threshold = IDLE_TICKET_SECONDS
            if blocker.startswith("WAIT-UNTIL:"):
                until = _iso_seconds(blocker.split()[0].removeprefix("WAIT-UNTIL:"))
                if until and now < until:
                    continue
                if until is not None:
                    threshold = 0
            elif current in ts.WAITING:
                threshold = WAITING_TIMEOUT_SECONDS
            if live:
                continue
            if declared_wait and wait_nudged.get(wait_key) == wait_identity:
                continue
            # Idle counts from the later of the ticket's own update and its last dependency's acceptance:
            # an unclaimed ticket's updated_at is its creation, long before it could start (KVT2-R-03
            # was nudged as "145 minutes idle" ten seconds after KVT2-R-02 was accepted).
            stamps = [_iso_seconds(rec.get("updated_at")),
                      *(_iso_seconds((everything.get(d) or {}).get("accepted_at")) for d in deps)]
            since = max((x for x in stamps if x), default=now)
            if now - since < threshold or now - float(nudged.get(name, 0)) < IDLE_RENUDGE_SECONDS:
                continue
            nudged[name] = now
            if declared_wait:
                wait_nudged[wait_key] = wait_identity
            minutes = int((now - since) / 60)
            status = str(rec.get("status") or "").upper()
            events.append({
                "kind": "attention", "ticket": name, "status": "idle", "queued_at": now,
                "summary": (f"{batch}/{name} 已 {minutes} 分钟没有在跑的执行者或审查（状态 {current}"
                            f"{'' if status else '，未派'}，依赖已全部验收）。请派下一轮或验收；如果是有意等待，"
                            f"用 host-dag ticket set --state WAITING_RESOURCE|WAITING_DEPENDENCY --blocker "
                            f"\"WAIT-UNTIL:<ISO时间> <原因>\" 写明，或把 state 设为 NEEDS_USER / EXTERNALLY_BLOCKED。"
                            + (" 本次等待只提醒一次；按真实资源恢复时间处理，不要只滚动延后期限。"
                               if declared_wait else "")),
                "evidence": rec.get("evidence"),
            })
    return events


def review_verdict_events(state: dict[str, Any], data: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """A REVIEWING ticket whose review already wrote <evidence>/review-*/verdict.json after the ticket's
    last update.

    2026-10-27: incremental re-checks sent into the held reviewer session (thread send) are not
    tracked executions, so the watcher never saw them finish; BATCH-CHAIN sat in REVIEWING with
    a PASS verdict written at 14:05 and again at 16:22. Only the ticket's own evidence directory is
    listed (one level), never a tree walk. Each verdict file wakes the coordinator once."""
    seen = state.setdefault("review_verdicts_seen", {})
    events: list[dict[str, Any]] = []
    everything = ts.all_tickets(data)
    for batch, body in (data.get("batches") or {}).items():
        for name, rec in ((body or {}).get("tickets") or {}).items():
            if not isinstance(rec, dict) or ts.derive_state(rec, everything) != ts.REVIEWING:
                continue
            evidence = rec.get("evidence")
            if not evidence:
                continue
            try:
                dirs = [d for d in Path(evidence).iterdir() if d.is_dir() and d.name.startswith("review-")]
            except OSError:
                continue
            updated = _iso_seconds(rec.get("updated_at")) or 0.0
            for d in dirs:
                verdict_path = d / "verdict.json"
                try:
                    mtime = verdict_path.stat().st_mtime
                    verdict = json.loads(verdict_path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    continue
                key = f"{name}:{verdict_path}"
                if mtime <= updated or seen.get(key) == mtime:
                    continue
                seen[key] = mtime
                result = str(verdict.get("verdict") or verdict.get("status") or "?")
                candidate = str(verdict.get("candidate") or "")[:9]
                events.append({
                    "kind": "attention", "ticket": name, "status": "review-verdict", "queued_at": now,
                    "summary": (f"{batch}/{name} 的审查已写出结论 {result}（候选 {candidate or '—'}，"
                                f"{verdict_path}），票仍是 REVIEWING。请按结论验收或派返修。"),
                    "evidence": str(verdict_path),
                })
    return events


def state_drift_events(state: dict[str, Any], data: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """Tickets whose executor the watcher tracks as running while the written state still says waiting.

    2026-10-15: KV17-05 round 7 resumed in its held session but stayed WAITING_RESOURCE for 40+ minutes;
    the board and the idle guard both trusted the stale state. The reverse (RUNNING with nobody running)
    is already caught by idle_ticket_events.
    """
    tracked = state.get("tracked") or {}
    live = {k for k, rec in tracked.items() if isinstance(rec, dict) and not rec.get("terminal")}
    nudged = state.setdefault("drift_nudged", {})
    events: list[dict[str, Any]] = []
    for batch, body in (data.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        for name, rec in tickets.items():
            if not isinstance(rec, dict) or name not in live:
                continue
            current = ts.derive_state(rec, tickets)
            if current not in (ts.CREATED, *ts.WAITING):
                continue
            since = _iso_seconds(rec.get("updated_at")) or now
            if now - since < DRIFT_GRACE_SECONDS or now - float(nudged.get(name, 0)) < IDLE_RENUDGE_SECONDS:
                continue
            nudged[name] = now
            events.append({
                "kind": "attention", "ticket": name, "status": "state-drift", "queued_at": now,
                "summary": (f"{batch}/{name} 的执行者仍在跑，但 state 写的是 {current}"
                            f"（{int((now - since) / 60)} 分钟未更新）。请写 host-dag ticket set --batch {batch} "
                            f"--ticket {name} --state RUNNING（attempt 不变）；如果执行者其实已经停笔，请按实际情况更新 state。"),
                "evidence": rec.get("evidence"),
            })
    return events


DRIFT_GRACE_SECONDS = 10 * 60
RESOLVED_WAIT_GRACE_SECONDS = 5 * 60


def _advisor_verdicts(run_dir: Path) -> dict[str, float]:
    """advisor question key -> time of its latest decided/answered entry."""
    out: dict[str, float] = {}
    try:
        lines = (run_dir / "advisor/log.jsonl").read_text(encoding="utf-8").splitlines()
    except OSError:
        return out
    for line_number, raw in enumerate(lines, 1):
        try:
            rec = json.loads(raw)
        except ValueError:
            continue
        if not isinstance(rec, dict):
            continue
        if dagview.is_provisional_answer(rec):
            # A new pending review makes an older verdict for this key inapplicable.
            out.pop(str(rec.get("key") or ""), None)
            continue
        if dagview.is_final_answer(rec) and rec.get("key"):
            # Advisor CLI entries use epoch seconds; user decisions may use ISO dates.
            # One malformed timestamp must not stop notifications for the whole run.
            try:
                try:
                    stamp = float(rec.get("at"))
                except (TypeError, ValueError):
                    stamp = _iso_seconds(rec.get("at"))
                if stamp is None or not math.isfinite(stamp) or isinstance(rec.get("at"), bool):
                    raise ValueError("invalid timestamp")
                # Validate the same platform conversion used by the wait notice.
                time.strftime('%H:%M', time.localtime(stamp))
            except (TypeError, ValueError, OverflowError, OSError):
                logging.getLogger(__name__).warning(
                    "%s:%d: invalid advisor timestamp; entry ignored",
                    run_dir / "advisor/log.jsonl", line_number,
                )
                continue
            out[str(rec["key"])] = stamp
    return out


STALE_WAIT_TOKEN = __import__("re").compile(r"WAIT-(?:RESOURCE|DEPENDENCY)\b:?")


def state_after_release(t: dict[str, Any]) -> str | None:
    """The legal state once the program clears what a ticket waited on (core-mirror: it used to
    drop ``state``, and ticket-A / ticket-B lost theirs). A waiting state moves on: a claimed or
    already dispatched ticket is RUNNING (between rounds, the coordinator dispatches next), an
    undispatched one CREATED (still shown as WAITING_DEPENDENCY while other prerequisites are open). Any
    other written state is kept; None: the ticket had no written state (derived as before)."""
    written = ts.explicit_state(t)
    if written not in ts.WAITING:
        return written
    return ts.RUNNING if t.get("status") == "CLAIMED" or ts.attempt_of(t) > 0 else ts.CREATED


def _release_state(t: dict[str, Any]) -> None:
    """Replace the waiting state with state_after_release() and its legacy fields; never leave None."""
    nxt = state_after_release(t)
    if nxt is not None and nxt != t.get("state"):
        dagview.apply_state(t, nxt)


def release_wait(run_dir: Path, batch: str, name: str, observed: dict[str, Any], kind: str, detail: str,
                 verdicts: dict[str, float], now: float) -> dict[str, Any] | None:
    """Stage A #1/#5: release a wait whose object is already there, under the run lock.

    Conditional: applied only if the ticket still has the observed wait_on and updated_at (the
    coordinator may have changed it since). The released wait is kept in `resolved_waits`, the
    next move goes to `pending_action` (with the advisor's full answer), and the coordinator is woken.
    Returns the wake event, or None when nothing was changed.
    """
    run_path = run_dir / "run.json"
    answers = dagview.advisor_answers(run_dir)
    answer = dagview.answer_for(answers, detail) if kind == "decision" else None
    if kind == "decision" and answer is None:
        return None  # never release on a stale scan that predates a provisional answer

    def mutate(run: dict[str, Any]) -> dict[str, Any] | None:
        t = ((dagview.batches(run).get(batch) or {}).get("tickets") or {}).get(name)
        if not isinstance(t, dict) or t.get("wait_on") != observed.get("wait_on") \
                or t.get("updated_at") != observed.get("updated_at"):
            return None
        stamp = dagview.next_updated_at(t.get("updated_at"))
        t.setdefault("resolved_waits", []).append({
            "wait_on": t.get("wait_on"), "wait_holder": t.get("wait_holder"), "at": stamp, "by": "watcher",
            "because": (f"advisor answered {detail}" if kind == "decision" else f"{detail or t.get('wait_holder')} accepted"),
            **({"answerAt": answer.get("at"), "verdict": answer.get("verdict")} if answer else {})})
        for field in ("wait_on", "wait_holder"):
            t.pop(field, None)
        _release_state(t)
        old = str(t.get("blocker") or "")
        t["blocker"] = ("[自动解除等待] " + (f"顾问已裁定 {detail}" if kind == "decision" else f"{detail or 'X'} 已验收")
                        + ("；原说明：" + STALE_WAIT_TOKEN.sub("", old).strip() if old else ""))[:600]
        t["pending_action"] = {
            "kind": "apply_decision" if kind == "decision" else "continue", "at": stamp,
            **({"question": detail, "verdict": answer.get("verdict"),
                "decision": str(answer.get("decision") or "")[:4000]} if answer else {"accepted": detail})}
        t["updated_at"] = stamp
        return dict(t)

    try:
        changed = dagview.update_run(run_path, mutate)
    except (OSError, ValueError, dagview.ViewError):
        return None
    if changed is None:
        return None
    action = changed["pending_action"]
    if action["kind"] == "apply_decision":
        summary = (f"{batch}/{name} 原本等顾问 {detail}，顾问已裁定（{action.get('verdict')}），观察器已自动解除等待。"
                   f"请按裁定执行（派下一步或写新 state）。裁定全文：{action.get('decision')}")
    else:
        summary = (f"{batch}/{name} 原本等 {detail}，它已验收，观察器已自动解除等待。请派 {name} 的下一步并写 state。")
    return {"kind": "attention", "ticket": name, "status": "wait-released", "queued_at": now,
            "summary": summary, "evidence": changed.get("evidence")}


PAGE_WINDOW_CLI = Path(__file__).resolve().parent / "local-dev/page-window.py"
PAGE_WINDOW_PYTHON = os.environ.get("HOST_DAG_PAGE_WINDOW_PYTHON") or sys.executable
PAGE_RETRY_SECONDS = 5 * 60
PAGE_LOST_SECONDS = 5 * 60


def _unit_prefix(owner: str) -> str:
    """Same name the library gives the window's units (pagewindow.window.Window)."""
    return "pw-" + "".join(c if c.isalnum() or c == "-" else "-" for c in owner.lower())[:60]


def _units_active(owner: str) -> bool:
    prefix = _unit_prefix(owner)
    proc = subprocess.run(["systemctl", "--user", "list-units", f"{prefix}-run*", f"{prefix}-finally*",
                           "--state=active,activating", "--no-legend", "--plain"],
                          capture_output=True, text=True, timeout=20, check=False)
    return bool(proc.stdout.strip())


def _latest_result(spec_path: Path) -> dict[str, Any] | None:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    root = Path(spec.get("evidence") or spec_path.parent)
    try:
        return json.loads((root / "result.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _run_cli(*args: str, timeout: float = 120) -> subprocess.CompletedProcess:
    return subprocess.run([PAGE_WINDOW_PYTHON, str(PAGE_WINDOW_CLI), *args], capture_output=True, text=True,
                          timeout=timeout, check=False)


def page_queue_events(state: dict[str, Any], data: dict[str, Any], now: float,
                      run_cli=None, units_active=None) -> list[dict[str, Any]]:
    """Stage A #4: run approved page-window specs when the page is free, one at a time.

    A request is approved by the coordinator (host-dag ticket set --approve-window). This loop claims
    the next one under the run lock, launches page-window.py run, and reads the window's result:
    WAIT_RESOURCE goes back to the queue; any other outcome is recorded and the coordinator woken.
    A claimed request whose run cannot be found is marked needs_attention, never relaunched blindly.
    """
    run_cli = run_cli or _run_cli
    units_active = units_active or _units_active
    anchor = state.get("state_path") or state.get("run_receipt")
    if not anchor:
        return []
    run_dir = Path(anchor).parent
    run_path = run_dir / "run.json"
    rows = [(b, n, t) for b, body in (data.get("batches") or {}).items()
            for n, t in ((body or {}).get("tickets") or {}).items()
            if isinstance(t, dict) and isinstance(t.get("page_request"), dict)]
    events: list[dict[str, Any]] = []

    def update(batch: str, name: str, request_id: str, change) -> dict[str, Any] | None:
        def mutate(run: dict[str, Any]):
            t = ((dagview.batches(run).get(batch) or {}).get("tickets") or {}).get(name)
            req = (t or {}).get("page_request") or {}
            if req.get("id") != request_id:
                return None  # withdrawn or replaced since it was read
            change(t, req)
            t["updated_at"] = dagview.next_updated_at(t.get("updated_at"))
            return dict(t)
        return dagview.update_run(run_path, mutate)

    running = [(b, n, t) for b, n, t in rows if t["page_request"].get("status") == "claimed"]
    for batch, name, t in running:
        req = t["page_request"]
        result = None
        try:
            result = _latest_result(Path(req["spec"]))
        except (OSError, ValueError):
            pass
        finished = result is not None and float(result.get("at") or 0) >= float(req.get("claimed_at_ts") or 0)
        if finished:
            outcome = result.get("outcome") or result.get("status")
            if outcome == "WAIT_RESOURCE":
                def back(t, req, result=result):
                    req.update(status="pending", next_check_ts=now + PAGE_RETRY_SECONDS,
                               last={"outcome": "WAIT_RESOURCE", "busy": result.get("busy"), "at": dagview.now_iso()})
                update(batch, name, req["id"], back)
                continue
            def done(t, req, outcome=outcome, result=result):
                req.update(status="done", outcome=outcome, finished_at=dagview.now_iso(),
                           result_status=result.get("status"), run_dir=result.get("runDir"))
                t["pending_action"] = {"kind": "window_result", "at": dagview.now_iso(), "request": req["id"],
                                       "outcome": outcome, "status": result.get("status")}
                t.pop("wait_on", None)
                _release_state(t)
            if update(batch, name, req["id"], done):
                events.append({"kind": "attention", "ticket": name, "status": "window-finished", "queued_at": now,
                               "summary": f"{batch}/{name} 的页面窗口 {req.get('window') or req['id']} 已结束：{outcome}"
                                          f"（{result.get('status')}）。结果 {req['spec']} 旁的 result.json；请验收或派下一步。",
                               "evidence": req["spec"]})
            continue
        if units_active(req.get("owner") or ""):
            continue
        if now - float(req.get("claimed_at_ts") or now) >= PAGE_LOST_SECONDS:
            def lost(t, req):
                req.update(status="needs_attention", reason="run unit gone without a result")
                t["pending_action"] = {"kind": "window_lost", "at": dagview.now_iso(), "request": req["id"]}
            if update(batch, name, req["id"], lost):
                events.append({"kind": "attention", "ticket": name, "status": "window-lost", "queued_at": now,
                               "summary": f"{batch}/{name} 的页面请求 {req['id']} 已启动但找不到运行单元也没有结果；"
                                          f"不会自动重启，请核对后重新批准或处置。", "evidence": req["spec"]})
    if running:
        return events
    try:
        page = json.loads((run_dir / "control/api-page-validation-pause.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return events
    if page.get("owner") or page.get("status") != "RESUMED":
        return events
    queue = sorted(((b, n, t) for b, n, t in rows if t["page_request"].get("status") == "pending"
                    and float(t["page_request"].get("next_check_ts") or 0) <= now
                    and ts.derive_state(t, None) not in ts.DONE),
                   key=lambda r: (-int(r[2]["page_request"].get("priority") or 0), r[2]["page_request"].get("approved_at") or ""))
    for batch, name, t in queue:
        req = t["page_request"]
        check = run_cli("check", req["spec"])
        if check.returncode != 0:
            def refused(t, req, check=check):
                req.update(status="needs_attention", reason="offline check failed", check=(check.stdout or check.stderr)[-1500:])
                t["pending_action"] = {"kind": "window_check_failed", "at": dagview.now_iso(), "request": req["id"]}
            if update(batch, name, req["id"], refused):
                events.append({"kind": "attention", "ticket": name, "status": "window-check-failed", "queued_at": now,
                               "summary": f"{batch}/{name} 批准的页面 spec 离线 check 不通过，未启动：{(check.stdout or check.stderr)[-600:]}",
                               "evidence": req["spec"]})
            continue
        def claim(t, req):
            req.update(status="claimed", claimed_at=dagview.now_iso(), claimed_at_ts=now)
        if update(batch, name, req["id"], claim) is None:
            continue
        launch = run_cli("run", req["spec"])
        if launch.returncode != 0:
            def failed(t, req, launch=launch):
                req.update(status="needs_attention", reason="launch failed", launch=(launch.stdout + launch.stderr)[-1500:])
                t["pending_action"] = {"kind": "window_launch_failed", "at": dagview.now_iso(), "request": req["id"]}
            update(batch, name, req["id"], failed)
            events.append({"kind": "attention", "ticket": name, "status": "window-launch-failed", "queued_at": now,
                           "summary": f"{batch}/{name} 页面窗口启动失败：{(launch.stdout + launch.stderr)[-600:]}",
                           "evidence": req["spec"]})
        else:
            events.append({"kind": "attention", "ticket": name, "status": "window-started", "queued_at": now,
                           "summary": f"{batch}/{name} 的已批准页面窗口 {req.get('window') or req['id']} 已由观察器启动。",
                           "evidence": req["spec"]})
        break  # one window at a time
    return events


def page_metrics_tick(state: dict[str, Any], data: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """v0 instrumentation (docs/local-dev/parallel-page-window-design-20261019.md): log every change of the
    page registry occupant and of each approved request's phase, with why it waits and on whom, to
    control/page-window-metrics.jsonl. Returns the rows written (for tests)."""
    anchor = state.get("state_path") or state.get("run_receipt")
    if not anchor:
        return []
    run_dir = Path(anchor).parent
    try:
        page = json.loads((run_dir / "control/api-page-validation-pause.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        page = {}
    last = state.setdefault("page_metric_last", {})
    rows: list[dict[str, Any]] = []
    occupant = [page.get("owner"), page.get("status"), (page.get("review") or {}).get("owner")]
    if last.get("page") != occupant:
        last["page"] = occupant
        rows.append({"event": "page", "owner": occupant[0], "status": occupant[1], "review": occupant[2]})
    requests = [(b, n, t, t["page_request"]) for b, body in (data.get("batches") or {}).items()
                for n, t in ((body or {}).get("tickets") or {}).items()
                if isinstance(t, dict) and isinstance(t.get("page_request"), dict)]
    pending = sorted((r for r in requests if r[3].get("status") == "pending"),
                     key=lambda r: (-int(r[3].get("priority") or 0), r[3].get("approved_at") or ""))
    first_due = next((r[3].get("id") for r in pending if float(r[3].get("next_check_ts") or 0) <= now), None)
    seen = set()
    for batch, name, t, req in requests:
        rid = req.get("id")
        seen.add(rid)
        phase, reason, holder = req.get("status"), None, None
        if phase == "pending":
            if float(req.get("next_check_ts") or 0) > now:
                reason = "retry-backoff"
            elif page.get("status") == "DRIFT_REVIEW":
                reason, holder = "page-drift-review", (page.get("review") or {}).get("owner")
            elif page.get("owner") or page.get("status") != "RESUMED":
                reason, holder = "page-held", page.get("owner")
            elif rid != first_due:
                reason, holder = "behind-in-queue", first_due
            else:
                reason = "eligible"
        key = [phase, reason, holder]
        if last.get(rid) != key:
            last[rid] = key
            rows.append({"event": "request", "request": rid, "ticket": name, "batch": batch,
                         "window": req.get("window"), "phase": phase, "reason": reason, "holder": holder,
                         "priority": req.get("priority"), "approved_at": req.get("approved_at"),
                         **({"outcome": req.get("outcome")} if req.get("outcome") else {})})
    for rid in [k for k in last if k not in seen and k != "page"]:
        last.pop(rid)
    if rows:
        stamp = dagview.now_iso()
        with (run_dir / "control/page-window-metrics.jsonl").open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps({"at": now, "iso": stamp, **row}, ensure_ascii=False) + "\n")
    return rows


LOCK_WAIT_KINDS = ("devcluster-shared", "host", "heavy-slot")
LOCK_FREE_SECONDS = 10 * 60


def _wait_lock_files(run_dir: Path, kind: str, detail: str) -> list[Path]:
    """The flock files behind a lock-type wait (historical case: the board said another ticket held
    devcluster-shared long after the lock was free; nobody looked at the lock itself)."""
    control = run_dir / "control"
    if kind == "devcluster-shared":
        return [control / "devcluster-shared.lock"]
    if kind == "host" and detail:
        return [control / f"host-{detail.replace('.', '-')}.lock"]
    if kind == "heavy-slot":
        return resource_locks.observed_paths(run_dir)
    return []


def _lock_free(path: Path) -> bool | None:
    """Read kernel ownership by device/inode without joining or disturbing its flock queue."""
    try:
        if not path.exists():
            return None
        return not resource_locks.flock_rows(path)
    except OSError:
        return None


def page_wait_ready(state: dict[str, Any], rec: dict[str, Any], tickets: dict[str, Any],
                    page: dict[str, Any], now: float) -> bool:
    if (rec.get("wait_on") != "page-window" or rec.get("page_request")
            or ts.derive_state(rec, tickets) not in ts.WAITING):
        return False
    holder = rec.get("wait_holder")
    watched = (state.get("tracked") or {}).get(holder)
    holder_ticket = tickets.get(holder) or {}
    if isinstance(watched, dict):
        if not watched.get("terminal"):
            return False
    elif holder_ticket.get("executor") and ts.derive_state(holder_ticket, tickets) == ts.RUNNING:
        return False
    since = _iso_seconds(rec.get("updated_at")) or now
    return not page.get("owner") and page.get("status") == "RESUMED" and now - since >= 10 * 60


def prune_page_wait_events(state: dict[str, Any], now: float) -> None:
    """Revalidate busy-parent notices; preserve packets until delivery recovery finishes."""
    if (state.get("pending_send") or {}).get("status") in SEND_REPLAY_STATES:
        return
    anchor = state.get("state_path") or state.get("run_receipt")
    if not anchor:
        return
    run_dir = Path(anchor).parent
    data = registrations.read_object(state.get("run_receipt") or run_dir / "run.json")
    tickets = ts.all_tickets(data)
    page = registrations.read_object(run_dir / "control/api-page-validation-pause.json")
    kept = []
    for event in state.get("pending_events") or []:
        page_notice = event.get("status") == "resolved-wait" and (
            event.get("wait_on") == "page-window"
            or "（wait_on=page-window）" in str(event.get("summary") or ""))
        rec = tickets.get(event.get("ticket")) or {}
        if page_notice and not page_wait_ready(state, rec, tickets, page, now):
            state.setdefault("resolved_wait_nudged", {}).pop(f"{event.get('ticket')}:page-window:", None)
            continue
        kept.append(event)
    state["pending_events"] = kept


def resolved_wait_events(state: dict[str, Any], data: dict[str, Any], now: float) -> list[dict[str, Any]]:
    """Tickets still recorded as waiting on something that is already there.

    avoid re-triggering advisor alerts for tickets already answered
    and a waited ticket had been accepted a day earlier (KV18-06 -> KV18-03), yet nobody moved them.
    Checked live: decision:<key> answered in the advisor log, ticket:<id> accepted in any batch,
    page-window free in the registry. One nudge per ticket and reason, repeated at the idle interval.
    """
    anchor = state.get("state_path") or state.get("run_receipt")
    if not anchor:
        return []
    run_dir = Path(anchor).parent
    verdicts = _advisor_verdicts(run_dir)
    everything = {name: rec for body in (data.get("batches") or {}).values()
                  for name, rec in ((body or {}).get("tickets") or {}).items() if isinstance(rec, dict)}
    try:
        page = json.loads((run_dir / "control/api-page-validation-pause.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        page = {}
    nudged = state.setdefault("resolved_wait_nudged", {})
    events: list[dict[str, Any]] = []
    for batch, body in (data.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        for name, rec in tickets.items():
            if not isinstance(rec, dict) or ts.derive_state(rec, tickets) not in ts.WAITING:
                continue
            kind, _, detail = str(rec.get("wait_on") or "").partition(":")
            since = _iso_seconds(rec.get("updated_at")) or now
            reason = None
            if kind == "decision" and detail:
                when = next((at for key, at in verdicts.items() if key == detail or key.endswith(detail)), None)
                if when is not None and now - when >= RESOLVED_WAIT_GRACE_SECONDS:
                    reason = (f"它等的顾问问题 {detail} 已于 {time.strftime('%H:%M', time.localtime(when))} 裁定，"
                              f"请按裁定执行并更新 state")
            elif kind == "ticket":
                target = detail or rec.get("wait_holder")
                other = everything.get(target or "")
                if other is not None and ts.derive_state(other, everything) in ts.DONE:
                    reason = f"它等的票 {target} 已验收，请派下一步或更新 state"
            elif kind in LOCK_WAIT_KINDS:
                files = _wait_lock_files(run_dir, kind, detail)
                states = [_lock_free(f) for f in files]
                free_since = state.setdefault("lock_free_since", {})
                lock_key = f"{name}:{kind}:{detail}"
                # Admission and build locks are independent constraints, not interchangeable slots.
                free = bool(states) and all(x is True for x in states)
                if not free:
                    free_since.pop(lock_key, None)
                    continue
                first = float(free_since.setdefault(lock_key, now))
                if now - first >= LOCK_FREE_SECONDS and now - since >= LOCK_FREE_SECONDS:
                    holder = rec.get("wait_holder") or "?"
                    reason = (f"它等的{dagview.WAIT_KINDS.get(kind, kind)}（{', '.join(f.name for f in files)}）"
                              f"已连续 {int((now - first) // 60)} 分钟没人持有（登记的占用方 {holder}）；"
                              f"请核实后撤掉这条等待或改成真实的 wait_on")
            elif kind == "page-window":
                if page_wait_ready(state, rec, everything, page, now):
                    reason = "它等的本机页面窗口已空闲，请按队列开窗"
            if not reason:
                continue
            if kind in ("decision", "ticket"):
                released = release_wait(run_dir, batch, name, rec, kind, detail, verdicts, now)
                if released is not None:
                    events.append(released)
                    continue
                if kind == "decision" and dagview.answer_for(dagview.advisor_answers(run_dir), detail) is None:
                    continue  # the scan became stale; don't emit an "already answered" nudge either
            key = f"{name}:{kind}:{detail}"
            if now - float(nudged.get(key, 0)) < IDLE_RENUDGE_SECONDS:
                continue
            nudged[key] = now
            events.append({"kind": "attention", "ticket": name, "status": "resolved-wait", "queued_at": now,
                           "wait_on": rec.get("wait_on"),
                           "summary": f"{batch}/{name} 还登记为等待（wait_on={rec.get('wait_on')}），但{reason}。",
                           "evidence": rec.get("evidence")})
    return events


def check_idle_tickets(state: dict[str, Any], now: float) -> None:
    try:
        resource_locks.watch_locks(state, now)
    except (OSError, ValueError):
        logging.exception("resource lock observation failed")
    receipt = state.get("run_receipt")
    try:
        data = json.loads(Path(receipt).read_text(encoding="utf-8")) if receipt else {}
    except (OSError, json.JSONDecodeError):
        return
    data = data if isinstance(data, dict) else {}
    prune_page_wait_events(state, now)
    # A reminder queued while the parent was busy may have become obsolete.
    # Never alter a packet whose delivery is still being recovered.
    send = state.get("pending_send") or {}
    if send.get("status") not in SEND_REPLAY_STATES:
        tickets = {name: rec for body in (data.get("batches") or {}).values()
                   for name, rec in (body.get("tickets") or {}).items()}
        def obsolete_idle(event: dict[str, Any]) -> bool:
            if event.get("kind") != "attention" or event.get("status") != "idle":
                return False
            rec = tickets.get(event.get("ticket")) or {}
            blocker = str(rec.get("blocker") or "")
            if blocker.startswith("WAIT-UNTIL:"):
                until = _iso_seconds(blocker.split()[0].removeprefix("WAIT-UNTIL:"))
                return until is not None and now < until
            return False
        state["pending_events"] = [e for e in state.get("pending_events", []) if not obsolete_idle(e)]
    # Pushes come from state transitions, not from how the coordinator worded its alert (2026-10-15).
    if data.get("batches"):
        state_alerts(state, data, now)
    for event in state_drift_events(state, data, now):
        queue_event(state, event)
        alert(state, f"drift:{event['ticket']}", f"{event['ticket']} 状态与执行者不一致，已提醒协调者",
              event["summary"], now, desktop=False)
    try:
        page_metrics_tick(state, data, now)
    except OSError:
        pass  # instrumentation must never stop the watcher
    for event in page_queue_events(state, data, now):
        queue_event(state, event)
        alert(state, f"page-queue:{event['ticket']}:{event['status']}", f"{event['ticket']} 页面队列：{event['status']}",
              event["summary"], now, desktop=False)
    for event in resolved_wait_events(state, data, now):
        queue_event(state, event)
        alert(state, f"resolved-wait:{event['ticket']}", f"{event['ticket']} 等的东西已经好了，已提醒协调者",
              event["summary"], now, desktop=False)
    for event in review_verdict_events(state, data, now):
        queue_event(state, event)
        alert(state, f"review-verdict:{event['ticket']}", f"{event['ticket']} 审查已出结论，已提醒协调者",
              event["summary"], now, desktop=False)
    for event in idle_ticket_events(state, data, now):
        queue_event(state, event)
        # Log only: the coordinator gets the nudge; the user found the desktop popups noisy (2026-10-15).
        alert(state, f"idle:{event['ticket']}", f"{event['ticket']} 没人在跑，已提醒协调者", event["summary"], now,
              desktop=False)


def observe_credit_pause(state: dict[str, Any], now: float, pause: dict[str, Any]) -> dict[str, Any]:
    """Original observer, local receipts only: never resume work or call a model.

    Keep all prior tracked/pending records. Only the two explicitly granted
    tickets may have registrations consumed; terminal events stay in the same
    original queue until the parent handles them. The original quota timer
    supplies the balance; a stale/missing reading fails closed for active CLI.
    """
    allowed = set(pause.get('priority_tickets') or [])
    path = Path(state['state_path'])
    registrations.drain(state, path, save_state, allowed_tickets=allowed)
    before = len(state.get('pending_events') or [])
    observed = []
    balance = pause.get('last_balance')
    stamp = _iso_seconds(pause.get('last_checked'))
    funded = isinstance(balance, (int, float)) and balance > 2300 and stamp is not None and 0 <= now - stamp < 180
    direction = state.get('direction_interval')
    state['direction_interval'] = math.inf  # suppress cadence events during local-only observation
    try:
        for ticket, rec in (state.get('tracked') or {}).items():
            if ticket not in allowed or rec.get('provider') != 'reasonix':
                continue
            if not funded:
                run = registrations.read_object(rec.get('run_file') or '')
                if run.get('unit') and not (Path(rec['run_file']).parent / 'cli-result.json').exists() and cli_unit_active(run['unit']):
                    subprocess.run([sys.executable, str(Path(__file__).with_name('host-dag-cli-agent.py')),
                                    'cancel', '--out', str(Path(rec['run_file']).parent)],
                                   capture_output=True, timeout=25, check=False)
            observe_cli_ticket(state, ticket, rec, now)
            observed.append(ticket)
    finally:
        state['direction_interval'] = direction
    state['credit_pause_observation'] = {'at': now, 'tickets': observed, 'allowed': sorted(allowed),
                                       'funded': funded, 'balance': balance, 'called_model': False}
    save_state(path, state)
    b5_mirror(state)
    return {'ok': True, 'called_host': False, 'events': (state.get('pending_events') or [])[before:],
            'send': 'held_credit_pause', 'truncated': False, 'observed': observed}


def _grace(state: dict[str, Any]) -> float:
    return float(state.get("terminal_grace_seconds", DEFAULT_TERMINAL_GRACE_SECONDS) or 0)


def event_delivered(state: dict[str, Any], ticket: str) -> bool:
    """No event of this ticket waits in pending_events (a send only drops events once delivered)."""
    return not any(isinstance(e, dict) and e.get("ticket") == ticket for e in state.get("pending_events") or [])


def host_read_mode(state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float) -> str:
    """required (counted) for live records; optional or skip for terminal ones."""
    if not rec.get("terminal"):
        return "required"
    if rec.get("terminal_since") is None:
        rec["terminal_since"] = now - _grace(state)  # terminal before this watcher version: settled
    if now >= float(rec["terminal_since"]) + _grace(state) and event_delivered(state, ticket):
        return "skip"
    if now < float(state.get("terminal_recheck_backoff_until") or 0):
        return "skip"
    return "optional"


def note_terminal(rec: dict[str, Any], now: float) -> None:
    if not rec.get("terminal"):
        rec.pop("terminal_since", None)
        rec.pop("terminal_checked_at", None)
    elif rec.get("terminal_since") is None:
        rec["terminal_since"] = now


def optional_host_read(state: dict[str, Any], ticket: str, rec: dict[str, Any], now: float,
                       called: HostCalls, read) -> Any:
    """Run read(); its host failure is recorded (optional, not counted) instead of raised."""
    mark, successes = len(called.timeouts), called.successes
    try:
        return read()
    except HostError as exc:
        timeout = called.timeouts[mark:]
        error = dict(timeout[-1]) if timeout else {
            "code": exc.code, "message": exc.message, "returncode": exc.returncode, "at": time.time(),
            "call": [state["host_cli"], "thread", "read", str(rec.get("thread"))],
        }
        error.update(optional=True, counted=False, ticket=ticket)
        called.optional_errors.append(error)
        state["last_host_error"] = error
        if exc.code in {"TIMEOUT", "RUNTIME_UNREACHABLE"}:
            state["terminal_recheck_backoff_until"] = now + TERMINAL_RECHECK_BACKOFF_SECONDS
        return None
    finally:
        del called.timeouts[mark:]  # optional reads never feed consecutive_host_timeouts
        called.successes = successes  # ...nor reset it


def recheck_terminal(state: dict[str, Any], tracked: dict[str, Any], now: float, called: HostCalls) -> None:
    """Optional, rate-limited read of one skipped terminal record (oldest check first).

    It only re-arms a record whose thread moved on (running again, or a new result fingerprint); the
    next poll then observes it through the normal path, which owns all event logic.
    """
    interval = float(state.get("terminal_recheck_seconds", DEFAULT_TERMINAL_RECHECK_SECONDS) or 0)
    if interval <= 0 or now < float(state.get("terminal_recheck_backoff_until") or 0):
        return
    due = []
    for ticket, rec in tracked.items():
        if not isinstance(rec, dict) or not rec.get("terminal") or not rec.get("thread") \
                or rec.get("provider") in (*CLI_PROVIDERS, "grok") \
                or host_read_mode(state, ticket, rec, now) != "skip":
            continue
        if rec.get("terminal_checked_at") is None:
            rec["terminal_checked_at"] = now  # first skip: start the clock, no burst after deploy
            continue
        if now >= float(rec["terminal_checked_at"]) + interval:
            due.append((float(rec["terminal_checked_at"]), ticket, rec))
    if not due:
        return
    _, ticket, rec = min(due, key=lambda item: item[:2])
    rec["terminal_checked_at"] = now
    payload = optional_host_read(state, ticket, rec, now, called,
                                 lambda: run_host(state, ["thread", "read", rec["thread"]], called))
    if payload is None:
        return
    if payload.get("status") in KNOWN_RUNNING or fingerprint(payload) != rec.get("last_result_fingerprint"):
        rec["terminal"] = False
        rec["terminal_rearmed_at"] = now
        note_terminal(rec, now)


def poll_once(state: dict[str, Any], now: float) -> dict[str, Any]:
    global _b5_seen
    _b5_seen = []
    called = HostCalls()
    started = time.monotonic()
    pause_path = Path(state['state_path']).parent / 'control' / 'credit-pause.json'
    if pause_path.exists():
        pause = json.loads(pause_path.read_text())
        if pause.get('active'):
            return observe_credit_pause(state, now, pause)
    registrations.drain(state, Path(state["state_path"]), save_state)
    def sync_view() -> str:
        return json.dumps([state.get("tracked"), state.get("pending_events"),
                           state.get("idle_wait_nudged"), state.get("registration_sources"),
                           state.get("registration_gaps")], sort_keys=True, default=str)

    synced_from = sync_view()
    sync_run_if_needed(state, now)
    tracked = state.get("tracked") or {}
    before = list(state.get("pending_events") or [])
    if not tracked:
        check_idle_tickets(state, now)
    pending = state.setdefault("pending_events", [])
    in_flight = isinstance(state.get("pending_send"), dict) and state["pending_send"].get("status") in {
        "intent", "busy", "uncertain",
    }
    if not tracked and not pending and not in_flight:
        if synced_from != sync_view():
            save_state(Path(state["state_path"]), state)  # keep the receipt-driven cleanup
        return {"ok": True, "called_host": False, "events": [], "truncated": False, "send": None}
    state["server_identity"] = server_identity() or state.get("server_identity")
    # Observe all local receipts before *any* host call, including goal pause
    # and host-backed workers. Persist them before a potentially slow parent read.
    for ticket, rec in list(tracked.items()):
        if rec.get("provider") in (*CLI_PROVIDERS, "grok"):
            observe_ticket(state, ticket, rec, now, called)

    def added_events() -> list[dict[str, Any]]:
        before_ids = {id(event) for event in before}
        added = [event for event in state.get("pending_events") or [] if id(event) not in before_ids]
        for event in added:
            event.setdefault("queued_at", now)
        return added

    added = added_events()
    if synced_from != sync_view():
        save_state(Path(state["state_path"]), state)
    send_info: dict[str, Any] = {"send": "held_host_error"}
    try:
        if state.get("pause_parent_goal") and parent_goal_pause_due(state, now):
            pause_parent_goal(state, now, called)
        for ticket, rec in list(tracked.items()):
            if rec.get("provider") in (*CLI_PROVIDERS, "grok"):
                continue
            mode = host_read_mode(state, ticket, rec, now)
            if mode == "required":
                observe_ticket(state, ticket, rec, now, called)
            elif mode == "optional":
                optional_host_read(state, ticket, rec, now, called,
                                   lambda t=ticket, r=rec: observe_ticket(state, t, r, now, called))
            note_terminal(rec, now)
        if tracked:
            check_idle_tickets(state, now)
        added = added_events()
        coalesce_pending(state)
        send_info = deliver_pending(state, now, called)
        recheck_terminal(state, tracked, now, called)
        seen = {id(event) for event in added}  # delivery already dropped sent events from the queue
        added += [event for event in added_events() if id(event) not in seen]
    except HostError as exc:
        added = added_events()
        if exc.code != "TIMEOUT":
            state["last_host_error"] = {"code": exc.code, "message": exc.message, "returncode": exc.returncode}
            save_state(Path(state["state_path"]), state)
            raise
        # Also support adapters/tests that raise TIMEOUT without going through
        # subprocess.run. Real calls already recorded their exact duration.
        if not called.timeouts:
            called.timeouts.append({
                "code": exc.code, "message": exc.message, "returncode": exc.returncode,
                "at": time.time(), "call": [state['host_cli'], *called[-1]] if called else [],
                "elapsed_seconds": time.monotonic() - started,
            })
    timed_out = bool(called.timeouts)
    if timed_out:
        error = called.timeouts[-1]
        state["last_host_error"] = error
        failures = int(state.get("consecutive_host_timeouts") or 0) + 1
        state["consecutive_host_timeouts"] = failures
        error["consecutive"] = failures
    elif called.successes:
        if "consecutive_host_timeouts" in state:
            state["consecutive_host_timeouts"] = 0
        if send_info.get("send") in {None, "delivered", "held_busy", "thread_busy", "settling", "held_limit",
                                     "redelivered"}:
            state["last_host_error"] = None
    if not timed_out and called.optional_errors:
        state["last_host_error"] = called.optional_errors[-1]  # recorded, not counted
    limit = int(state.get("event_limit") or DEFAULT_EVENT_LIMIT)
    session_ok = send_info.get("send") not in {"unknown_parent"} and not any(
        item.get("kind") == "unknown_status" for item in added
    )
    save_state(Path(state["state_path"]), state)
    b5_mirror(state)
    if timed_out and failures >= HOST_TIMEOUT_FAILURE_LIMIT:
        alert(state, "host-timeout", "watcher host 连续超时，需检查宿主 CLI",
              f"连续 {failures} 轮 TIMEOUT；本地完成事件已入队。watcher 退出后由 systemd 在 60 秒后自动重启；请父协调者检查 {state['host_cli']}。",
              now)
        save_state(Path(state["state_path"]), state)
        raise HostError("TIMEOUT", "host cli timed out in consecutive polling cycles", -1)
    payload = {
        "called_host": bool(called),
        "events": added[:limit],
        **send_info,
        "ok": session_ok,
        "truncated": bool(send_info.get("truncated")) or len(added) > limit,
    }
    return payload


# ---------------------------------------------------------------- stage B5: first-hand facts for the core
#
# Design §10 B5 / §13.4 / §16. Each observation above (the session's own status, turn id and final
# message) is copied into ``_b5_seen``; after the cycle's state is saved, ``b5_mirror`` hands the copies to
# a daemon worker bounded by HOST_DAG_DAG_WATCH_MIRROR_BUDGET (default 2 s). Until that worker has seen
# <run>/sm/state.db the watcher knows of no core: it imports nothing more, writes nothing, warns nothing
# and installs no signal handler -- the live run has no sm/, so the watcher behaves exactly as before.
# With a core, hostdag_dag_watch.mirror submits the facts (one connection, closed at once) and never
# raises; a worker still running from an earlier cycle makes this cycle skip (the next mirror reports it).
# Nothing here reads run.json: facts come only from what the watcher itself read from the sessions.

B5_BUDGET_ENV = "HOST_DAG_DAG_WATCH_MIRROR_BUDGET"
B5_DEFAULT_BUDGET = 2.0
_b5_seen: list[dict[str, Any]] | None = None
_b5_worker: threading.Thread | None = None
_b5_memory: dict[str, Any] = {}  # alert episodes etc.; touched only by the (single) worker
_b5_overruns = 0
_b5_last: dict[str, Any] = {}  # the last worker's report (tests)


def b5_note(ticket: str, rec: dict[str, Any], now: float, status: object, *, turn: object = None,
            text: object = None, error: str | None = None, healed: bool = False, cwd: object = None) -> None:
    if _b5_seen is None:
        return
    try:  # bookkeeping on the observation path: it must never change what the watcher does
        provider = rec.get("provider") or "codex"
        session = (rec.get("session") if provider == "grok" else
                   rec.get("run_file") if provider in CLI_PROVIDERS else rec.get("thread"))
        _b5_seen.append({
            "key": ticket, "provider": provider, "session": session, "status": str(status or ""),
            "at": dt.datetime.fromtimestamp(now).astimezone().isoformat(timespec="seconds"),
            "turn": turn if isinstance(turn, str) else None, "text": text if isinstance(text, str) else None,
            "error": error, "healed": healed, "cwd": cwd if isinstance(cwd, str) else None,
        })
    except Exception:  # noqa: BLE001
        pass


def _b5_budget() -> float:
    try:
        return max(0.1, float(os.environ.get(B5_BUDGET_ENV) or B5_DEFAULT_BUDGET))
    except ValueError:
        return B5_DEFAULT_BUDGET


def b5_mirror(state: dict[str, Any]) -> None:
    """Hand this cycle's observations to the bounded core mirror. Never raises, never blocks longer
    than the budget, prints nothing."""
    global _b5_worker, _b5_overruns
    try:
        seen, receipt = _b5_seen, state.get("run_receipt")
        if not seen or not receipt or os.environ.get("HOST_DAG_DAG_MIRROR") == "0":
            return
        if _b5_worker is not None and _b5_worker.is_alive():
            _b5_overruns += 1
            return
        run_dir = Path(receipt).parent
        observations = [dict(item) for item in seen]
        parent = state.get("parent_thread")
        budget = _b5_budget()
        overruns, _b5_overruns = _b5_overruns, 0

        def body() -> None:
            try:
                if not (run_dir / "sm" / "state.db").is_file():
                    return  # no core: the watcher of before B5
                import hostdag_dag_watch as core_watch  # noqa: PLC0415 - only when a core database exists
                _b5_last["report"] = core_watch.mirror(run_dir, observations, parent if isinstance(parent, str)
                                                       else None, _b5_memory, budget=budget, overruns=overruns)
            except BaseException as exc:  # noqa: BLE001 - never reaches the watcher loop
                _b5_last["error"] = f"{type(exc).__name__}: {exc}"

        worker = threading.Thread(target=body, name="host-watch-core-mirror", daemon=True)
        _b5_worker = worker
        worker.start()
        worker.join(budget)
    except Exception:  # noqa: BLE001
        pass


def b5_join() -> None:
    """A one-shot `poll` waits (bounded) for its mirror worker before the process exits, so the worker is
    not killed half way through an append (review N5). The long-running `watch` never waits here."""
    try:
        worker = _b5_worker
        if worker is not None and worker.is_alive():
            worker.join(_b5_budget())
    except Exception:  # noqa: BLE001
        pass


def public_tracked(state: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for ticket, rec in (state.get("tracked") or {}).items():
        row = {
            "ticket": ticket,
            "evidence": rec.get("evidence"),
            "last_status": rec.get("last_status"),
            "terminal": bool(rec.get("terminal")),
        }
        if rec.get("provider") == "grok":
            row["provider"] = "grok"
            row["session"] = rec.get("session")
            row["log"] = rec.get("log")
        elif rec.get("provider") in CLI_PROVIDERS:
            row["provider"] = rec.get("provider")
            row["run_file"] = rec.get("run_file")
        else:
            row["thread"] = rec.get("thread")
        rows.append(row)
    return rows


def public_send(state: dict[str, Any]) -> dict[str, Any] | None:
    send = state.get("pending_send")
    if not isinstance(send, dict):
        return None
    return {"status": send.get("status"), "marker": send.get("marker")}


def cmd_init(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    path = Path(args.state)
    with ExclusiveLock(path):
        existing: dict[str, Any] = {}
        if path.is_file():
            existing = load_state(path)
        host_cwd = args.host_cwd or os.path.abspath(os.getcwd())
        state = {
            **{k: v for k, v in existing.items()
               if k.startswith("registration_") or k.startswith("idle_")},
            "version": 1,
            "state_path": str(path),
            "parent_thread": args.parent_thread,
            "host_cli": require_abs(args.host_cli, "host-cli"),
            "host_cwd": require_abs(os.path.abspath(host_cwd), "host-cwd"),
            "poll_interval": float(args.poll_interval),
            "direction_interval": float(args.direction_interval),
            "host_timeout": float(args.host_timeout),
            "event_limit": int(args.event_limit),
            "settle_seconds": float(args.settle_seconds),
            "max_hold_seconds": float(args.max_hold_seconds),
            "tracked": existing.get("tracked") if isinstance(existing.get("tracked"), dict) else {},
            "pending_events": existing.get("pending_events") if isinstance(existing.get("pending_events"), list) else [],
            "pending_send": existing.get("pending_send"),
            "last_host_error": None,
            "run_receipt": require_abs(args.run_receipt, "run-receipt") if args.run_receipt else existing.get("run_receipt"),
            "pause_parent_goal": bool(
                existing.get("pause_parent_goal") if args.pause_parent_goal is None else args.pause_parent_goal
            ),
        }
        if args.poll_interval < 0 or args.direction_interval < 0:
            raise ValueError("intervals must be >= 0")
        if args.host_timeout <= 0:
            raise ValueError("host-timeout must be > 0")
        if args.event_limit < 1:
            raise ValueError("event-limit must be >= 1")
        if args.settle_seconds < 0 or args.max_hold_seconds <= 0:
            raise ValueError("settle-seconds must be >= 0 and max-hold-seconds > 0")
        if state.get("run_receipt"):
            sync_run_if_needed(state, now_value(args))
        save_state(path, state)
        return {
            "ok": True,
            "state": str(path),
            "parent_thread": state["parent_thread"],
            "host_cli": state["host_cli"],
            "host_cwd": state["host_cwd"],
        }, 0


def registration_args(args: argparse.Namespace) -> dict[str, Any]:
    ticket = require_ticket(args.ticket)
    op = args.command
    request = {"op": op, "ticket": ticket, "at": now_value(args)}
    if op == "track":
        request["record"] = {"thread": args.thread, "evidence": args.evidence}
    elif op == "track-grok":
        if not args.session:
            raise ValueError("session is required")
        request["record"] = {"provider": "grok", "session": args.session,
                             "log": require_abs(args.log, "log"),
                             "wrapper_pid": args.wrapper_pid, "evidence": args.evidence}
    elif op == "track-cli":
        request["record"] = {"provider": args.provider,
                             "run_file": require_abs(args.run_file, "run-file"), "evidence": args.evidence}
    return request


def cmd_registration(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    path = Path(args.state)
    load_state(path)  # Never acknowledge a request for an uninitialized observer.
    request = registration_args(args)
    # A separate atomic file can be submitted even while watch owns its lifetime lock.
    request_path = registrations.enqueue(path, request)
    try:
        with ExclusiveLock(path):
            state = load_state(path)
            registrations.drain(state, path, save_state)
    except BusyError:
        pass
    receipt = registrations.receipt(request_path)
    return {"ok": receipt.get("status") != "rejected", "ticket": args.ticket,
            **request.get("record", {}), "registration": receipt,
            "request": str(request_path)}, 2 if receipt.get("status") == "rejected" else 0


cmd_track = cmd_track_grok = cmd_track_cli = cmd_untrack = cmd_registration


def cmd_poll(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    path = Path(args.state)
    with ExclusiveLock(path):
        state = load_state(path)
        state["state_path"] = str(path)
        if args.poll_interval is not None:
            state["poll_interval"] = float(args.poll_interval)
        if args.direction_interval is not None:
            state["direction_interval"] = float(args.direction_interval)
        if args.run_receipt:
            state["run_receipt"] = require_abs(args.run_receipt, "run-receipt")
        try:
            payload = poll_once(state, now_value(args))
        except HostError as exc:
            return {
                "ok": False,
                "error": exc.code,
                "message": exc.message,
                "returncode": exc.returncode,
                "called_host": True,
            }, 3 if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"} else 1
        b5_join()
        return payload, 0


def reset_host_timeout_streak(path: Path) -> None:
    """Start this process's timeout streak at zero (caller holds the state lock).

    A count inherited from an earlier process made a restarted watcher exit on its first timeout
    (2026-11-04: consecutive_host_timeouts=9, 7 exits/hour). last_host_error stays for diagnosis.
    """
    state = load_state(path)
    if state.get("consecutive_host_timeouts"):
        state["consecutive_host_timeouts"] = 0
        save_state(path, state)


def cmd_watch(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    path = Path(args.state)
    stop = Path(args.stop_file) if args.stop_file else None
    cycles = 0
    stopped = "running"
    last: dict[str, Any] = {}
    with ExclusiveLock(path):
        try:
            reset_host_timeout_streak(path)
            while True:
                state = load_state(path)
                state["state_path"] = str(path)
                interval = float(args.poll_interval if args.poll_interval is not None else state.get("poll_interval") or 0)
                try:
                    last = poll_once(state, now_value(args))
                except HostError as exc:
                    return {
                        "ok": False,
                        "error": exc.code,
                        "message": exc.message,
                        "returncode": exc.returncode,
                        "cycles": cycles,
                    }, 3 if exc.code in {"RUNTIME_UNREACHABLE", "TIMEOUT"} else 1
                cycles += 1
                # Liveness for host-dag-watch-watchdog: written every cycle, even when nothing changed.
                path.with_name(path.name + ".heartbeat").write_text(str(time.time()), encoding="utf-8")
                if args.max_cycles is not None and cycles >= args.max_cycles:
                    stopped = "max-cycles"
                    break
                if stop is not None and stop.exists():
                    stopped = "stop-file"
                    break
                if interval > 0:
                    deadline = time.time() + interval
                    while time.time() < deadline:
                        if stop is not None and stop.exists():
                            stopped = "stop-file"
                            break
                        time.sleep(min(0.2, max(0.0, deadline - time.time())))
                    if stopped == "stop-file":
                        break
        except KeyboardInterrupt:
            stopped = "signal"
    return {"ok": True, "cycles": cycles, "stopped": stopped, "last": last}, 0


def cmd_status(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    path = Path(args.state)
    state = load_state(path)
    return {
        "ok": True,
        "registration_gaps": state.get("registration_gaps", {}),
        "registration_pending": len(list(registrations.inbox(path).glob("*.json"))),
        "parent_thread": state.get("parent_thread"),
        "host_cli": state.get("host_cli"),
        "tracked": public_tracked(state),
        "pending_events": list(state.get("pending_events") or []),
        "pending_send": public_send(state),
        "last_host_error": state.get("last_host_error"),
        "parent_goal": state.get("parent_goal"),
    }, 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="host-dag-watch.py",
        description="Observe Codex Host ticket threads and Grok ACP sessions; wake one parent thread.",
    )
    parser.add_argument("--state", required=True, help="observer state file")
    parser.add_argument("--now", type=float, default=None, help="frozen unix time for tests")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="bind parent thread, host CLI, and state file")
    init.add_argument("--parent-thread", required=True)
    init.add_argument("--host-cli", required=True, help="absolute Host CLI path")
    init.add_argument("--host-cwd", help="absolute cwd for Host subprocesses")
    init.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL)
    init.add_argument("--direction-interval", type=float, default=DEFAULT_DIRECTION_INTERVAL)
    init.add_argument("--host-timeout", type=float, default=DEFAULT_HOST_TIMEOUT)
    init.add_argument("--event-limit", type=int, default=DEFAULT_EVENT_LIMIT)
    init.add_argument(
        "--settle-seconds", type=float, default=DEFAULT_SETTLE_SECONDS,
        help="hold routine outcomes until no new event arrives for this long (0 sends at once)",
    )
    init.add_argument(
        "--max-hold-seconds", type=float, default=DEFAULT_MAX_HOLD_SECONDS,
        help="deliver settled events no later than this after the oldest one queued",
    )
    init.add_argument("--run-receipt", default=None, help="original DAG run.json for Grok ticket sync")
    init.add_argument(
        "--pause-parent-goal", action=argparse.BooleanOptionalAction, default=None,
        help="keep the parent Codex Goal paused so only watcher events wake it (host CLI needs `goal pause`)",
    )
    init.set_defaults(func=cmd_init)

    track = sub.add_parser("track", help="observe a ticket thread")
    track.add_argument("--ticket", required=True)
    track.add_argument("--thread", required=True)
    track.add_argument("--evidence", default="")
    track.set_defaults(func=cmd_track)

    grok = sub.add_parser("track-grok", help="observe a Grok ACP sessionId/log/wrapper_pid")
    grok.add_argument("--ticket", required=True)
    grok.add_argument("--session", required=True, help="Grok sessionId, not a Codex threadId")
    grok.add_argument("--log", required=True, help="absolute ACP NDJSON log path")
    grok.add_argument("--wrapper-pid", type=int, default=None)
    grok.add_argument("--evidence", default="")
    grok.set_defaults(func=cmd_track_grok)

    cli = sub.add_parser("track-cli", help="observe a Cursor/Claude/Qoder run started by host-dag-cli-agent.py")
    cli.add_argument("--ticket", required=True)
    cli.add_argument("--provider", choices=CLI_PROVIDERS, required=True)
    cli.add_argument("--run-file", required=True, help="absolute path to cli-run.json")
    cli.add_argument("--evidence", default="")
    cli.set_defaults(func=cmd_track_cli)

    untrack = sub.add_parser("untrack", help="stop observing a ticket")
    untrack.add_argument("--ticket", required=True)
    untrack.set_defaults(func=cmd_untrack)

    poll = sub.add_parser("poll", help="one bounded observation cycle")
    poll.add_argument("--poll-interval", type=float, default=None)
    poll.add_argument("--direction-interval", type=float, default=None)
    poll.add_argument("--run-receipt", default=None)
    poll.set_defaults(func=cmd_poll)

    watch = sub.add_parser("watch", help="bounded poll loop until stop")
    watch.add_argument("--poll-interval", type=float, default=None)
    watch.add_argument("--max-cycles", type=int, default=None)
    watch.add_argument("--stop-file", default=None)
    watch.set_defaults(func=cmd_watch)

    status = sub.add_parser("status", help="read local observer state")
    status.set_defaults(func=cmd_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        payload, code = args.func(args)
    except BusyError as exc:
        return emit({"ok": False, "error": "busy", "message": str(exc)}, 2)
    except ValueError as exc:
        return emit({"ok": False, "error": "invalid", "message": str(exc)}, 2)
    return emit(payload, code)


if __name__ == "__main__":
    sys.exit(main())

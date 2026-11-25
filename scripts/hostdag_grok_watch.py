"""Incremental Grok ACP log observer. No scheduler and no model calls."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable


LARGE_LINE = 16384
HEAD_BYTES = 500
GROK_PROVIDERS = {"grok-cli", "grok"}


def inspect_wrapper_pid(pid: Any) -> dict[str, Any]:
    if pid is None or pid == "":
        return {"present": None, "alive": None, "looks_like_wrapper": None, "pid": None}
    try:
        value = int(pid)
    except (TypeError, ValueError):
        return {"present": False, "alive": False, "looks_like_wrapper": False, "pid": pid}
    proc = Path("/proc") / str(value)
    if not proc.is_dir():
        return {"present": False, "alive": False, "looks_like_wrapper": False, "pid": value}
    try:
        raw = (proc / "cmdline").read_bytes()
    except (OSError, PermissionError):
        return {"present": True, "alive": True, "looks_like_wrapper": None, "pid": value}
    text = raw.replace(b"\x00", b" ").decode("utf-8", "replace")
    looks = "host-dag-grok.py" in text or "/host-dag-grok " in text or text.rstrip().endswith("host-dag-grok")
    return {"present": True, "alive": True, "looks_like_wrapper": looks, "pid": value}


def classify_turn(
    stop_reason: object,
    *,
    unanswered: bool = False,
    rejected: bool = False,
    cancel_sent: bool = False,
    cancel_requested: bool = False,
) -> str:
    if unanswered:
        return "unanswered"
    if cancel_requested or cancel_sent or stop_reason == "cancelled":
        return "cancelled"
    if rejected:
        return "rejected"
    if stop_reason == "end_turn":
        return "completed"
    if stop_reason:
        return "failed"
    return "unknown"


def grok_fingerprint(snapshot: dict[str, Any]) -> str:
    blob = json.dumps(
        {
            "turnId": snapshot.get("turnId"),
            "status": snapshot.get("status"),
            "stopReason": snapshot.get("stopReason"),
            "pending_permission": bool(snapshot.get("pending_permission")),
            "permission_id": snapshot.get("permission_id"),
        },
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def event_kind(status: str) -> str | None:
    if status == "completed":
        return "completed"
    if status == "attention":
        return "attention"
    if status in {"cancelled", "rejected", "unanswered"}:
        return status
    if status in {"error", "failed"}:
        return "failed"
    if status == "unknown":
        return "unknown_status"
    return None


def tickets_from_run(data: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, Any]] = {}
    claims = data.get("claims") if isinstance(data.get("claims"), dict) else {}
    active = data.get("active_operations") if isinstance(data.get("active_operations"), dict) else {}
    for name, rec in claims.items():
        if not isinstance(rec, dict):
            continue
        executor = rec.get("executor") if isinstance(rec.get("executor"), dict) else {}
        provider = executor.get("provider") or rec.get("provider")
        if provider not in GROK_PROVIDERS:
            continue
        session = rec.get("sessionId")
        log = rec.get("log")
        if not session or not log:
            continue
        out[str(name)] = {
            "session": session,
            "log": log,
            "wrapper_pid": rec.get("wrapper_pid"),
            "evidence": rec.get("evidence") or "",
        }
    for name, rec in active.items():
        if not isinstance(rec, dict):
            continue
        session = rec.get("sessionId")
        log = rec.get("log")
        if not session or not log:
            continue
        if rec.get("threadId") and not rec.get("sessionId"):
            continue
        key = str(name)
        if key not in out:
            continue
        item = dict(out.get(key) or {})
        item["session"] = session
        item["log"] = log
        if rec.get("wrapper_pid") is not None:
            item["wrapper_pid"] = rec.get("wrapper_pid")
        if rec.get("evidence"):
            item["evidence"] = rec.get("evidence")
        out[key] = item
    return out


def apply_run_tickets(state: dict[str, Any], tickets: dict[str, dict[str, Any]], now: float) -> None:
    tracked = state.setdefault("tracked", {})
    for ticket, info in tickets.items():
        rec = dict(tracked.get(ticket) or {})
        if rec.get("thread") and rec.get("provider") not in {None, "grok"}:
            continue
        if rec.get("thread") and rec.get("session") is None and rec.get("provider") != "grok":
            continue
        changed = (
            rec.get("session") != info.get("session")
            or rec.get("log") != info.get("log")
            or rec.get("wrapper_pid") != info.get("wrapper_pid")
        )
        rec["provider"] = "grok"
        rec["session"] = info["session"]
        rec["log"] = info["log"]
        rec["wrapper_pid"] = info.get("wrapper_pid")
        if info.get("evidence"):
            rec["evidence"] = info["evidence"]
        rec.setdefault("evidence", "")
        rec.setdefault("direction_anchor", now)
        rec.setdefault("terminal", False)
        if changed:
            rec["log_offset"] = 0
            rec["log_inode"] = None
            rec["log_partial"] = ""
            rec["parser"] = {}
            rec["terminal"] = False
        tracked[ticket] = rec


def line_maybe_relevant(line: bytes) -> bool:
    if len(line) <= LARGE_LINE:
        return True
    head = line[:HEAD_BYTES]
    markers = (
        b'"direction": "observer"',
        b'"method": "session/prompt"',
        b'"method": "session/new"',
        b'"method": "session/load"',
        b'"method": "session/request_permission"',
        b'"method": "session/cancel"',
        b'"method": "_x.ai/session/prompt_complete"',
        b'"stopReason"',
    )
    return any(marker in head for marker in markers)


def _flags(parser: dict[str, Any]) -> dict[str, bool]:
    return {
        "unanswered": bool(parser.get("unanswered")),
        "rejected": bool(parser.get("rejected")),
        "cancel_sent": bool(parser.get("cancel_sent")),
        "cancel_requested": bool(parser.get("cancel_requested")),
    }


def _start_turn(parser: dict[str, Any], rpc_id: object, session_id: object) -> None:
    parser["prompt_rpc_id"] = rpc_id
    parser["prompt_id"] = None
    parser["pending_permissions"] = {}
    parser["rejected"] = False
    parser["unanswered"] = False
    parser["cancel_sent"] = False
    parser["cancel_requested"] = False
    parser["stop_reason"] = None
    parser["terminal_status"] = None
    parser["jsonrpc_error"] = None
    parser["lifecycle"] = None
    parser["process_exit"] = None
    parser["corrupt"] = False
    if session_id:
        parser["session_id"] = session_id


def _reset_connection(parser: dict[str, Any]) -> None:
    parser["prompt_rpc_id"] = None
    parser["prompt_id"] = None
    parser["pending_permissions"] = {}
    parser["rejected"] = False
    parser["unanswered"] = False
    parser["cancel_sent"] = False
    parser["cancel_requested"] = False
    parser["stop_reason"] = None
    parser["terminal_status"] = None
    parser["jsonrpc_error"] = None
    parser["lifecycle"] = None
    parser["process_exit"] = None
    parser["corrupt"] = False


def _set_terminal(parser: dict[str, Any], stop_reason: object) -> None:
    parser["stop_reason"] = stop_reason
    parser["terminal_status"] = classify_turn(stop_reason, **_flags(parser))


def _handle_permission_outcome(parser: dict[str, Any], rpc_id: object, outcome: object) -> None:
    pending = parser.setdefault("pending_permissions", {})
    pending.pop(str(rpc_id), None)
    if not isinstance(outcome, dict):
        return
    option = outcome.get("optionId")
    kind = outcome.get("outcome")
    if option == "reject-once" or kind == "reject_once":
        parser["rejected"] = True
        return
    if kind == "selected" and option in {"allow-once", "allow_once"}:
        return
    if kind == "cancelled" or option is None:
        if not parser.get("cancel_sent"):
            parser["unanswered"] = True


def _handle_observer(parser: dict[str, Any], obj: dict[str, Any]) -> None:
    event = obj.get("event")
    if event == "lifecycle":
        prompt_rpc = obj.get("promptRpcId")
        current = parser.get("prompt_rpc_id")
        if current is None:
            if obj.get("status") in {"cancelled", "failed", "unanswered"}:
                parser["lifecycle"] = obj
                parser["terminal_status"] = obj.get("status")
                parser["session_id"] = obj.get("sessionId") or parser.get("session_id")
            return
        if prompt_rpc is not None and prompt_rpc != current:
            return
        parser["lifecycle"] = obj
        if obj.get("stopReason") is not None:
            parser["stop_reason"] = obj.get("stopReason")
        parser["terminal_status"] = classify_turn(parser.get("stop_reason"), **_flags(parser))
        if parser["terminal_status"] == "unknown" and obj.get("status"):
            parser["terminal_status"] = obj.get("status")
        parser["session_id"] = obj.get("sessionId") or parser.get("session_id")
        return
    if event == "process_exit":
        parser["process_exit"] = obj


def consume_line(parser: dict[str, Any], raw: bytes) -> None:
    if not line_maybe_relevant(raw):
        return
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        parser["corrupt"] = True
        return
    if not isinstance(obj, dict):
        parser["corrupt"] = True
        return
    direction = obj.get("direction")
    if direction == "observer":
        _handle_observer(parser, obj)
        return
    msg = obj.get("message")
    if not isinstance(msg, dict):
        return
    method = msg.get("method")
    rpc_id = msg.get("id")
    params = msg.get("params") if isinstance(msg.get("params"), dict) else {}
    if direction == "send":
        if method in {"session/new", "session/load", "session/prompt", "session/set_config_option"}:
            parser.setdefault("awaiting", {})[str(rpc_id)] = method
            if method in {"session/new", "session/load"}:
                _reset_connection(parser)
                if method == "session/load":
                    parser["session_id"] = params.get("sessionId") or parser.get("session_id")
            elif method == "session/prompt":
                _start_turn(parser, rpc_id, params.get("sessionId"))
        elif method == "session/cancel":
            parser["cancel_sent"] = True
        result = msg.get("result") if isinstance(msg.get("result"), dict) else None
        if result is not None and "outcome" in result:
            _handle_permission_outcome(parser, rpc_id, result.get("outcome"))
        return
    if direction != "receive":
        return
    if method == "session/request_permission":
        tool = params.get("toolCall") if isinstance(params.get("toolCall"), dict) else {}
        parser.setdefault("pending_permissions", {})[str(rpc_id)] = tool.get("title") or "permission"
        return
    if method == "_x.ai/session/prompt_complete":
        if parser.get("prompt_rpc_id") is None:
            return
        sid = params.get("sessionId")
        if sid and parser.get("session_id") and sid != parser.get("session_id"):
            return
        prompt_id = params.get("promptId")
        if prompt_id and parser.get("prompt_id") and prompt_id != parser.get("prompt_id"):
            return
        if prompt_id:
            parser["prompt_id"] = prompt_id
        return
    if method in {"_x.ai/session_notification", "session/update"}:
        upd = params.get("update") if isinstance(params.get("update"), dict) else {}
        if upd.get("sessionUpdate") == "interaction_resolved" and parser.get("pending_permissions"):
            parser["pending_permissions"] = {}
        return
    if method:
        return
    if rpc_id is None:
        return
    awaiting = parser.setdefault("awaiting", {})
    wanted = awaiting.pop(str(rpc_id), None)
    if "error" in msg and (wanted == "session/prompt" or rpc_id == parser.get("prompt_rpc_id")):
        err = msg.get("error")
        parser["jsonrpc_error"] = err.get("message") if isinstance(err, dict) else str(err)
        parser["terminal_status"] = "error"
        return
    result = msg.get("result") if isinstance(msg.get("result"), dict) else {}
    if wanted in {"session/new", "session/load"}:
        parser["session_id"] = result.get("sessionId") or parser.get("session_id")
        return
    if wanted == "session/prompt" or rpc_id == parser.get("prompt_rpc_id"):
        if "stopReason" in result:
            meta = result.get("_meta") if isinstance(result.get("_meta"), dict) else {}
            parser["prompt_id"] = meta.get("promptId") or parser.get("prompt_id")
            _set_terminal(parser, result.get("stopReason"))


def _ours_alive(wrapper: dict[str, Any]) -> bool:
    if wrapper.get("alive") is False:
        return False
    if wrapper.get("looks_like_wrapper") is False:
        return False
    return True


def _turn_id(parser: dict[str, Any]) -> str | None:
    if parser.get("prompt_id"):
        return str(parser["prompt_id"])
    if parser.get("prompt_rpc_id") is not None:
        return f"rpc:{parser['prompt_rpc_id']}"
    return None


def _summary(status: str, parser: dict[str, Any], wrapper: dict[str, Any]) -> str:
    if status == "error":
        return f"ACP error: {parser.get('jsonrpc_error') or 'prompt failed'}"
    if status == "attention":
        pending = parser.get("pending_permissions") or {}
        title = next(iter(pending.values()), "permission")
        return f"pending permission {title}"
    if status == "unknown":
        if parser.get("corrupt"):
            return "ACP log has a truncated or invalid line; coordinator must judge"
        if not _ours_alive(wrapper):
            return "wrapper pid is not a live host-dag-grok process; log has no current-turn terminal"
        return "Grok turn ended without a readable terminal"
    stop = parser.get("stop_reason")
    turn = _turn_id(parser)
    parts = [status]
    if stop:
        parts.append(f"stopReason={stop}")
    if turn:
        parts.append(f"turn={turn}")
    return " ".join(parts)


def _finalize(parser: dict[str, Any], rec: dict[str, Any], wrapper: dict[str, Any], bytes_read: int) -> dict[str, Any]:
    pending = parser.get("pending_permissions") or {}
    status = parser.get("terminal_status")
    if not status and parser.get("jsonrpc_error"):
        status = "error"
    if not status and pending and _ours_alive(wrapper) and parser.get("prompt_rpc_id") is not None:
        status = "attention"
    if not status and parser.get("prompt_rpc_id") is not None:
        if parser.get("process_exit") or not _ours_alive(wrapper) or parser.get("corrupt"):
            status = "unknown"
        else:
            status = "running"
    if not status:
        life = parser.get("lifecycle") or {}
        if life.get("status"):
            status = life.get("status")
        elif not _ours_alive(wrapper):
            status = "unknown"
        else:
            status = "running"
    session_id = parser.get("session_id") or rec.get("session")
    permission_id = next(iter(pending.keys()), None) if pending else None
    snapshot = {
        "sessionId": session_id,
        "status": status,
        "turnId": _turn_id(parser),
        "stopReason": parser.get("stop_reason"),
        "pending_permission": bool(pending) and status == "attention",
        "permission_id": permission_id,
        "summary": _summary(status, parser, wrapper),
        "bytes_read": bytes_read,
        "wrapper": {
            "alive": wrapper.get("alive"),
            "looks_like_wrapper": wrapper.get("looks_like_wrapper"),
            "pid": wrapper.get("pid"),
        },
    }
    return snapshot


def observe_record(
    rec: dict[str, Any],
    now: float | None = None,
    pid_inspect: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    inspect = inspect_wrapper_pid if pid_inspect is None else pid_inspect
    wrapper = inspect(rec.get("wrapper_pid"))
    log_path = rec.get("log")
    parser = dict(rec.get("parser") or {})
    if not log_path:
        rec["parser"] = parser
        return _finalize(parser, rec, wrapper, 0)
    path = Path(log_path)
    if not path.is_file():
        rec["parser"] = parser
        snap = _finalize(parser, rec, wrapper, 0)
        if snap["status"] == "running":
            snap["status"] = "unknown"
            snap["summary"] = _summary("unknown", parser, wrapper)
        return snap
    stat = path.stat()
    offset = int(rec.get("log_offset") or 0)
    partial = (rec.get("log_partial") or "").encode("latin-1")
    prev_inode = rec.get("log_inode")
    if prev_inode is not None and (int(prev_inode) != stat.st_ino or stat.st_size < offset):
        offset = 0
        partial = b""
        parser = {}
    with path.open("rb") as fh:
        fh.seek(offset)
        new = fh.read()
    data = partial + new
    parts = data.split(b"\n")
    if data.endswith(b"\n") or not data:
        complete = parts[:-1] if parts and parts[-1] == b"" else parts
        leftover = b""
    else:
        leftover = parts[-1]
        complete = parts[:-1]
    rec["log_offset"] = offset + len(new)
    rec["log_partial"] = leftover.decode("latin-1") if leftover else ""
    rec["log_inode"] = stat.st_ino
    for raw in complete:
        if raw.strip():
            consume_line(parser, raw)
    rec["parser"] = parser
    rec["session"] = parser.get("session_id") or rec.get("session")
    return _finalize(parser, rec, wrapper, len(new))

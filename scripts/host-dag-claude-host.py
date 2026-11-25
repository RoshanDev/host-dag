#!/usr/bin/env python3
"""A host-dag-codex-host stand-in that runs the DAG coordinator on Claude Code.

User decision 2026-10-14: when the Codex coordinator runs out of quota, Claude takes
over unattended, without the user coming back to a terminal. The watcher only needs
`thread read|send|cancel` from its host CLI, so this script speaks that contract:

- thread ids starting with `claude-` are Claude Code sessions run headless
  (`claude -p --resume <session>`), one detached process per turn;
- every other thread id (the Codex workers) is passed through to host-dag-codex-host.

usage: host-dag-claude-host.py thread send [--effort E] (--message M | --message-file F) THREAD
       host-dag-claude-host.py thread read THREAD
       host-dag-claude-host.py thread cancel THREAD [--turn-id T]
       host-dag-claude-host.py thread new [--cwd DIR] [--model M]     # prints a fresh claude- thread id
"""
from __future__ import annotations

import json
import argparse
import fcntl
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import hostdag_token_guard as token_guard

STATE = Path(os.environ.get("HOST_DAG_CLAUDE_HOST_STATE", Path.home() / ".local/state/host-dag-claude-host"))
CODEX_HOST = os.environ.get("HOST_DAG_CODEX_HOST", str(Path.home() / ".local/bin/host-dag-codex-host"))
CLAUDE = os.environ.get("HOST_DAG_CLAUDE_BIN") or shutil.which("claude") or str(Path.home() / ".local/bin/claude")
DEFAULT_MODEL = "opus[1m]"


def emit(body: dict[str, Any], code: int = 0) -> int:
    print(json.dumps(body, ensure_ascii=False))
    return code


def error(code: str, message: str, thread: str | None = None) -> int:
    return emit({"error": {"code": code, "message": message}, "threadId": thread}, 1)


def load(thread: str) -> dict[str, Any] | None:
    try:
        return json.loads((STATE / f"{thread}.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def save(thread: str, rec: dict[str, Any]) -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    # Per-writer temp name: concurrent readers (watcher, coordinator, CLI) each refresh and save, and a
    # shared .tmp let one rename another's half-written file into place (THREAD_NOT_FOUND on read).
    tmp = STATE / f"{thread}.json.{os.getpid()}.{time.time_ns()}.tmp"
    tmp.write_text(json.dumps(rec, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(STATE / f"{thread}.json")


def alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    try:  # a finished child we never reaped is a zombie, not a live turn
        with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
            return handle.read().split(")")[-1].split()[0] != "Z"
    except OSError:
        return False


def session_path(rec: dict[str, Any]) -> str:
    project = str(Path(rec["cwd"]).resolve()).replace("/", "-")
    return str(Path.home() / ".claude/projects" / project / f"{rec['session']}.jsonl")


def refresh(rec: dict[str, Any]) -> dict[str, Any]:
    """Settle the latest turn: running while its process lives, then completed/failed from its JSON output."""
    turn = rec["turns"][-1] if rec.get("turns") else None
    if not turn or turn["status"] != "running" or alive(turn.get("pid")):
        return rec
    text, status = "", "failed"
    try:
        raw = Path(turn["out"]).read_text(encoding="utf-8")
        if not raw.strip():
            raise ValueError("empty output; the native Claude session may not have started")
        out = json.loads(raw)
        if not isinstance(out, dict) or out.get("type") != "result" or not isinstance(out.get("is_error"), bool):
            raise ValueError("output is not a Claude result with an explicit is_error boolean")
        text = str(out.get("result") or "")
        status = "failed" if out.get("is_error") else "completed"
    except (OSError, ValueError) as exc:
        text = f"Claude turn ended without valid output ({turn['out']}): {exc}"
        try:
            stderr = Path(turn["stderr"]).read_text(encoding="utf-8").strip()
            if stderr:
                text += "\nLauncher stderr: " + stderr[-2000:]
        except (KeyError, OSError):
            pass
    turn.update({"status": status, "text": text, "ended": time.time()})
    return rec


def thread_new(args: list[str]) -> int:
    cwd = args[args.index("--cwd") + 1] if "--cwd" in args else os.getcwd()
    model = args[args.index("--model") + 1] if "--model" in args else DEFAULT_MODEL
    thread = f"claude-{uuid.uuid4()}"
    parser = argparse.ArgumentParser(add_help=False)
    token_guard.add_arguments(parser)
    guard_args, _ = parser.parse_known_args(args)
    # The first send contains the contract, so bind then (not with an empty T2 contract).
    guard_options = {k: str(v) if isinstance(v, Path) else v for k, v in vars(guard_args).items()}
    save(thread, {"thread": thread, "session": str(uuid.uuid4()), "cwd": cwd, "model": model,
                  "started": False, "turns": [], 'guardOptions': guard_options})
    return emit({"threadId": thread})


def thread_send(thread: str, args: list[str]) -> int:
    if (STATE / 'readonly' / f'{thread}.json').exists():
        return error('THREAD_READ_ONLY', 'Rotated coordinator is read-only', thread)
    rec = load(thread)
    if rec is None:
        return error("THREAD_NOT_FOUND", f"unknown Claude thread {thread}", thread)
    rec = refresh(rec)
    try:
        token_guard.refuse_resume(thread, retry=bool(rec['turns']) and rec['turns'][-1]['status'] in {'failed', 'cancelled'})
    except ValueError as exc:
        return error('TOKEN_GUARD_NEW_SESSION_REQUIRED', str(exc), thread)
    if rec["turns"] and rec["turns"][-1]["status"] == "running":
        save(thread, rec)
        return error("THREAD_BUSY", "Thread has an active turn; message was not submitted", thread)
    if "--message-file" in args:
        message = Path(args[args.index("--message-file") + 1]).read_text(encoding="utf-8")
    elif "--message" in args:
        message = args[args.index("--message") + 1]
    else:
        return error("BAD_REQUEST", "send needs --message or --message-file", thread)
    if not rec['started']:
        try:
            config, message = token_guard.prepare(argparse.Namespace(**rec.get('guardOptions', {})), message)
        except (OSError, ValueError) as exc:
            return error('TOKEN_GUARD_BAD_HANDOFF', str(exc), thread)
    else:
        config = None
    scoped = shutil.which("systemd-run") and os.environ.get("HOST_DAG_CLAUDE_HOST_SCOPE", "1") != "0"
    if scoped:
        missing = [key for key in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS") if not os.environ.get(key)]
        if missing:
            return error("USER_BUS_UNAVAILABLE", "systemd-run --user requires " + ", ".join(missing)
                         + "; message was not submitted", thread)
    turn_id = f"{time.time_ns():x}-{uuid.uuid4().hex[:8]}"  # time-ordered like Codex turn ids
    (STATE / "turns").mkdir(parents=True, exist_ok=True)
    out = STATE / "turns" / f"{thread}.{turn_id}.out.json"
    # The input is kept so a turn lost to a usage limit (429) can be sent again after the reset
    # (2026-10-19 16:06/16:09: two watcher packets were lost that way).
    inp = out.with_name(f"{thread}.{turn_id}.in.txt")
    inp.write_text(message, encoding="utf-8")
    os.chmod(inp, 0o600)
    session = ["--resume", rec["session"]] if rec["started"] else ["--session-id", rec["session"]]
    argv = [CLAUDE, "-p", "--model", rec["model"], "--permission-mode", "bypassPermissions",
            "--output-format", "json", *session,
            # A headless turn that schedules its own wakeup stays alive until it fires, so the thread
            # reads busy and watcher events queue behind it (10-17 16:09: an hour idle, page window unused).
            # The watcher and WAIT-UNTIL blockers already bring the coordinator back.
            "--disallowedTools", "ScheduleWakeup,CronCreate,CronDelete,RemoteTrigger"]
    err = out.with_suffix('.stderr.log')
    guarded = config is not None or token_guard.binding(thread) is not None
    if guarded:
        log = out.with_suffix('.stream.jsonl')
        spec_file = out.with_suffix('.guard.json')
        exclude = [out, inp, err, log, spec_file]
        if config:
            guard = token_guard.Guard.create(identity=thread, cwd=rec['cwd'], contract=message,
                                             publish=True, exclude=exclude, **config)
            mode = guard.state['mode']
        else:
            with token_guard.locked(thread) as guard:
                guard.state['exclude'].extend(map(str, exclude))
                # New stream file, same Claude session: keep total input/seen message ids.
                guard.state.setdefault('usage', {}).pop('result', None)
                mode = guard.state['mode']
        argv[argv.index('--output-format') + 1] = 'stream-json'
        argv += ['--verbose']
        if mode == 'enforce':
            argv += ['--input-format', 'stream-json']
        token_guard.write_json(spec_file, dict(argv=argv, cwd=rec['cwd'], prompt=str(inp), log=str(log),
                                               stderr=str(err), identity=thread, stream_input=mode == 'enforce'))
        argv = [sys.executable, str(Path(token_guard.__file__).resolve()), str(spec_file)]
    # Callers are systemd units (the watcher, the coordinator quota check); a turn left in their cgroup
    # died when they stopped or restarted (10-16 18:13 takeover: two empty turns). Give it its own scope.
    if scoped:
        argv = ["systemd-run", "--user", "--scope", "--quiet", "--collect",
                f"--unit=host-dag-claude-turn-{turn_id}", *argv]
    with open(out, "w", encoding="utf-8") as stdout, open(err, "w", encoding="utf-8") as stderr:
        proc = subprocess.Popen(argv, cwd=rec["cwd"], stdin=subprocess.PIPE, stdout=stdout,
                                stderr=stderr, start_new_session=True, text=True)
        assert proc.stdin is not None
        if not guarded:
            proc.stdin.write(message)
        proc.stdin.close()
    rec["started"] = True
    rec["turns"].append({"id": turn_id, "pid": proc.pid, "out": str(out), "stderr": str(err),
                         "status": "running", "started": time.time()})
    save(thread, rec)
    return emit({"threadId": thread, "turn": {"id": turn_id, "status": "inProgress"}})


def thread_read(thread: str) -> int:
    rec = load(thread)
    if rec is None:
        return error("THREAD_NOT_FOUND", f"unknown Claude thread {thread}", thread)
    rec = refresh(rec)
    save(thread, rec)
    turn = rec["turns"][-1] if rec["turns"] else {"id": None, "status": "completed", "text": ""}
    status = "running" if turn["status"] == "running" else turn["status"]
    with token_guard.locked(thread) as guard:
        if guard and status in {'failed', 'cancelled'}:
            guard.failed()
        if guard and guard.blocked:
            status = 'attention'
            turn['text'] = 'TOKEN_GUARD_NEW_SESSION_REQUIRED: ' + guard.instruction()
    return emit({"threadId": thread, "status": status, "model": rec["model"], "cwd": rec["cwd"],
                 "path": session_path(rec), "nativeStatus": {"type": "active" if status == "running" else "idle"},
                 "result": {"turnId": turn["id"], "text": "" if status == "running" else turn.get("text", ""),
                            "availability": "available"}})


def thread_cancel(thread: str, args: list[str]) -> int:
    rec = load(thread)
    if rec is None:
        return error("THREAD_NOT_FOUND", f"unknown Claude thread {thread}", thread)
    want = args[args.index("--turn-id") + 1] if "--turn-id" in args else None
    turn = rec["turns"][-1] if rec["turns"] else None
    if turn and turn["status"] == "running" and (want is None or want == turn["id"]):
        try:
            os.killpg(turn["pid"], signal.SIGTERM)
        except OSError:
            pass
        turn.update({"status": "cancelled", "text": "", "ended": time.time()})
        save(thread, rec)
    return emit({"threadId": thread, "status": "cancelled",
                 "result": {"turnId": turn["id"] if turn else None, "text": "", "availability": "available"}})


def thread_phase(thread: str, args: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--phase', choices=['implement', 'deploy', 'acceptance'], required=True)
    parser.add_argument('--context-fraction', type=float, required=True)
    values, _ = parser.parse_known_args(args)
    rec = load(thread)
    if rec is None:
        return error('THREAD_NOT_FOUND', f'unknown Claude thread {thread}', thread)
    rec = refresh(rec)
    terminal = not rec['turns'] or rec['turns'][-1]['status'] != 'running'
    result = token_guard.set_phase(thread, values.phase, values.context_fraction, terminal=terminal)
    return emit({'threadId': thread, 'phase': values.phase, **result})


def thread_read_only(thread: str, *, rollback: bool = False) -> int:
    rec = load(thread)
    if rec is None:
        return error('THREAD_NOT_FOUND', 'Unknown coordinator', thread)
    rec = refresh(rec)
    if rec.get('turns') and rec['turns'][-1]['status'] == 'running':
        return error('THREAD_BUSY', 'Cannot retire an active coordinator', thread)
    marker = STATE / 'readonly' / f'{thread}.json'
    if rollback:
        marker.unlink(missing_ok=True)
    else:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(json.dumps({'at': time.time()}))
    return emit({'threadId': thread, 'read_only': not rollback})


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[0] == "thread" and argv[1] == "new":
        return thread_new(argv[2:])
    thread = next((a for a in argv[2:] if a.startswith("claude-")), None)
    if thread is None:  # Codex workers and everything else keep using the Codex host
        return subprocess.run([CODEX_HOST, *argv], check=False).returncode
    if argv[:2] in (["thread", "send"], ['thread', 'read-only']):
        STATE.mkdir(parents=True, exist_ok=True)
        with (STATE / f'{thread}.send.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return error('THREAD_BUSY', 'Another send is in progress', thread)
            if argv[1] == 'read-only':
                return thread_read_only(thread, rollback='--rotation-rollback' in argv)
            return thread_send(thread, argv[2:])
    if argv[:2] == ["thread", "read"]:
        return thread_read(thread)
    if argv[:2] == ["thread", "cancel"]:
        return thread_cancel(thread, argv[2:])
    if argv[:2] == ['thread', 'phase']:
        return thread_phase(thread, argv[2:])
    if argv[:2] == ["goal", "pause"]:
        return emit({"threadId": thread, "status": "noop"})
    return error("UNSUPPORTED", f"{' '.join(argv[:2])} is not supported for Claude threads", thread)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

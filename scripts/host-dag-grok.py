#!/usr/bin/python3
"""Foreground Grok ACP client for Host DAG.

One child: grok agent --model --effort [--always-approve] stdio
(start_new_session=True). --permission-mode bypassPermissions maps to ACP
--always-approve. default keeps stdin allow-once / reject-once.
SIGINT sets a cancel flag, unblocks permission wait, and either stops
before prompt or sends session/cancel and waits for that turn's stopReason.
stdio is ACP only; Codex thread protocol is rejected.
"""
from __future__ import annotations

import argparse
import json
import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path


DEFAULT_GROK = Path.home() / ".grok/bin/grok"
EFFORT_RANK = {"low": 1, "medium": 2, "high": 3, "xhigh": 4}
DECISIONS = {"allow-once": "allow_once", "reject-once": "reject_once"}
PERMISSION_MODES = ("default", "bypassPermissions")
CANCEL_WAIT = 25


class UsageError(Exception):
    def __init__(self, code, message, **extra):
        self.payload = {"error": {"code": code, "message": message, **extra}}
        super().__init__(message)


def grok_bin():
    return Path(os.environ.get("HOST_DAG_GROK_BIN", str(DEFAULT_GROK)))


def require_grok():
    path = grok_bin()
    if not path.is_file():
        raise UsageError("GROK_BIN_MISSING", "Official grok binary not found", grokBin=str(path))
    return path


def emit(payload, stream=sys.stdout):
    json.dump(payload, stream, ensure_ascii=False)
    stream.write("\n")
    stream.flush()


def write_observer(log_fp, event, **fields):
    if log_fp is None:
        return
    payload = {"direction": "observer", "event": event}
    payload.update(fields)
    log_fp.write(json.dumps(payload, ensure_ascii=False) + "\n")
    log_fp.flush()


def inspect_argv(grok, cwd):
    return [str(grok), "--cwd", str(cwd), "inspect", "--json"]


def agent_argv(grok, model, effort, permission_mode="default"):
    argv = [str(grok), "agent", "--model", model, "--effort", effort]
    if permission_mode == "bypassPermissions":
        argv.append("--always-approve")
    argv.append("stdio")
    return argv


def effort_ids(efforts):
    return [item.get("id") or item.get("value") for item in efforts if item.get("id") or item.get("value")]


def advertised_highest_effort(efforts):
    ids = effort_ids(efforts)
    if not ids:
        return None
    return max(ids, key=lambda name: EFFORT_RANK.get(name, 0))


def confirm_model_effort(model_state, model, effort):
    models = (model_state or {}).get("availableModels") or []
    record = next((item for item in models if item.get("modelId") == model), None)
    if record is None:
        raise UsageError("MODEL_UNAVAILABLE", "Requested model is not in official modelState", model=model)
    meta = record.get("_meta") or {}
    efforts = meta.get("reasoningEfforts") or []
    ids = effort_ids(efforts)
    highest = advertised_highest_effort(efforts)
    if effort not in ids:
        raise UsageError(
            "EFFORT_UNAVAILABLE",
            "Requested effort is not advertised for this model; not using a mutable default",
            model=model, effort=effort, advertisedHighestEffort=highest, advertisedEfforts=ids,
        )
    return {
        "model": model,
        "effort": effort,
        "advertisedHighestEffort": highest,
        "totalContextTokens": meta.get("totalContextTokens"),
    }


def config_current(options, config_id):
    for item in options or []:
        if item.get("id") == config_id:
            return item.get("currentValue")
    return None


def preview_permission(message):
    params = dict(message.get("params") or {})
    tool = dict(params.get("toolCall") or {})
    raw = {}
    for key, value in (tool.get("rawInput") or {}).items():
        if isinstance(value, str) and len(value) > 250:
            raw[key] = value[:250] + "... [full proposal in ACP log]"
        else:
            raw[key] = value
    tool["rawInput"] = raw
    params["toolCall"] = tool
    return {**message, "params": params}


def session_params(cwd, session_id=None):
    params = {"cwd": str(cwd), "mcpServers": []}
    if session_id:
        params["sessionId"] = session_id
    return params


class AcpClient:
    def __init__(self, proc, log_fp):
        self.proc = proc
        self.log = log_fp
        self.buf = b""
        self.next_id = 0
        self.session_id = None
        self.phase = "start"
        self.prompt_sent = False
        self.cancel_requested = False
        self.cancel_sent = False
        self.cancel_at = None
        self.unanswered = False
        self.rejected = False
        self.decision_buf = b""
        self.decision_lines = []

    def send(self, payload):
        self.log.write(json.dumps({"direction": "send", "message": payload}, ensure_ascii=False) + "\n")
        self.log.flush()
        self.proc.stdin.write((json.dumps(payload) + "\n").encode())
        self.proc.stdin.flush()

    def request(self, method, params):
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params})
        return self.next_id

    def note_cancel(self, *_args):
        self.cancel_requested = True

    def maybe_send_cancel(self):
        if not self.cancel_requested or self.cancel_sent or not self.session_id or not self.prompt_sent:
            return
        self.send({"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": self.session_id}})
        self.cancel_sent = True
        self.cancel_at = time.monotonic()
        print("CANCEL_SENT", self.session_id, flush=True)

    def discard_decision_input(self):
        self.decision_buf = b""
        self.decision_lines = []

    def poll_decision(self):
        print("DECISION (allow-once / reject-once): ", end="", flush=True)
        stdin_fd = sys.stdin.fileno()
        while True:
            if self.cancel_requested:
                self.discard_decision_input()
                return False
            self.maybe_send_cancel()
            if self.decision_lines:
                return self.decision_lines.pop(0)
            try:
                ready = select.select([stdin_fd], [], [], 0.1)[0]
            except InterruptedError:
                continue
            if not ready:
                continue
            try:
                chunk = os.read(stdin_fd, 4096)
            except InterruptedError:
                continue
            if chunk == b"":
                self.discard_decision_input()
                return None
            self.decision_buf += chunk
            while b"\n" in self.decision_buf:
                raw, self.decision_buf = self.decision_buf.split(b"\n", 1)
                self.decision_lines.append(raw.decode("utf-8", "replace").strip())

    def answer_permission(self, message):
        print("PERMISSION", json.dumps(preview_permission(message), ensure_ascii=False), flush=True)
        answer = self.poll_decision()
        if answer is False or self.cancel_requested:
            self.discard_decision_input()
            self.send({"jsonrpc": "2.0", "id": message["id"], "result": {"outcome": {"outcome": "cancelled"}}})
            return
        kind = DECISIONS.get(answer or "")
        options = (message.get("params") or {}).get("options") or []
        selected = next((item for item in options if item.get("kind") == kind or item.get("optionId") == answer), None)
        if self.cancel_requested:
            self.discard_decision_input()
            self.send({"jsonrpc": "2.0", "id": message["id"], "result": {"outcome": {"outcome": "cancelled"}}})
            return
        if selected is None:
            self.unanswered = True
            outcome = {"outcome": "cancelled"}
        else:
            if kind == "reject_once" or answer == "reject-once":
                self.rejected = True
            outcome = {"outcome": "selected", "optionId": selected["optionId"]}
        if self.cancel_requested:
            self.discard_decision_input()
            outcome = {"outcome": "cancelled"}
        self.send({"jsonrpc": "2.0", "id": message["id"], "result": {"outcome": outcome}})

    def receive(self, want):
        while self.proc.poll() is None:
            self.maybe_send_cancel()
            if self.cancel_requested and not self.prompt_sent:
                return None
            if self.cancel_at and time.monotonic() - self.cancel_at > CANCEL_WAIT:
                raise TimeoutError("Cancellation terminal response not observed")
            try:
                ready = select.select([self.proc.stdout], [], [], 0.1)[0]
            except InterruptedError:
                continue
            if not ready:
                continue
            raw = os.read(self.proc.stdout.fileno(), 65536)
            if not raw:
                break
            self.buf += raw
            while b"\n" in self.buf:
                line, self.buf = self.buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except ValueError:
                    continue
                self.log.write(json.dumps({"direction": "receive", "message": payload}, ensure_ascii=False) + "\n")
                self.log.flush()
                if payload.get("method") == "session/request_permission":
                    self.answer_permission(payload)
                    continue
                if payload.get("id") == want and "method" not in payload:
                    if "error" in payload:
                        raise RuntimeError(payload["error"])
                    return payload.get("result") or {}
        raise RuntimeError("ACP process ended without response")


def classify(stop_reason, client):
    if client.unanswered:
        return "unanswered"
    if client.cancel_requested or client.cancel_sent or stop_reason == "cancelled":
        return "cancelled"
    if client.rejected:
        return "rejected"
    if stop_reason == "end_turn":
        return "completed"
    return "failed"


def cleanup(proc):
    if proc.stdin:
        try:
            proc.stdin.close()
        except OSError:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()
        proc.wait(timeout=5)


def apply_session_effort(client, session, requested):
    options = session.get("configOptions") or []
    actual = config_current(options, "reasoning_effort")
    if actual == requested or client.cancel_requested:
        return actual
    result = client.receive(client.request("session/set_config_option", {
        "sessionId": client.session_id,
        "configId": "reasoning_effort",
        "value": requested,
    }))
    if result is None:
        return actual
    confirmed = config_current(result.get("configOptions"), "reasoning_effort")
    if confirmed != requested:
        raise UsageError(
            "EFFORT_MISMATCH",
            "session reasoning_effort did not accept the requested value",
            requested=requested, actual=confirmed,
        )
    return confirmed


def result_payload(status, args, client, proc, *, effort, stop_reason, extra=None):
    payload = {
        "status": status,
        "phase": client.phase,
        "sessionId": client.session_id,
        "model": args.model,
        "effort": effort,
        "permissionMode": args.permission_mode,
        "stopReason": stop_reason,
        "wrapperPid": os.getpid(),
        "grokPid": proc.pid,
    }
    if extra:
        payload.update(extra)
    return payload


def emit_local_cancel(args, client, proc, effort, extra, log_fp=None):
    payload = result_payload("cancelled", args, client, proc, effort=effort, stop_reason=None, extra=extra)
    write_observer(
        log_fp, "lifecycle",
        status="cancelled", sessionId=client.session_id, promptRpcId=None,
        stopReason=None, phase=client.phase, wrapperPid=os.getpid(), grokPid=proc.pid,
    )
    print("LOCAL_CANCEL", json.dumps(payload, ensure_ascii=False), flush=True)
    print("RESULT", json.dumps(payload, ensure_ascii=False), flush=True)
    return 1


def run_session(args):
    grok = require_grok()
    prompt = args.prompt_file
    if not prompt.is_file():
        raise UsageError("PROMPT_MISSING", "Prompt file not found", promptFile=str(prompt))
    cwd = args.cwd.resolve()
    log_path = args.log
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fp = log_path.open("a", buffering=1, encoding="utf-8")
    err_fp = Path(str(log_path) + ".stderr").open("a", encoding="utf-8")
    env = os.environ.copy()
    env.update(GROK_MEMORY="0", GROK_CLAUDE_HOOKS_ENABLED="0", GROK_CURSOR_HOOKS_ENABLED="0")
    proc = subprocess.Popen(
        agent_argv(grok, args.model, args.effort, args.permission_mode),
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=err_fp,
        env=env, cwd=str(cwd), bufsize=0, start_new_session=True,
    )
    client = AcpClient(proc, log_fp)
    signal.signal(signal.SIGINT, client.note_cancel)
    print("WRAPPER_PID", os.getpid(), flush=True)
    print("GROK_PID", proc.pid, flush=True)
    binding = None
    actual_effort = None
    prompt_id = None
    try:
        init = client.receive(client.request("initialize", {
            "protocolVersion": 1,
            "clientCapabilities": {},
            "clientInfo": {"name": "host-dag-grok", "version": "1"},
        }))
        if client.cancel_requested:
            return emit_local_cancel(args, client, proc, actual_effort, None, log_fp)
        model_state = (init.get("_meta") or {}).get("modelState") or {}
        binding = confirm_model_effort(model_state, args.model, args.effort)
        print("INITIALIZED", json.dumps(model_state, ensure_ascii=False), flush=True)
        print("ADVERTISED_HIGHEST_EFFORT", binding["advertisedHighestEffort"], flush=True)
        auth = client.receive(client.request("authenticate", {"methodId": "cached_token", "_meta": {"headless": True}}))
        if client.cancel_requested or auth is None:
            return emit_local_cancel(args, client, proc, actual_effort, {
                "advertisedHighestEffort": binding["advertisedHighestEffort"],
                "totalContextTokens": binding["totalContextTokens"],
            }, log_fp)
        if args.command == "resume":
            session = client.receive(client.request("session/load", session_params(cwd, args.session_id)))
        else:
            session = client.receive(client.request("session/new", session_params(cwd)))
        if client.cancel_requested or session is None:
            return emit_local_cancel(args, client, proc, actual_effort, {
                "advertisedHighestEffort": binding["advertisedHighestEffort"],
                "totalContextTokens": binding["totalContextTokens"],
            }, log_fp)
        client.session_id = getattr(args, "session_id", None) or session.get("sessionId")
        client.phase = "session"
        current = ((session.get("models") or {}).get("currentModelId") or model_state.get("currentModelId"))
        if current and current != args.model:
            raise UsageError("MODEL_MISMATCH", "session model differs from requested --model", requested=args.model, current=current)
        print("SESSION", client.session_id, flush=True)
        actual_effort = apply_session_effort(client, session, args.effort)
        extra = {
            "advertisedHighestEffort": binding["advertisedHighestEffort"],
            "totalContextTokens": binding["totalContextTokens"],
        }
        if client.cancel_requested:
            return emit_local_cancel(args, client, proc, actual_effort, extra, log_fp)
        client.phase = "prompt"
        client.prompt_sent = True
        prompt_id = client.request("session/prompt", {
            "sessionId": client.session_id,
            "prompt": [{"type": "text", "text": prompt.read_text(encoding="utf-8")}],
        })
        print("PROMPT_WAITING", flush=True)
        terminal = client.receive(prompt_id)
        stop_reason = (terminal or {}).get("stopReason")
        print("TERMINAL", json.dumps(terminal, ensure_ascii=False), flush=True)
        status = classify(stop_reason, client)
        payload = result_payload(status, args, client, proc, effort=actual_effort, stop_reason=stop_reason, extra=extra)
        write_observer(
            log_fp, "lifecycle",
            status=status, sessionId=client.session_id, promptRpcId=prompt_id,
            stopReason=stop_reason, phase=client.phase,
            wrapperPid=os.getpid(), grokPid=proc.pid,
        )
        print("RESULT", json.dumps(payload, ensure_ascii=False), flush=True)
        return 0 if status == "completed" else 1
    finally:
        cleanup(proc)
        write_observer(
            log_fp, "process_exit",
            sessionId=client.session_id, returncode=proc.returncode,
            wrapperPid=os.getpid(), grokPid=proc.pid,
            phase=client.phase, promptSent=client.prompt_sent,
            promptRpcId=prompt_id,
        )
        print("ACP_EXIT", proc.returncode, flush=True)
        log_fp.close()
        err_fp.close()


def parse_args(argv):
    if argv and argv[0] == "thread":
        raise UsageError("WRONG_HOST", "Codex threads use host-dag-codex-host; this entry speaks Grok ACP sessionId only")
    parser = argparse.ArgumentParser(prog="host-dag-grok", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--cwd", type=Path, default=Path.cwd())
    run = sub.add_parser("run")
    run.add_argument("--cwd", required=True, type=Path)
    run.add_argument("--model", required=True)
    run.add_argument("--effort", required=True)
    run.add_argument("--prompt-file", required=True, type=Path)
    run.add_argument("--log", required=True, type=Path)
    run.add_argument("--permission-mode", choices=PERMISSION_MODES, default="default")
    resume = sub.add_parser("resume")
    resume.add_argument("--cwd", required=True, type=Path)
    resume.add_argument("--model", required=True)
    resume.add_argument("--effort", required=True)
    resume.add_argument("--session-id", required=True)
    resume.add_argument("--prompt-file", required=True, type=Path)
    resume.add_argument("--log", required=True, type=Path)
    resume.add_argument("--permission-mode", choices=PERMISSION_MODES, default="default")
    return parser.parse_args(argv)


def dispatch(args):
    if args.command == "inspect":
        grok = require_grok()
        return subprocess.run(inspect_argv(grok, args.cwd.resolve())).returncode
    return run_session(args)


def main(argv=None):
    try:
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True)
        return dispatch(parse_args(sys.argv[1:] if argv is None else argv))
    except UsageError as error:
        emit(error.payload)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

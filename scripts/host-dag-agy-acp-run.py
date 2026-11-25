#!/usr/bin/env python3
"""Run one agy executor turn through Google's official Antigravity ACP server instead of `agy -p`.

11-23: the user signed in the official antigravity-acp (oauth-personal, ~/.gemini/antigravity-acp/acp_token.json).
This wrapper starts the unmodified agy_acp_server.par, opens a session in the ticket worktree, selects the model,
switches the session to the yolo mode (the ACP equivalent of --dangerously-skip-permissions), sends the prompt
once and stops when the prompt returns. Same arguments and outputs as host-dag-agy-run.py, so dispatch and the
watcher (cli-run.json finished/exit_code) are unchanged; the board reads cli.ndjson with GrokTimeline, because
the log uses the same {"direction", "message"} ACP line format as the Grok wrapper, plus a `ts` per line.

Exit codes: 0 end_turn, 1 other stop reason or JSON-RPC error, 75 quota/rate limit (429, RESOURCE_EXHAUSTED);
a quota stop also writes "quota": true into cli-run.json so the coordinator stops agy units and alerts.

Usage (inside a systemd user unit named host-dag-cli-agy-<TICKET>-MMDDHHMMSS):
  host-dag-agy-acp-run.py --unit <unit> --out <evidence dir> --cwd <worktree> --model gemini-4-neon-high \
      [--add-dir DIR ...] --prompt-file <file>
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

PAR = os.environ.get("HOST_DAG_AGY_ACP",
                     str(Path.home() / ".local/share/agy-acp-server/1.5.2/agy_acp_server.par"))
QUOTA = re.compile(r"\b429\b|RESOURCE_EXHAUSTED|quota|rate.?limit", re.I)


class Acp:
    def __init__(self, cwd: Path, log: Path, err: Path, tmp: Path) -> None:
        env = {**os.environ, "TMPDIR": str(tmp)}  # the par writes its glog files and cacert copies to TMPDIR
        self.proc = subprocess.Popen([PAR, "--uid="], cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=open(err, "ab"), text=True, bufsize=1, env=env)
        self.sink = open(log, "a", encoding="utf-8")
        self.next_id = 0

    def _log(self, direction: str, msg: dict[str, Any]) -> None:
        self.sink.write(json.dumps({"direction": direction, "ts": time.time(), "message": msg},
                                   ensure_ascii=False) + "\n")
        self.sink.flush()

    def observer(self, **rec: Any) -> None:
        self.sink.write(json.dumps({"direction": "observer", "ts": time.time(), **rec}, ensure_ascii=False) + "\n")
        self.sink.flush()

    def _send(self, msg: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self._log("out", msg)
        self.proc.stdin.write(json.dumps(msg, ensure_ascii=False) + "\n")
        self.proc.stdin.flush()

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send one request and serve the agent's own requests until its response arrives."""
        self.next_id += 1
        rid = self.next_id
        self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                self.observer(event="raw", text=line[:2000])
                continue
            self._log("in", msg)
            if "method" in msg and "id" in msg:
                self._answer(msg)
            elif msg.get("id") == rid:
                return msg
        return {"error": {"code": -1, "message": f"ACP server exited ({self.proc.wait()}) during {method}"}}

    def _answer(self, req: dict[str, Any]) -> None:
        if req["method"] == "session/request_permission":
            opts = (req.get("params") or {}).get("options") or []
            pick = next((o for kind in ("allow_always", "allow_once") for o in opts if o.get("kind") == kind), None)
            result = ({"outcome": {"outcome": "selected", "optionId": pick["optionId"]}} if pick
                      else {"outcome": {"outcome": "cancelled"}})
            self._send({"jsonrpc": "2.0", "id": req["id"], "result": result})
        else:  # no fs/terminal client capabilities were advertised
            self._send({"jsonrpc": "2.0", "id": req["id"],
                        "error": {"code": -32601, "message": f"method not supported: {req['method']}"}})

    def close(self) -> None:
        try:
            self.proc.terminate()
            self.proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        self.sink.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--unit", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--cwd", required=True, type=Path)
    ap.add_argument("--model", required=True)
    ap.add_argument("--add-dir", action="append", default=[])
    ap.add_argument("--prompt-file", required=True, type=Path)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    # TMPDIR is inherited by every tool the agent runs (go test binaries reach
    # 100+ MB), so keep it on the local SSD cache, never in the evidence dir.
    tmp = Path.home() / ".cache" / "host-dag-build-out" / "acp-tmp" / args.unit
    tmp.mkdir(parents=True, exist_ok=True)
    log = args.out / "cli.ndjson"
    run: dict[str, Any] = {"unit": args.unit, "provider": "agy", "transport": "acp", "model": args.model,
                           "cwd": str(args.cwd), "log": str(log), "prompt_file": str(args.prompt_file),
                           "started": time.time()}
    run_file = args.out / "cli-run.json"
    run_file.write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")

    acp = Acp(args.cwd, log, args.out / "agy.log", tmp)
    code, reason, err = 1, "", ""
    try:
        steps = [("initialize", lambda: {"protocolVersion": 1, "clientCapabilities": {}}),
                 ("session/new", lambda: {"cwd": str(args.cwd), "mcpServers": [],
                                          **({"additionalDirectories": args.add_dir} if args.add_dir else {})})]
        sid = ""
        for method, params in steps:
            resp = acp.call(method, params())
            if "error" in resp:
                err = f"{method}: {resp['error'].get('message')}"
                break
            if method == "session/new":
                sid = resp["result"]["sessionId"]
                run["sessionId"] = sid
        if sid:
            for method, params in (("session/set_model", {"sessionId": sid, "modelId": args.model}),
                                   ("session/set_mode", {"sessionId": sid, "modeId": "yolo"})):
                resp = acp.call(method, params)
                if "error" in resp:
                    err = f"{method}: {resp['error'].get('message')}"
                    break
        if sid and not err:
            resp = acp.call("session/prompt", {"sessionId": sid, "prompt": [
                {"type": "text", "text": args.prompt_file.read_text()}]})
            if "error" in resp:
                err = f"session/prompt: {json.dumps(resp['error'], ensure_ascii=False)[:1000]}"
            else:
                result = resp.get("result") or {}
                reason = str(result.get("stopReason") or "")
                run["usage"] = result.get("usage")
                code = 0 if reason == "end_turn" else 1
        if err:
            code = 75 if QUOTA.search(err) else 1
            run["error"] = err
            run["quota"] = code == 75
        acp.observer(event="lifecycle", status="completed" if code == 0 else "failed",
                     stopReason=reason or err[:200])
    except Exception as exc:  # noqa: BLE001 - any crash must still leave exit_code in cli-run.json for the watcher
        code, err = 1, f"wrapper error: {exc!r}"[:1000]
        run["error"] = err
        acp.observer(event="lifecycle", status="failed", stopReason=err[:200])
    finally:
        acp.close()
    run.update(finished=time.time(), exit_code=code, stopReason=reason)
    run_file.write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())

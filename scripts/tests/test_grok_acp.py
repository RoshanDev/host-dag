"""Install recovery and Grok ACP session behavior. Does not call a live model."""
from __future__ import annotations

import importlib.util
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
ENTRY = ROOT / "scripts/host-dag-grok.py"

FAKE_GROK = r'''#!/usr/bin/python3
import json, os, sys
open(os.environ["HOST_DAG_FAKE_ARGV"], "w", encoding="utf-8").write(json.dumps(sys.argv))
if "inspect" in sys.argv:
    json.dump({"skills": [{"name": "host-dag"}, {"name": "dag"}]}, sys.stdout)
    sys.exit(0)
rx = open(os.environ["HOST_DAG_FAKE_RX"], "a", encoding="utf-8")
models = {"currentModelId": "grok-4.6", "availableModels": [{
    "modelId": "grok-4.6", "_meta": {
        "totalContextTokens": 500000,
        "reasoningEffort": "high",
        "reasoningEfforts": [
            {"id": "xhigh", "value": "xhigh", "default": False},
            {"id": "high", "value": "high", "default": True},
        ],
    }
}]}
session_effort = os.environ.get("HOST_DAG_FAKE_SESSION_EFFORT", "xhigh")
pending = None
cancelled = False

def options(effort):
    return [
        {"id": "model", "currentValue": "grok-4.6"},
        {"id": "reasoning_effort", "currentValue": effort,
         "options": [{"value": "xhigh"}, {"value": "high"}]},
    ]

def reply(msg_id, result):
    print(json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": result}), flush=True)

while True:
    line = sys.stdin.readline()
    if not line:
        break
    msg = json.loads(line)
    rx.write(json.dumps(msg) + "\n"); rx.flush()
    method = msg.get("method")
    if method == "initialize":
        if os.environ.get("HOST_DAG_FAKE_HANG_INIT"):
            continue
        reply(msg["id"], {"_meta": {"modelState": models}})
    elif method == "authenticate":
        reply(msg["id"], {})
    elif method == "session/new":
        reply(msg["id"], {"sessionId": "sess-new", "models": models, "configOptions": options(session_effort)})
    elif method == "session/load":
        reply(msg["id"], {"sessionId": msg["params"]["sessionId"], "models": models, "configOptions": options(session_effort)})
    elif method == "session/set_config_option":
        if msg["params"].get("configId") == "reasoning_effort":
            session_effort = msg["params"]["value"]
        reply(msg["id"], {"configOptions": options(session_effort)})
    elif method == "session/prompt":
        pending = msg["id"]
        if cancelled:
            reply(pending, {"stopReason": "cancelled"}); pending = None; continue
        if os.environ.get("HOST_DAG_FAKE_PERMISSION"):
            print(json.dumps({"jsonrpc": "2.0", "id": 0, "method": "session/request_permission", "params": {
                "sessionId": msg["params"]["sessionId"],
                "toolCall": {"title": "write marker", "rawInput": {"content": "x"}},
                "options": [
                    {"optionId": "allow-once", "kind": "allow_once"},
                    {"optionId": "reject-once", "kind": "reject_once"},
                ],
            }}), flush=True)
            continue
        if os.environ.get("HOST_DAG_FAKE_HANG"):
            continue
        reply(pending, {"stopReason": "end_turn"}); pending = None
    elif method == "session/cancel":
        cancelled = True
        if pending is not None:
            reply(pending, {"stopReason": "cancelled"}); pending = None
    elif "id" in msg and "method" not in msg and pending is not None:
        outcome = (msg.get("result") or {}).get("outcome") or {}
        option = outcome.get("optionId")
        if option == "allow-once":
            reason = "end_turn"
        elif option == "reject-once":
            reason = "end_turn"
        else:
            reason = "cancelled"
        reply(pending, {"stopReason": reason}); pending = None
'''


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


grok = load(ENTRY, "hostdag_grok")


def flags(**kwargs):
    state = SimpleNamespace(unanswered=False, cancel_requested=False, cancel_sent=False, rejected=False)
    for key, value in kwargs.items():
        setattr(state, key, value)
    return state


class EntryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="host-dag-grok-cli-")
        self.addCleanup(self.tmp.cleanup)
        self.cwd = Path(self.tmp.name)
        self.prompt = self.cwd / "contract.txt"
        self.prompt.write_text("ticket", encoding="utf-8")
        self.log = self.cwd / "acp.ndjson"
        self.fake = self.cwd / "fake-grok"
        self.argv_file = self.cwd / "argv.json"
        self.rx_file = self.cwd / "rx.ndjson"
        self.fake.write_text(FAKE_GROK, encoding="utf-8")
        self.fake.chmod(self.fake.stat().st_mode | stat.S_IXUSR)
        self.env = dict(os.environ, HOST_DAG_GROK_BIN=str(self.fake),
                        HOST_DAG_FAKE_ARGV=str(self.argv_file),
                        HOST_DAG_FAKE_RX=str(self.rx_file),
                        PYTHONUNBUFFERED="1")

    def run_args(self, command="run"):
        args = [command, "--cwd", str(self.cwd), "--model", "grok-4.6", "--effort", "xhigh",
                "--prompt-file", str(self.prompt), "--log", str(self.log)]
        if command == "resume":
            args.extend(["--session-id", "sess-live"])
        return args

    def run_entry(self, args, stdin="", extra_env=None, expected=0, timeout=8):
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        result = subprocess.run(
            [sys.executable, str(ENTRY), *args],
            env=env, cwd=str(self.cwd), input=stdin, capture_output=True,
            text=True, timeout=timeout,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result

    def start_entry(self, extra_env=None, command="run"):
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        proc = subprocess.Popen(
            [sys.executable, str(ENTRY), *self.run_args(command)],
            env=env, cwd=str(self.cwd), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        self.addCleanup(lambda: proc.poll() is not None or proc.kill())
        return proc

    def wait_token(self, proc, token, timeout=5):
        output = ""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if line:
                output += line
                if token in output:
                    return output
            elif proc.poll() is not None:
                break
        self.fail("missing %s in %r exit=%s" % (token, output, proc.poll()))

    def wait_exit(self, proc, timeout=5):
        deadline = time.monotonic() + timeout
        while proc.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertIsNotNone(proc.poll(), "wrapper did not exit while stdin stayed open")
        rest_out, rest_err = proc.communicate(timeout=2)
        return rest_out, rest_err

    def received(self):
        if not self.rx_file.exists():
            return []
        return [json.loads(line) for line in self.rx_file.read_text(encoding="utf-8").splitlines()]

    def methods(self):
        return [item.get("method") for item in self.received()]

    def permission_outcomes(self):
        found = []
        for item in self.received():
            outcome = (item.get("result") or {}).get("outcome")
            if isinstance(outcome, dict):
                found.append(outcome)
        return found

    def test_inspect_passes_worktree_cwd(self):
        self.run_entry(["inspect", "--cwd", str(self.cwd)])
        argv = json.loads(self.argv_file.read_text(encoding="utf-8"))
        self.assertIn("inspect", argv)
        self.assertEqual(argv[argv.index("--cwd") + 1], str(self.cwd))

    def test_run_sends_model_effort_cwd_and_session_new(self):
        result = self.run_entry(self.run_args())
        argv = json.loads(self.argv_file.read_text(encoding="utf-8"))
        self.assertEqual(argv[argv.index("--model") + 1], "grok-4.6")
        self.assertEqual(argv[argv.index("--effort") + 1], "xhigh")
        self.assertIn("session/new", self.methods())
        self.assertNotIn("session/load", self.methods())
        self.assertNotIn("session/set_config_option", self.methods())
        new = next(item for item in self.received() if item.get("method") == "session/new")
        self.assertEqual(Path(new["params"]["cwd"]), self.cwd)
        self.assertNotIn("totalContextTokens", new["params"])
        self.assertIn("ADVERTISED_HIGHEST_EFFORT xhigh", result.stdout)
        self.assertIn('"effort": "xhigh"', result.stdout)
        self.assertIn('"permissionMode": "default"', result.stdout)
        self.assertNotIn("--always-approve", argv)
        self.assertIn('"status": "completed"', result.stdout)

    def test_bypass_permissions_maps_to_always_approve(self):
        result = self.run_entry(self.run_args() + ["--permission-mode", "bypassPermissions"])
        argv = json.loads(self.argv_file.read_text(encoding="utf-8"))
        self.assertIn("--always-approve", argv)
        self.assertLess(argv.index("agent"), argv.index("--always-approve"))
        self.assertLess(argv.index("--always-approve"), argv.index("stdio"))
        self.assertNotIn("bypassPermissions", argv)
        self.assertIn('"permissionMode": "bypassPermissions"', result.stdout)
        self.assertIn('"status": "completed"', result.stdout)

    def test_resume_loads_the_same_session(self):
        self.run_entry(self.run_args("resume"))
        load = next(item for item in self.received() if item.get("method") == "session/load")
        self.assertEqual(load["params"]["sessionId"], "sess-live")
        self.assertNotIn("session/new", self.methods())

    def test_resume_restores_high_effort_to_requested_xhigh(self):
        result = self.run_entry(self.run_args("resume"), extra_env={"HOST_DAG_FAKE_SESSION_EFFORT": "high"})
        setter = next(item for item in self.received() if item.get("method") == "session/set_config_option")
        self.assertEqual(setter["params"]["sessionId"], "sess-live")
        self.assertEqual(setter["params"]["configId"], "reasoning_effort")
        self.assertEqual(setter["params"]["value"], "xhigh")
        self.assertIn('"effort": "xhigh"', result.stdout)

    def test_unanswered_permission_is_not_success(self):
        result = self.run_entry(self.run_args(), stdin="", extra_env={"HOST_DAG_FAKE_PERMISSION": "1"}, expected=1)
        self.assertIn('"status": "unanswered"', result.stdout)
        self.assertNotIn('"status": "completed"', result.stdout)

    def test_allow_once_selects_advertised_option(self):
        result = self.run_entry(self.run_args(), stdin="allow-once\n", extra_env={"HOST_DAG_FAKE_PERMISSION": "1"})
        reply = next(item for item in self.received() if item.get("result") and "outcome" in item["result"])
        self.assertEqual(reply["result"]["outcome"]["optionId"], "allow-once")
        self.assertIn('"status": "completed"', result.stdout)

    def test_reject_once_is_not_success_even_with_end_turn(self):
        result = self.run_entry(
            self.run_args(), stdin="reject-once\n",
            extra_env={"HOST_DAG_FAKE_PERMISSION": "1"}, expected=1,
        )
        self.assertEqual(grok.classify("end_turn", flags(rejected=True)), "rejected")
        self.assertIn('"status": "rejected"', result.stdout)
        self.assertNotIn('"status": "completed"', result.stdout)

    def test_cancel_during_permission_exits_with_stdin_open(self):
        proc = self.start_entry({"HOST_DAG_FAKE_PERMISSION": "1"})
        seen = self.wait_token(proc, "PERMISSION")
        os.kill(proc.pid, signal.SIGINT)
        rest, err = self.wait_exit(proc, timeout=5)
        output = seen + rest
        self.assertEqual(proc.returncode, 1, output + err)
        self.assertNotIn('"status": "completed"', output)
        self.assertIn('"status": "cancelled"', output)
        self.assertIn("TERMINAL", output)

    def test_cancel_during_partial_permission_line_does_not_select_allow(self):
        proc = self.start_entry({"HOST_DAG_FAKE_PERMISSION": "1"})
        seen = self.wait_token(proc, "PERMISSION")
        proc.stdin.write("allow-")
        proc.stdin.flush()
        time.sleep(0.2)
        os.kill(proc.pid, signal.SIGINT)
        rest, err = self.wait_exit(proc, timeout=5)
        output = seen + rest
        self.assertEqual(proc.returncode, 1, output + err)
        self.assertIn("session/cancel", self.methods())
        self.assertIn("CANCEL_SENT", output)
        self.assertNotIn('"status": "completed"', output)
        self.assertNotIn({"outcome": "selected", "optionId": "allow-once"}, self.permission_outcomes())

    def test_permission_line_can_arrive_in_chunks(self):
        proc = self.start_entry({"HOST_DAG_FAKE_PERMISSION": "1"})
        seen = self.wait_token(proc, "PERMISSION")
        proc.stdin.write("allow-")
        proc.stdin.flush()
        time.sleep(0.1)
        proc.stdin.write("once\n")
        proc.stdin.flush()
        rest, err = self.wait_exit(proc, timeout=5)
        output = seen + rest
        self.assertEqual(proc.returncode, 0, output + err)
        self.assertIn({"outcome": "selected", "optionId": "allow-once"}, self.permission_outcomes())
        self.assertIn('"status": "completed"', output)

    def test_prefetched_extra_line_is_not_a_second_selection(self):
        result = self.run_entry(
            self.run_args(), stdin="allow-once\nreject-once\n",
            extra_env={"HOST_DAG_FAKE_PERMISSION": "1"},
        )
        self.assertEqual(self.permission_outcomes(), [{"outcome": "selected", "optionId": "allow-once"}])
        self.assertIn('"status": "completed"', result.stdout)

    def test_cancel_before_session_does_not_send_prompt(self):
        proc = self.start_entry({"HOST_DAG_FAKE_HANG_INIT": "1"})
        seen = self.wait_token(proc, "WRAPPER_PID")
        os.kill(proc.pid, signal.SIGINT)
        rest, err = self.wait_exit(proc, timeout=5)
        output = seen + rest
        self.assertEqual(proc.returncode, 1, output + err)
        self.assertNotIn("session/prompt", self.methods())
        self.assertNotIn("session/new", self.methods())
        self.assertNotIn("TERMINAL", output)
        self.assertIn("LOCAL_CANCEL", output)
        self.assertIn('"phase": "start"', output)
        self.assertIn('"status": "cancelled"', output)

    def test_cancel_after_prompt_waits_for_stop_reason(self):
        proc = self.start_entry({"HOST_DAG_FAKE_HANG": "1"})
        seen = self.wait_token(proc, "PROMPT_WAITING")
        os.kill(proc.pid, signal.SIGINT)
        rest, err = self.wait_exit(proc, timeout=5)
        output = seen + rest
        self.assertEqual(proc.returncode, 1, output + err)
        self.assertIn("session/cancel", self.methods())
        self.assertIn("CANCEL_SENT", output)
        self.assertIn("TERMINAL", output)
        self.assertIn('"stopReason": "cancelled"', output)
        self.assertIn('"status": "cancelled"', output)
        self.assertNotIn('"status": "completed"', output)

    def test_thread_subcommand_is_rejected_without_touching_codex(self):
        result = self.run_entry(["thread", "start"], expected=1)
        self.assertEqual(json.loads(result.stdout)["error"]["code"], "WRONG_HOST")
        self.assertFalse(self.argv_file.exists())

    def observer_rows(self):
        if not self.log.exists():
            return []
        rows = []
        for line in self.log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            obj = json.loads(line)
            if obj.get("direction") == "observer":
                rows.append(obj)
        return rows

    def test_observer_lifecycle_is_in_log_not_acp_stdin(self):
        result = self.run_entry(self.run_args())
        events = [row.get("event") for row in self.observer_rows()]
        self.assertIn("lifecycle", events)
        self.assertIn("process_exit", events)
        life = next(row for row in self.observer_rows() if row.get("event") == "lifecycle")
        self.assertEqual(life["status"], "completed")
        self.assertEqual(life["sessionId"], "sess-new")
        self.assertEqual(life["promptRpcId"], 4)
        self.assertFalse(any(item.get("method") == "observer" or item.get("direction") == "observer" for item in self.received()))
        self.assertIn('"status": "completed"', result.stdout)

    def test_observer_lifecycle_keeps_rejected_status(self):
        self.run_entry(
            self.run_args(), stdin="reject-once\n",
            extra_env={"HOST_DAG_FAKE_PERMISSION": "1"}, expected=1,
        )
        life = next(row for row in self.observer_rows() if row.get("event") == "lifecycle")
        self.assertEqual(life["status"], "rejected")
        self.assertNotEqual(life["status"], "completed")

    def test_highest_effort_is_not_the_default_flag(self):
        state = {"availableModels": [{
            "modelId": "grok-4.6",
            "_meta": {"reasoningEfforts": [
                {"id": "xhigh", "default": False},
                {"id": "high", "default": True},
            ]},
        }]}
        binding = grok.confirm_model_effort(state, "grok-4.6", "xhigh")
        self.assertEqual(binding["advertisedHighestEffort"], "xhigh")
        with self.assertRaises(grok.UsageError):
            grok.confirm_model_effort(state, "grok-4.6", "made-up")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
HOST = SCRIPTS / "host-dag-claude-host.py"

FAKE_CLAUDE = r'''#!/usr/bin/env python3
import json, os, sys, time
prompt = sys.stdin.read()
with open(os.environ["FAKE_LOG"], "a") as fh:
    fh.write(json.dumps(sys.argv[1:]) + "\n")
time.sleep(float(os.environ.get("FAKE_SLEEP", "0")))
print(json.dumps({"type": "result", "is_error": False, "result": "handled: " + prompt.strip()}))
'''

FAKE_CODEX = r'''#!/usr/bin/env python3
import json, sys
print(json.dumps({"delegated": sys.argv[1:]}))
'''


class ClaudeHostTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        claude, codex = base / "claude", base / "codex"
        claude.write_text(FAKE_CLAUDE)
        codex.write_text(FAKE_CODEX)
        claude.chmod(0o755)
        codex.chmod(0o755)
        self.log = base / "claude.log"
        self.env = {**os.environ, "HOST_DAG_CLAUDE_HOST_STATE": str(base / "state"), "HOST_DAG_CLAUDE_BIN": str(claude),
                    "HOST_DAG_CODEX_HOST": str(codex), "FAKE_LOG": str(self.log)}
        self.cwd = str(base)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_host(self, *args: str, **env: str) -> tuple[int, dict]:
        proc = subprocess.run([sys.executable, str(HOST), *args], capture_output=True, text=True,
                              env={**self.env, **env}, check=False)
        return proc.returncode, json.loads(proc.stdout or "{}")

    def wait_done(self, thread: str) -> dict:
        for _ in range(100):
            _, body = self.run_host("thread", "read", thread)
            if body["status"] != "running":
                return body
            time.sleep(0.1)
        self.fail("turn never finished")

    def test_turns_resume_one_session_and_report_like_codex(self) -> None:
        _, new = self.run_host("thread", "new", "--cwd", self.cwd)
        thread = new["threadId"]
        self.assertTrue(thread.startswith("claude-"))
        rc, sent = self.run_host("thread", "send", "--message", "event one", thread)
        self.assertEqual(rc, 0)
        first = self.wait_done(thread)
        self.assertEqual(first["result"]["turnId"], sent["turn"]["id"])
        self.assertEqual(first["result"]["text"], "handled: event one")
        self.run_host("thread", "send", "--message", "event two", thread)
        self.wait_done(thread)
        calls = [json.loads(line) for line in self.log.read_text().splitlines()]
        self.assertIn("--session-id", calls[0])
        self.assertIn("--resume", calls[1])
        self.assertEqual(calls[0][calls[0].index("--session-id") + 1], calls[1][calls[1].index("--resume") + 1])
        self.assertIn("opus[1m]", calls[0])

    def test_busy_while_running_and_cancel_stops_it(self) -> None:
        _, new = self.run_host("thread", "new", "--cwd", self.cwd)
        thread = new["threadId"]
        _, sent = self.run_host("thread", "send", "--message", "slow", thread, FAKE_SLEEP="30")
        rc, busy = self.run_host("thread", "send", "--message", "again", thread)
        self.assertEqual((rc, busy["error"]["code"]), (1, "THREAD_BUSY"))
        _, read = self.run_host("thread", "read", thread)
        self.assertEqual((read["status"], read["result"]["text"]), ("running", ""))
        _, cancelled = self.run_host("thread", "cancel", thread, "--turn-id", sent["turn"]["id"])
        self.assertEqual(cancelled["status"], "cancelled")
        _, after = self.run_host("thread", "read", thread)
        self.assertEqual(after["status"], "cancelled")

    def test_codex_threads_pass_through(self) -> None:
        _, body = self.run_host("thread", "read", "01a0db1b-2810-7802-85d5-a269a0878d2d")
        self.assertEqual(body["delegated"], ["thread", "read", "01a0db1b-2810-7802-85d5-a269a0878d2d"])


if __name__ == "__main__":
    unittest.main()

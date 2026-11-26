"""scripts/hostdag_guard_selfcheck.py against fake settings, a fake codex and real guard scripts."""
from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import hostdag_guard_selfcheck as sc  # noqa: E402

GUARD = SCRIPTS / "hostdag_pretool_guard.py"


class SelfcheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = Path(self.tmp.name)
        self.cfg = self.d / "guard.json"
        self.cfg.write_text((SCRIPTS / "pretool-guard.example.json").read_text())
        self.cmd = f"{sys.executable} {GUARD} --config {self.cfg}"

    def tearDown(self):
        self.tmp.cleanup()

    def settings(self, cmd):
        p = self.d / "settings.json"
        p.write_text(json.dumps({"hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": cmd}]}]}}))
        return p

    def fake_codex(self, trust, cmd=None):
        hook = {"key": "/x/hooks.json:pre_tool_use:1:0", "eventName": "preToolUse", "command": cmd or self.cmd,
                "enabled": True, "trustStatus": trust, "currentHash": "sha256:abc"}
        answer = json.dumps({"id": 2, "result": {"data": [{"cwd": "/", "hooks": [hook], "errors": []}]}})
        p = self.d / "codex"
        p.write_text(f"#!/bin/sh\nhead -n 3 >/dev/null\necho '{answer}'\n")
        p.chmod(0o755)
        return p

    def run_check(self, *args):
        out = io.StringIO()
        with redirect_stdout(out):
            rc = sc.main([*args, "--json"])
        return rc, json.loads(out.getvalue())

    def test_all_good(self):
        rc, rep = self.run_check("--claude-settings", str(self.settings(self.cmd)), "--codex", str(self.fake_codex("trusted")))
        self.assertEqual(rc, 0, rep)

    def test_codex_untrusted_is_reported(self):
        rc, rep = self.run_check("--claude-settings", str(self.settings(self.cmd)), "--codex", str(self.fake_codex("modified")))
        self.assertEqual(rc, 1)
        self.assertTrue(any("trustStatus=modified" in e for e in rep["errors"]))

    def test_missing_script_is_reported(self):
        gone = f"{sys.executable} {self.d}/moved/pretool_guard.py"
        rc, rep = self.run_check("--claude-settings", str(self.settings(gone)), "--no-codex")
        self.assertEqual(rc, 1)
        self.assertTrue(any("does not exist" in e for e in rep["errors"]))

    def test_guard_that_allows_stash_is_reported(self):
        self.cfg.write_text(json.dumps({"git": [], "search": []}))
        rc, rep = self.run_check("--claude-settings", str(self.settings(self.cmd)), "--no-codex")
        self.assertEqual(rc, 1)
        self.assertTrue(any("was not denied" in e for e in rep["errors"]))

    def test_hook_absent_is_reported(self):
        rc, rep = self.run_check("--claude-settings", str(self.settings("/bin/true")), "--no-codex")
        self.assertEqual(rc, 1)
        self.assertTrue(any("no PreToolUse hook" in e for e in rep["errors"]))


if __name__ == "__main__":
    unittest.main()

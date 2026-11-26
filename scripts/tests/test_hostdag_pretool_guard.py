"""Samples for scripts/hostdag_pretool_guard.py (configurable PreToolUse guard)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import hostdag_pretool_guard as guard  # noqa: E402

EXAMPLE = SCRIPTS / "pretool-guard.example.json"
CFG = guard.load_config(str(EXAMPLE))
RUN = "/mnt/evidence/runs/r1"


def bash(cmd, cwd="/home/u/project", cfg=CFG):
    return guard.evaluate({"tool_name": "Bash", "tool_input": {"command": cmd}, "cwd": cwd}, cfg)


class GitRuleTest(unittest.TestCase):
    def test_stash_writes_denied(self):
        for cmd in ["git stash", "git stash push -m wip", "git stash pop", "git stash -u",
                    "git -C /x stash drop", "/usr/bin/git stash save x", "cd /tmp && git stash",
                    "timeout 30 git stash", "FOO=1 git -c a.b=c stash", "bash -lc 'git stash'",
                    "rtk proxy git stash", "git status; git stash apply", "git stash 'unbalanced"]:
            with self.subTest(cmd=cmd):
                self.assertIn("stash", bash(cmd) or "")

    def test_stash_readonly_and_mentions_allowed(self):
        for cmd in ["git stash list", "git stash show -p stash@{0}", "echo git stash",
                    "git commit -m 'never git stash'", "git log --grep stash",
                    "cat <<'X'\ngit stash\nX"]:
            with self.subTest(cmd=cmd):
                self.assertIsNone(bash(cmd))

    def test_push_flag_rule(self):
        self.assertTrue(bash("git push --force origin main"))
        self.assertTrue(bash("git push -f origin main"))
        self.assertTrue(bash("git push --force-with-lease=main:abc origin main"))
        self.assertIsNone(bash("git push origin HEAD:refs/heads/feature/x"))

    def test_whole_subcommand_rule(self):
        cfg = {"git": [{"subcommand": "clean"}], "search": []}
        self.assertTrue(bash("git clean -fdx", cfg=cfg))
        self.assertIsNone(bash("git status", cfg=cfg))


class SearchRuleTest(unittest.TestCase):
    def test_denied(self):
        for cmd, cwd in [(f"grep -rn foo {RUN}", "/"), ("grep -r foo", RUN), ("grep -rl foo ..", RUN + "/control"),
                         (f"rg foo {RUN}", "/"), ("rg foo", "/mnt/evidence"), ("grep -r foo /", "/home"),
                         ("grep -R foo /mnt", "/"), (f"cd {RUN} && grep -rn foo .", "/home"),
                         (f"grep -rn foo {RUN}/evidence", "/"), ("grep --recursive foo /mnt/evidence/runs", "/"),
                         ("grep -d recurse foo /mnt/evidence", "/")]:
            with self.subTest(cmd=cmd, cwd=cwd):
                self.assertIn("blocked", bash(cmd, cwd) or "")

    def test_allowed(self):
        for cmd, cwd in [(f"grep -rn foo {RUN}/control", "/"), (f"grep -rn foo {RUN}/evidence/T1", "/"),
                         (f"grep -n foo {RUN}/log.jsonl", "/"), (f"ls {RUN}", "/"), ("grep foo *.md", RUN),
                         ("grep -rn foo .", "/home/u/project"), ("rg foo /mnt/other", "/")]:
            with self.subTest(cmd=cmd, cwd=cwd):
                self.assertIsNone(bash(cmd, cwd))

    def test_ancestors_optional(self):
        cfg = {"git": [], "search": [{"paths": ["/mnt/evidence"], "deny_ancestors": False}]}
        self.assertIsNone(bash("grep -r foo /mnt", "/", cfg))
        self.assertTrue(bash("grep -r foo /mnt/evidence", "/", cfg))

    def test_existing_file_target_allowed(self):
        with tempfile.TemporaryDirectory() as d:
            f = Path(d) / "a.txt"
            f.write_text("x")
            cfg = {"git": [], "search": [{"paths": [d]}]}
            self.assertIsNone(bash(f"grep -r x {f}", "/", cfg))
            self.assertTrue(bash(f"grep -r x {d}", "/", cfg))

    def test_grep_tool(self):
        ev = lambda tin, cwd: guard.evaluate({"tool_name": "Grep", "tool_input": tin, "cwd": cwd}, CFG)
        self.assertTrue(ev({"pattern": "x", "path": RUN}, "/"))
        self.assertTrue(ev({"pattern": "x"}, RUN))
        self.assertIsNone(ev({"pattern": "x", "path": RUN + "/control"}, "/"))


class ConfigAndProtocolTest(unittest.TestCase):
    def run_guard(self, payload, *args, env=None):
        return subprocess.run([sys.executable, str(SCRIPTS / "hostdag_pretool_guard.py"), *args],
                              input=payload, text=True, capture_output=True,
                              env={"PATH": "/usr/bin:/bin", "HOME": env or "/nonexistent"})

    def test_exit_codes_and_log(self):
        with tempfile.TemporaryDirectory() as d:
            cfg = json.loads(EXAMPLE.read_text())
            cfg["log"] = str(Path(d) / "denials.jsonl")
            cp = Path(d) / "cfg.json"
            cp.write_text(json.dumps(cfg))
            ok = self.run_guard(json.dumps({"tool_name": "Bash", "tool_input": {"command": "ls"}, "cwd": "/"}), "--config", str(cp))
            self.assertEqual((ok.returncode, ok.stdout, ok.stderr), (0, "", ""))
            no = self.run_guard(json.dumps({"tool_name": "Bash", "tool_input": {"command": "git stash"}, "cwd": "/"}), "--config", str(cp))
            self.assertEqual(no.returncode, 2)
            self.assertIn("worktree", no.stderr)
            self.assertEqual(Path(cfg["log"]).read_text().count("\n"), 1)
            bad = self.run_guard("{not json", "--config", str(cp))
            self.assertEqual(bad.returncode, 2)
            self.assertIn("guard fault", bad.stderr)

    def test_missing_config_allows_everything(self):
        r = self.run_guard(json.dumps({"tool_name": "Bash", "tool_input": {"command": "git stash"}, "cwd": "/"}),
                           "--config", "/nonexistent/guard.json")
        self.assertEqual(r.returncode, 0)

    def test_invalid_config_is_fault(self):
        with tempfile.TemporaryDirectory() as d:
            cp = Path(d) / "cfg.json"
            for bad in ["[]", '{"search":[{"paths":["relative/dir"]}]}', '{"git":[{}]}', "{oops"]:
                with self.subTest(cfg=bad):
                    cp.write_text(bad)
                    r = self.run_guard("{}", "--config", str(cp))
                    self.assertEqual(r.returncode, 2)
                    self.assertIn("guard fault", r.stderr)


if __name__ == "__main__":
    unittest.main()

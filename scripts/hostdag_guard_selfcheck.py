#!/usr/bin/env python3
"""Check that a PreToolUse guard hook is still live in Claude Code and Codex.

Why: Codex runs a user hook only while its persisted trust hash matches. Reordering
~/.codex/hooks.json, inserting a hook before the guard, editing its command line, or a
Codex auto-update can leave the guard "untrusted"; Codex then skips it silently and the
blocked command runs again. A fail-closed guard whose script file vanished blocks every
command instead. Run this after any hook edit or client upgrade, and on a timer.

Checks (each hook whose command contains --match, default "pretool_guard"):
  1. Claude: ~/.claude/settings.json has it under PreToolUse; every absolute path in
     the command exists.
  2. Codex: `codex app-server` hooks/list reports it present, enabled and trusted.
  3. Probe: running the command with a payload that must be denied (default: Bash
     `git stash`) exits 2; running it with `true` exits 0.

usage: hostdag_guard_selfcheck.py [--match S] [--claude-settings P] [--codex BIN | --no-codex]
                                  [--deny-command CMD] [--json]
exit 0 = all checks pass, 1 = a check failed (details on stdout).
"""
from __future__ import annotations

import argparse
import json
import os
import select
import shlex
import shutil
import subprocess
import time
from pathlib import Path


def claude_hooks(settings: Path, match: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    try:
        cfg = json.loads(settings.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return [], [f"claude: cannot read {settings}: {e}"]
    cmds = [h.get("command", "") for e in (cfg.get("hooks") or {}).get("PreToolUse", [])
            for h in e.get("hooks", []) if match in h.get("command", "")]
    if not cmds:
        errors.append(f"claude: no PreToolUse hook containing {match!r} in {settings}")
    return cmds, errors


def codex_hooks(codex: str, match: str, timeout: float = 40.0) -> tuple[list[str], list[str]]:
    msgs = [{"id": 1, "method": "initialize", "params": {"clientInfo": {"name": "guard_selfcheck", "version": "1"}}},
            {"method": "initialized"},
            {"id": 2, "method": "hooks/list", "params": {"cwds": [str(Path.home())]}}]
    try:
        proc = subprocess.Popen([codex, "app-server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, text=True, cwd=str(Path.home()))
    except OSError as e:
        return [], [f"codex: cannot start app-server: {e}"]
    result = None
    assert proc.stdin is not None and proc.stdout is not None
    try:
        for m in msgs:
            proc.stdin.write(json.dumps(m) + "\n")
        proc.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            ready, _, _ = select.select([proc.stdout], [], [], max(0.0, deadline - time.monotonic()))
            if not ready:
                break
            line = proc.stdout.readline()
            if not line:
                break
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            if msg.get("id") == 2:
                result = msg
                break
    finally:
        proc.kill()
        proc.wait()
    if result is None:
        return [], ["codex: hooks/list gave no answer"]
    if "error" in result:
        return [], [f"codex: hooks/list error: {result['error']}"]
    cmds, errors = [], []
    for scope in result["result"].get("data", []):
        for e in scope.get("errors", []):
            errors.append(f"codex: hook config error: {e}")
        for h in scope.get("hooks", []):
            if h.get("eventName") != "preToolUse" or match not in (h.get("command") or ""):
                continue
            cmds.append(h["command"])
            if not h.get("enabled", False):
                errors.append(f"codex: {h['key']} is disabled")
            if h.get("trustStatus") != "trusted":
                errors.append(f"codex: {h['key']} trustStatus={h.get('trustStatus')} (Codex skips it silently); "
                              f"currentHash={h.get('currentHash')}")
    if not cmds:
        errors.append(f"codex: no preToolUse hook containing {match!r}")
    return cmds, errors


def probe(cmd: str, deny_command: str) -> list[str]:
    errors = []
    try:
        argv = shlex.split(cmd)
    except ValueError as e:
        return [f"probe: cannot parse hook command {cmd!r}: {e}"]
    for tok in argv:
        if tok.startswith("/") and not os.path.exists(tok):
            errors.append(f"probe: {tok} does not exist (fail-closed hook would block every command)")
    if errors:
        return errors
    def run(command: str) -> subprocess.CompletedProcess:
        payload = json.dumps({"tool_name": "Bash", "tool_input": {"command": command}, "cwd": str(Path.home())})
        return subprocess.run(cmd, shell=True, input=payload, text=True, capture_output=True, timeout=20)
    deny = run(deny_command)
    if deny.returncode != 2 or not deny.stderr.strip():
        errors.append(f"probe: {deny_command!r} was not denied (rc={deny.returncode}) by {cmd}")
    allow = run("true")
    if allow.returncode != 0:
        errors.append(f"probe: plain `true` was not allowed (rc={allow.returncode}): {allow.stderr.strip()[:200]}")
    return errors


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--match", default="pretool_guard")
    ap.add_argument("--claude-settings", default=str(Path.home() / ".claude/settings.json"))
    ap.add_argument("--no-claude", action="store_true")
    ap.add_argument("--codex", default=shutil.which("codex") or "codex")
    ap.add_argument("--no-codex", action="store_true")
    ap.add_argument("--deny-command", default="git stash")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    errors: list[str] = []
    commands: set[str] = set()
    if not a.no_claude:
        c, e = claude_hooks(Path(a.claude_settings), a.match)
        commands.update(c)
        errors += e
    if not a.no_codex:
        c, e = codex_hooks(a.codex, a.match)
        commands.update(c)
        errors += e
    for cmd in sorted(commands):
        errors += probe(cmd, a.deny_command)
    report = {"ok": not errors, "commands": sorted(commands), "errors": errors,
              "at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    if a.json:
        print(json.dumps(report, ensure_ascii=False))
    else:
        print("OK" if not errors else "FAIL")
        for e in errors:
            print(" -", e)
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())

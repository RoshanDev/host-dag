#!/usr/bin/env python3
"""Configurable PreToolUse guard for Claude Code and Codex.

Turns "never do X" rules that keep being broken into checks that run before a tool call:
  * git subcommands to block (e.g. every `git stash` write, only `list` / `show` pass;
    or `git push` only when `--force` is present);
  * directories where recursive content search (grep -r/-R, rg, Claude Grep tool) is
    not allowed (e.g. a huge evidence disk), optionally with all their parent dirs.

Rules come from a JSON file (see scripts/pretool-guard.example.json):
  --config PATH  >  $HOST_DAG_GUARD_CONFIG  >  ~/.config/host-dag/pretool-guard.json
No config file = no rules (allow everything). An unreadable/invalid config is a guard fault.

Hook protocol (same for both clients): allow = exit 0 with no output;
deny = exit 2 with the reason on stderr. Any internal error exits 2 ("guard fault"):
the guard fails closed. Input is never rewritten, so it can sit next to rewrite hooks.

Install the script outside any git checkout you switch branches in (for example
~/.local/libexec/), because a fail-closed hook whose file disappears blocks every command.

Not covered: commands inside scripts, xargs, find -exec, paths built from variables,
shells nested more than 3 levels, interactive stdin writes.
"""
from __future__ import annotations

import fnmatch
import json
import os
import re
import shlex
import sys
import time
from pathlib import Path

DEFAULT_CONFIG = Path.home() / ".config/host-dag/pretool-guard.json"
DEFAULT_LOG = "~/.local/state/host-dag/pretool-guard-denials.jsonl"

DEFAULT_GIT_MSG = "Blocked by pretool guard: git {sub} is not allowed here."
DEFAULT_SCAN_MSG = ("Blocked by pretool guard: recursive search of {target} is not allowed; "
                    "search a narrower subdirectory or a single file instead.")

CONTROL_CHARS = set(";&|()\n")
WRAPPERS = {"sudo", "env", "command", "builtin", "exec", "nice", "nohup", "time", "stdbuf",
            "ionice", "chrt", "rtk", "proxy", "unbuffer"}
SHELLS = {"bash", "sh", "zsh", "dash"}
GREP_NAMES = {"grep", "egrep", "fgrep", "zgrep"}
RG_NAMES = {"rg"}
GIT_OPTS_WITH_ARG = {"-C", "-c", "--git-dir", "--work-tree", "--namespace", "--exec-path", "--config-env"}
GREP_TAKES_ARG = {"-e", "-f", "-m", "-A", "-B", "-C", "-d", "-D", "--include", "--exclude",
                  "--exclude-dir", "--color", "--colour", "--label", "--context",
                  "--regexp", "--file", "--max-count", "--directories", "--devices"}
RG_TAKES_ARG = {"-e", "-f", "-g", "--glob", "--iglob", "-t", "--type", "-T", "--type-not",
                "-m", "--max-count", "-A", "-B", "-C", "-j", "--threads", "--max-depth", "-d",
                "-M", "--max-columns", "--color", "--colors", "-E", "--encoding",
                "--type-add", "--sort", "--sortr", "--max-filesize", "--pre", "--pre-glob",
                "-r", "--replace", "--ignore-file", "--path-separator", "--context-separator",
                "--field-match-separator", "--field-context-separator", "--engine",
                "--regexp", "--file"}


class Deny(Exception):
    pass


class GuardConfigError(Exception):
    pass


# ---------------------------------------------------------------- config

def load_config(path: str | None) -> dict:
    p = Path(os.path.expanduser(path or os.environ.get("HOST_DAG_GUARD_CONFIG") or str(DEFAULT_CONFIG)))
    if not p.exists():
        return {"git": [], "search": []}
    try:
        cfg = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise GuardConfigError(f"cannot read {p}: {e}") from e
    if not isinstance(cfg, dict):
        raise GuardConfigError(f"{p}: top level must be an object")
    for rule in cfg.setdefault("git", []):
        if not isinstance(rule, dict) or not isinstance(rule.get("subcommand"), str):
            raise GuardConfigError(f"{p}: every git rule needs a string 'subcommand'")
    for rule in cfg.setdefault("search", []):
        if not isinstance(rule, dict) or not isinstance(rule.get("paths"), list) or not rule["paths"]:
            raise GuardConfigError(f"{p}: every search rule needs a non-empty 'paths' list")
        for pat in rule["paths"]:
            if not isinstance(pat, str) or not pat.startswith(("/", "~")):
                raise GuardConfigError(f"{p}: search paths must be absolute: {pat!r}")
    return cfg


# ---------------------------------------------------------------- path rules

def _parts(path: str) -> list[str]:
    return [c for c in os.path.normpath(path).split("/") if c]


def _match(pattern_parts: list[str], target_parts: list[str]) -> bool:
    return (len(pattern_parts) == len(target_parts)
            and all(fnmatch.fnmatchcase(t, p) for p, t in zip(pattern_parts, target_parts)))


def _literal_prefix(pattern_parts: list[str]) -> list[str]:
    out = []
    for c in pattern_parts:
        if any(ch in c for ch in "*?["):
            break
        out.append(c)
    return out


def path_denied(target: str, rule: dict) -> bool:
    cands = {os.path.normpath(target)}
    try:
        cands.add(os.path.realpath(target))
    except OSError:
        pass
    for cand in cands:
        tp = _parts(cand)
        for pat in rule["paths"]:
            pp = _parts(os.path.expanduser(pat))
            if _match(pp, tp):
                return True
            # a parent of a protected dir contains it, so searching the parent scans it too
            if rule.get("deny_ancestors", True):
                lit = _literal_prefix(pp)
                if len(tp) < len(lit) and lit[:len(tp)] == tp:
                    return True
    return False


# ---------------------------------------------------------------- shell parsing

def strip_heredocs(cmd: str) -> str:
    """Drop heredoc bodies; their text is data for the command, not shell words."""
    out, pending = [], []
    for line in cmd.split("\n"):
        if pending:
            tag, dash = pending[0]
            if (line.lstrip("\t") if dash else line) == tag:
                pending.pop(0)
            continue
        out.append(line)
        for m in re.finditer(r"<<(-?)\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\2", line):
            pending.append((m.group(3), m.group(1) == "-"))
    return "\n".join(out)


def tokenize(cmd: str) -> list[str]:
    lex = shlex.shlex(cmd.replace("\n", " ; "), posix=True, punctuation_chars=";&|()")
    lex.whitespace = " \t\r\n"
    lex.whitespace_split = True
    lex.commenters = "#"
    return list(lex)


def segments(tokens: list[str]):
    cur: list[str] = []
    for t in tokens:
        if t and set(t) <= CONTROL_CHARS or t in ("{", "}", "!"):
            if cur:
                yield cur
            cur = []
        else:
            cur.append(t)
    if cur:
        yield cur


def unwrap(words: list[str]) -> list[str]:
    w = list(words)
    while w:
        head = os.path.basename(w[0])
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", w[0]):
            w = w[1:]
        elif head == "timeout":
            w = w[1:]
            while w and (w[0].startswith("-") or re.match(r"^\d+(\.\d+)?[smhd]?$", w[0])):
                w = w[1:]
        elif head in WRAPPERS:
            w = w[1:]
            while w and w[0].startswith("-"):
                w = w[1:]
        else:
            break
    return w


# ---------------------------------------------------------------- checks

def check_git(words: list[str], cfg: dict) -> None:
    i = 1
    while i < len(words):
        a = words[i]
        if a in GIT_OPTS_WITH_ARG:
            i += 2
        elif a.startswith("-"):
            i += 1
        else:
            break
    if i >= len(words):
        return
    sub, args = words[i], words[i + 1:]
    for rule in cfg["git"]:
        if rule["subcommand"] != sub:
            continue
        msg = rule.get("message") or DEFAULT_GIT_MSG.format(sub=sub)
        if "allow" in rule:
            first = next((a for a in args if not a.startswith("-")), None)
            if first not in rule["allow"]:
                raise Deny(msg)
        elif "deny_if_args_any" in rule:
            flags = rule["deny_if_args_any"]
            if any(a in flags or any(a.startswith(f + "=") for f in flags if f.startswith("--"))
                   for a in args):
                raise Deny(msg)
        else:
            raise Deny(msg)


def search_targets(words: list[str], cwd: str, kind: str) -> list[str]:
    """Resolved directory operands of a recursive grep/rg call ([] if not recursive)."""
    recursive = kind == "rg"
    takes = RG_TAKES_ARG if kind == "rg" else GREP_TAKES_ARG
    pattern_given = False
    operands: list[str] = []
    i, end_opts = 1, False
    while i < len(words):
        a = words[i]
        if not end_opts and a == "--":
            end_opts = True
        elif not end_opts and a.startswith("--"):
            name, _, val = a.partition("=")
            if kind == "grep" and name in ("--recursive", "--dereference-recursive"):
                recursive = True
            if name in ("--regexp", "--file"):
                pattern_given = True
            if kind == "grep" and name == "--directories":
                v = val or (words[i + 1] if i + 1 < len(words) else "")
                recursive = recursive or v == "recurse"
            if not val and name in takes:
                i += 1
        elif not end_opts and a.startswith("-") and len(a) > 1:
            flags = a[1:]
            for j, ch in enumerate(flags):
                opt = "-" + ch
                if kind == "grep" and ch in "rR":
                    recursive = True
                if opt in ("-e", "-f"):
                    pattern_given = True
                if opt in takes:
                    rest = flags[j + 1:]
                    if kind == "grep" and opt == "-d":
                        v = rest or (words[i + 1] if i + 1 < len(words) else "")
                        recursive = recursive or v == "recurse"
                    if not rest:
                        i += 1
                    break
        elif not pattern_given:
            pattern_given = True
        else:
            operands.append(a)
        i += 1
    if not recursive:
        return []
    return [os.path.normpath(os.path.join(cwd, os.path.expanduser(o))) for o in (operands or ["."])]


def check_target(target: str, cfg: dict) -> None:
    if os.path.isfile(target):
        return
    for rule in cfg["search"]:
        if path_denied(target, rule):
            raise Deny((rule.get("message") or DEFAULT_SCAN_MSG).format(target=target))


def check_command(cmd: str, cwd: str, cfg: dict, depth: int = 0) -> None:
    if depth > 3:
        return
    body = strip_heredocs(cmd)
    try:
        tokens = tokenize(body)
    except ValueError:
        fallback_scan(body, cfg)
        return
    for seg in segments(tokens):
        words = unwrap(seg)
        if not words:
            continue
        head = os.path.basename(words[0])
        if head in ("cd", "pushd") and len(words) >= 2:
            cwd = os.path.normpath(os.path.join(cwd, os.path.expanduser(words[1])))
        elif head == "git":
            check_git(words, cfg)
        elif head in GREP_NAMES or head in RG_NAMES:
            kind = "rg" if head in RG_NAMES else "grep"
            for t in search_targets(words, cwd, kind):
                check_target(t, cfg)
        elif head in SHELLS:
            for k, a in enumerate(words[1:-1], 1):
                if a == "-c" or (a.startswith("-") and not a.startswith("--") and "c" in a[1:]):
                    check_command(words[k + 1], cwd, cfg, depth + 1)
                    break


def fallback_scan(body: str, cfg: dict) -> None:
    """Only for commands that do not tokenize (unbalanced quotes): word-level git check."""
    for line in body.split("\n"):
        for m in re.finditer(r"(?:^|[\s;&|(`])(?:\S*/)?git((?:\s+-[Cc]\s+\S+|\s+--\S+)*)\s+([A-Za-z-]+)((?:\s+[^\s;&|)`]+)*)", line):
            check_git(["git", m.group(2), *m.group(3).split()], cfg)


def evaluate(payload: dict, cfg: dict) -> str | None:
    tool = payload.get("tool_name") or ""
    tin = payload.get("tool_input") or {}
    cwd = payload.get("cwd") or os.getcwd()
    try:
        if tool in ("Bash", "shell", "exec_command", "local_shell"):
            cmd = tin.get("command")
            if isinstance(cmd, list):
                cmd = " ".join(shlex.quote(c) for c in cmd)
            if isinstance(cmd, str):
                check_command(cmd, tin.get("workdir") or cwd, cfg)
        elif tool == "Grep":
            check_target(os.path.normpath(os.path.join(cwd, os.path.expanduser(tin.get("path") or cwd))), cfg)
    except Deny as d:
        return str(d)
    return None


def log_denial(cfg: dict, payload: dict, msg: str) -> None:
    log = cfg.get("log", DEFAULT_LOG)
    if not log:
        return
    try:
        p = Path(os.path.expanduser(log))
        p.parent.mkdir(parents=True, exist_ok=True)
        rec = {"at": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "tool": payload.get("tool_name"),
               "cwd": payload.get("cwd"), "session": payload.get("session_id"),
               "input": payload.get("tool_input"), "reason": msg[:300]}
        with p.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    try:
        cfg_path = None
        if argv[:1] == ["--config"] and len(argv) >= 2:
            cfg_path = argv[1]
        elif argv:
            raise GuardConfigError(f"unknown arguments {argv!r}; usage: hostdag_pretool_guard.py [--config PATH]")
        cfg = load_config(cfg_path)
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
        msg = evaluate(payload, cfg)
    except Exception as e:  # fail closed
        sys.stderr.write(f"pretool guard fault: {type(e).__name__}: {e}. "
                         "To roll back, remove this hook entry from the client's hook config.\n")
        return 2
    if msg:
        log_denial(cfg, payload, msg)
        sys.stderr.write(msg + "\n")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

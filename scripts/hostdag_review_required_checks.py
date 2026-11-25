#!/usr/bin/env python3
"""Checks a review contract must run, chosen by the guard's covered file patterns.

TOOL-KK-PY37-GUARD-HOOK (Fable advisor/log.jsonl#L1132 §3). Picking review packages
only from the changed paths never selected pkg/compat, where the target node
Python 3.7 guard lives, so pkg/*/lifecycle.py changes (shlex.join) passed three
reviews. Each rule here names the files its guard reads; a diff that touches any
of them makes the guard mandatory whatever package the diff lives in.

Every review dispatch through host-dag-cli-agent.py run or host-dag-codex-host.py thread
start appends the section to the contract. A dispatch is a review when any of these hold
(review_reasons): role reviewer, an explicit --review-axis, or an out/prompt/message path
with a review-* directory, so the coordinator's usual command needs no extra flag.
KK checkouts are found at cwd, among cwd's direct children (parent worktree layout
<ticket>/{tools,kk}) and at paths the contract names. The diff base is --review-base,
else the contract's A..B range; unknown base, or a review naming KK without a locatable
checkout, fails closed (guard required).

CLI:
  hostdag_review_required_checks.py select --cwd KK [--base SHA] [--changed PATH ...]
  hostdag_review_required_checks.py augment --cwd KK [--base SHA] --contract IN --out OUT [--ticket T]
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
from pathlib import Path

PY37_IMAGE = "python:3.7-slim"
PY37_RUNTIME = ("docker run --rm --network none -v \"$PWD\":/src:ro -w /src " + PY37_IMAGE +
                " python -B scripts/tests/test_node_python37.py")

RULES: tuple[dict, ...] = ()


def match(path: str, pattern: str) -> bool:
    """Glob where ** spans directories (fnmatch * already does) and ``a/**/b`` also matches ``a/b``."""
    return fnmatch.fnmatchcase(path, pattern) or ("/**/" in pattern and
                                                  fnmatch.fnmatchcase(path, pattern.replace("/**/", "/")))


def git(cwd: Path, *argv: str) -> str:
    return subprocess.run(["git", "-C", str(cwd), *argv], check=True, capture_output=True, text=True).stdout


def toplevel(cwd: Path) -> Path | None:
    try:
        return Path(git(cwd, "rev-parse", "--show-toplevel").strip())
    except (subprocess.CalledProcessError, OSError):
        return None


def changed_files(root: Path, base: str) -> list[str]:
    """Committed and uncommitted changes against base, plus untracked files."""
    names = git(root, "diff", "--name-only", base, "--").splitlines()
    names += git(root, "ls-files", "--others", "--exclude-standard").splitlines()
    return sorted(set(n for n in names if n))


def embeds_go(root: Path, path: str) -> bool:
    """Any //go:embed in a non-test Go file: directory or glob patterns (``assets/*``) can pull in
    node Python just as a literal ``.py`` does, and node_sources() keeps whatever .py they resolve to."""
    if path.endswith("_test.go"):
        return False
    file = root / path
    if not file.is_file():
        return True  # deleted embedding file: the guard decides whether sources went missing
    return bool(re.search(r"^\s*//go:embed\s", file.read_text(errors="replace"), re.M))


def rule_hits(rule: dict, root: Path, changed: list[str]) -> list[str]:
    hits = [p for p in changed if any(match(p, pat) for pat in rule["patterns"])]
    hits += [p for p in changed if p not in hits and any(match(p, pat) for pat in rule["embed_patterns"])
             and embeds_go(root, p)]
    return hits


# ---- Which dispatches are reviews (P1-a, round 2) -------------------------------------------
REVIEW_DIR = re.compile(r"^review([-_].*)?$")


def review_reasons(role: str | None = None, axis: str | None = None, paths=()) -> list[str]:
    """Why a dispatch counts as a review; empty = not a review.

    Any one signal is enough, so the hook does not depend on the coordinator remembering a flag:
    --role/--dispatch-role reviewer, an explicit --review-axis, or an evidence path (out dir,
    prompt/message file) with a ``review-*`` component such as ``<evidence>/review-20261109/cli``.
    """
    reasons = []
    if role == "reviewer":
        reasons.append("role=reviewer")
    if axis:
        reasons.append("review-axis=" + axis)
    for raw in paths:
        if not raw:
            continue
        parts = Path(str(raw)).parts
        dirs = parts[:-1] if Path(str(raw)).suffix else parts
        hit = next((d for d in dirs if REVIEW_DIR.match(d)), None)
        if hit:
            reasons.append(f"review-dir={hit} ({raw})")
    return reasons


# ---- Where the KK checkouts are (P1-b, round 2) ---------------------------------------------
PATH_TOKEN = re.compile(r"(/[^\s`'\"，。；、（）()：<>\[\]{}|]+)")
DIFF_RANGE = re.compile(r"\b([0-9a-f]{7,40})\.\.\.?([0-9a-f]{7,40})\b")
KK_HINT = re.compile(r"(?!x)x")


def is_kk_root(path: Path) -> bool:
    return any((path / rule["marker"]).is_file() for rule in RULES)


def kk_root_of(path: Path) -> Path | None:
    """The KK checkout containing path: its git toplevel, else the nearest ancestor with the marker."""
    while not path.exists() and path != path.parent:
        path = path.parent
    start = path if path.is_dir() else path.parent
    top = toplevel(start)
    if top and is_kk_root(top):
        return top.resolve()
    for d in (start, *start.parents):
        if is_kk_root(d):
            return d.resolve()
    return None


def kk_roots(cwd: Path, contract: str | None = None) -> list[tuple[Path, str]]:
    """[(root, found_by)]: cwd's checkout, else direct child checkouts, plus checkouts named in the contract."""
    found: dict[Path, str] = {}
    top = toplevel(cwd) or cwd
    if is_kk_root(top):
        found[top.resolve()] = "cwd"
    else:
        try:
            children = sorted(c for c in cwd.iterdir() if c.is_dir() and not c.name.startswith("."))
        except OSError:
            children = []
        for child in children:
            if is_kk_root(child):
                found.setdefault(child.resolve(), "child:" + child.name)
    for token in list(dict.fromkeys(PATH_TOKEN.findall(contract or "")))[:200]:
        token = token.rstrip(".,;:")
        if len(token) < 2 or not Path(token).is_absolute():
            continue
        root = kk_root_of(Path(token))
        if root:
            found.setdefault(root, "contract:" + token)
    return list(found.items())


def resolves(root: Path, rev: str) -> bool:
    try:
        git(root, "rev-parse", "--verify", "--quiet", rev + "^{commit}")
        return True
    except (subprocess.CalledProcessError, OSError):
        return False


def contract_base(root: Path, contract: str | None) -> str | None:
    """Left side of the first ``A..B`` range in the contract that resolves in this checkout."""
    for left, _ in DIFF_RANGE.findall(contract or ""):
        if resolves(root, left):
            return left
    return None


def relative_changed(cwd: Path, root: Path, changed: list[str]) -> list[str]:
    """Explicit changed paths (relative to cwd, or absolute) mapped into root."""
    out = []
    for name in changed:
        p = Path(name)
        full = (p if p.is_absolute() else cwd / p).resolve()
        try:
            out.append(full.relative_to(root).as_posix())
        except ValueError:
            continue
    return out


def select(cwd, base: str | None = None, changed: list[str] | None = None, contract: str | None = None) -> dict:
    """Return {repo, base, changed, roots:[...], required:[{id, repo, reason, hits, commands, why}]}.

    Every KK checkout found (cwd, its direct children, paths the contract names) gets the rule.
    Per checkout the diff comes from --review-base, else the contract's ``A..B`` range; a base that
    does not resolve there, or none at all, fails closed. A review that names KK but where no KK
    checkout can be located fails closed too (KK_ROOT_NOT_FOUND_FAIL_CLOSED).
    """
    cwd = Path(cwd).resolve()
    roots = kk_roots(cwd, contract)
    out = dict(repo=str(roots[0][0] if roots else (toplevel(cwd) or cwd)), base=base, changed=changed,
               roots=[], required=[])
    for root, found_by in roots:
        if changed is not None:
            root_changed, root_base, source = relative_changed(cwd, root, changed), base, "explicit"
        else:
            root_base, source = base, "review-base"
            if root_base and not resolves(root, root_base):
                root_base, source = None, "review-base-not-in-repo"
            if not root_base and not base:
                root_base = contract_base(root, contract)
                source = "contract-range" if root_base else "none"
            root_changed = changed_files(root, root_base) if root_base else None
        out["roots"].append(dict(repo=str(root), found_by=found_by, base=root_base, base_source=source,
                                 changed=root_changed))
        if len(roots) == 1 and root_changed is not None:
            out["changed"] = root_changed
        for rule in RULES:
            if not (root / rule["marker"]).is_file():
                continue
            if root_changed is None:
                hits = []
                reason = ("REVIEW_BASE_NOT_IN_REPO_FAIL_CLOSED" if source == "review-base-not-in-repo"
                          else "REVIEW_BASE_UNKNOWN_FAIL_CLOSED")
            else:
                hits = rule_hits(rule, root, root_changed)
                reason = "GUARD_PATTERN_HIT" if hits else None
            if reason:
                out["required"].append(dict(id=rule["id"], repo=str(root), reason=reason, hits=hits,
                                            commands=list(rule["commands"]), why=rule["why"]))
    if not roots:
        hinted = bool(KK_HINT.search(contract or "")) or any(
            n.split("/", 1)[0] == "kk" for n in (changed or []))
        if hinted:
            for rule in RULES:
                out["required"].append(dict(id=rule["id"], repo=None, reason="KK_ROOT_NOT_FOUND_FAIL_CLOSED",
                                            hits=[], commands=list(rule["commands"]), why=rule["why"]))
    return out


def contract_section(selection: dict, ticket: str | None = None) -> str:
    if not selection["required"]:
        return ""
    lines = ["", "## 必跑检查（硬钩子 hostdag_review_required_checks，自动注入，不可删减）", ""]
    if ticket:
        lines.append(f"票号：{ticket}")
    if selection.get("review_reasons"):
        lines.append("判定为复审：" + "；".join(selection["review_reasons"]))
    bases = {r["repo"]: r for r in selection.get("roots", [])}
    for req in selection["required"]:
        hits = ", ".join(req["hits"][:20]) + (" …" if len(req["hits"]) > 20 else "")
        lines += ["", f"- 规则 {req['id']}（{req['reason']}）：{req['why']}。"]
        if req.get("repo"):
            root = bases.get(req["repo"], {})
            lines.append(f"  KK 检出：{req['repo']}；基线：{root.get('base') or '未定位（fail-closed，必跑）'}")
        else:
            lines.append("  复审涉及 KK 但派单 cwd、其直接子目录与合同路径都定位不到 KK 检出（fail-closed）："
                         "先找到被审 KK 工作树，在其根目录执行；找不到则 verdict 写 BLOCKED。")
        if hits:
            lines.append(f"  命中守卫覆盖文件：{hits}")
        lines.append("  在该 KK 检出根目录逐条执行，原始输出写入证据目录（cmd > file 2>&1），verdict 写明命令、退出码、日志路径：")
        lines += [f"  - `{c}`" for c in req["commands"]]
    lines += ["", "任一条缺跑、无日志、非 0 退出或 3.7 运行时不可用，verdict 不得 PASS（缺环境写 BLOCKED，失败写 FAIL）。", ""]
    return "\n".join(lines)


def augment(prompt: str, cwd, base: str | None = None, ticket: str | None = None,
            reasons: list[str] | None = None) -> tuple[str, dict]:
    selection = select(cwd, base, contract=prompt)
    if reasons:
        selection["review_reasons"] = list(reasons)
    return prompt + contract_section(selection, ticket), selection


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("select", "augment"):
        s = sub.add_parser(name)
        s.add_argument("--cwd", required=True)
        s.add_argument("--base")
        s.add_argument("--ticket")
        if name == "select":
            s.add_argument("--changed", nargs="*", help="explicit changed paths (relative to --cwd) instead of git diff")
            s.add_argument("--contract", help="review contract: KK paths and A..B ranges it names are probed")
        else:
            s.add_argument("--contract", required=True)
            s.add_argument("--out", required=True)
    a = p.parse_args(argv)
    if a.cmd == "select":
        contract = Path(a.contract).read_text() if a.contract else None
        result = select(a.cwd, a.base, a.changed, contract)
        print(json.dumps(result, ensure_ascii=False, indent=1))
        return 0
    text, result = augment(Path(a.contract).read_text(), a.cwd, a.base, a.ticket)
    Path(a.out).write_text(text)
    print(json.dumps(dict(out=a.out, required=[r["id"] for r in result["required"]]), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

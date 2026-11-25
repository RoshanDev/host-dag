"""B7 deployment preflight: read files only, never update wrapper, unit or contracts."""
from __future__ import annotations

import re
import shlex
from pathlib import Path

import hostdag_dag_core as c
from hostdag_dag_core import Json, JsonObj


def interpreter(path: Path, *, unit: bool = False) -> tuple[str | None, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, str(exc)
    if unit:
        commands = re.findall(r"^ExecStart=(.+)$", text, re.MULTILINE)
        if len(commands) != 1:
            return None, "need exactly one ExecStart (pass the effective unit file, including overrides)"
        words = shlex.split(commands[0])
    else:
        lines = text.splitlines()
        if lines and lines[0].strip() == "#!/bin/sh":
            commands = [line for line in lines if line.startswith("exec ")]
            words = shlex.split(commands[0][5:]) if len(commands) == 1 else []
        else:
            words = shlex.split(lines[0][2:]) if lines and lines[0].startswith("#!") else []
    if not words:
        return None, "interpreter not found"
    exe = words[0].replace("${HOME}", str(Path.home())).replace("$HOME", str(Path.home()))
    if unit:
        # Expand the user-unit home specifier, preserving literal %% sequences.
        exe = re.sub(r"%%|%h", lambda m: "%" if m[0] == "%%" else str(Path.home()), exe)
    return exe, ""


def check(wrapper: Path, unit: Path, executor_contract: Path | None = None,
          reviewer_contract: Path | None = None) -> JsonObj:
    checks: list[Json] = []
    for kind, path, is_unit in (("host_dag_interpreter", wrapper, False), ("observer_interpreter", unit, True)):
        exe, problem = interpreter(path, unit=is_unit)
        ok = exe is not None and Path(exe).resolve() == c.SM_PYTHON.resolve()
        checks.append({"check": kind, "path": str(path), "interpreter": exe, "ok": ok,
                       "detail": problem or ("pinned" if ok else f"must use {c.SM_PYTHON}")})
    contracts = {"executor": executor_contract, "reviewer": reviewer_contract}
    distinct = (executor_contract is not None and reviewer_contract is not None
                and executor_contract.resolve() != reviewer_contract.resolve())
    if executor_contract is not None and reviewer_contract is not None and distinct:
        try:
            distinct = not executor_contract.samefile(reviewer_contract)
        except OSError:
            pass  # Missing/unreadable files fail their own role check below.
    checks.append({"check": "contract_templates", "ok": distinct,
                   "detail": "pass distinct --executor-contract and --reviewer-contract files"})
    for role, contract_path in contracts.items():
        try:
            text = contract_path.read_text(encoding="utf-8") if contract_path is not None else ""
            ok = "HOST-DAG-RESULT:" in text or "HOST-DAG-RESULT-FILE:" in text
            detail = "result line present" if ok else "missing role template or HOST-DAG-RESULT line"
        except OSError as exc:
            ok, detail = False, str(exc)
        checks.append({"check": "contract_result_line", "role": role, "path": str(contract_path), "ok": ok, "detail": detail})
    checks.append({"check": "coordinator_invocation", "ok": None,
                   "detail": "advisor must verify coordinator uses host-dag, not python3 scripts/host-dag-view.py"})
    return {"ready": all(isinstance(row, dict) and row.get("ok") is not False for row in checks),
            "checks": checks, "writes": False,
            "manual": "§15.4/§16.3: sync core modules, restart observer with pinned interpreter; C1 gates remain required"}

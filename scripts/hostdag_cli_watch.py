"""Observe Cursor / Claude / Qoder / Reasonix tickets started by host-dag-cli-agent.py.

The watcher reads only the run's files and the transient unit's state; it
never contacts the agent, answers prompts or retries a run.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Callable

CLI_PROVIDERS = ("cursor", "claude", "qoder", "reasonix", "agy")
EVENT_KIND = {
    "COMPLETED": "completed",
    "FAILED": "failed",
    "CANCELLED": "cancelled",
    "QUOTA_EXHAUSTED": "attention",
    "AUTH_FAILED": "attention",
    "LOST": "failed",
    "START_FAILED": "failed",
}


def unit_active(unit: str) -> bool:
    done = subprocess.run(["systemctl", "--user", "is-active", unit], capture_output=True, text=True)
    return done.stdout.strip() == "active"


def _read(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def observe_record(rec: dict[str, Any], is_active: Callable[[str], bool] = unit_active) -> dict[str, Any]:
    run_file = Path(rec["run_file"])
    run = _read(run_file) or {}
    # An explicit path may be stale; the wrapper's conventional receipt is
    # still authoritative. A final receipt must never look like RUNNING.
    result = None
    for path in dict.fromkeys([Path(run.get("result") or run_file.with_name("cli-result.json")),
                               run_file.with_name("cli-result.json"), run_file.with_name("result.json")]):
        if path.is_file():
            result = _read(path) or {}
            break
    if result is not None:
        status = str(result.get("status") or "FAILED").upper()
        if status not in EVENT_KIND:
            status = "FAILED"
        text = result.get("resultText") or result.get("stderrTail") or ""
        summary = f"{rec.get('provider')} {status} exit={result.get('exitCode')}: {text}"
        return {"status": status, "summary": summary[:600], "finishedAt": result.get("finishedAt"),
                "credentialFault": result.get('credentialFault') if run.get('provider') == 'reasonix' else None}
    if run.get("provider") == "agy" and run.get("finished") is not None:
        # host-dag-agy-run.py writes no cli-result.json; its cli-run.json carries finished/exit_code.
        code = run.get("exit_code")
        status = "COMPLETED" if code == 0 else "FAILED"
        return {"status": status, "summary": f"agy {status} exit={code}", "finishedAt": run.get("finished")}
    if run.get("status") == "START_FAILED":
        return {"status": "START_FAILED", "summary": str(run.get("error") or "systemd-run failed")}
    unit = run.get("unit")
    if unit and is_active(unit):
        return {"status": "RUNNING", "summary": ""}
    if not unit and run.get("status") == "STARTING":
        return {"status": "RUNNING", "summary": ""}
    return {"status": "LOST", "summary": f"unit {unit} ended without {run_file.with_name('cli-result.json').name}"}


def event_kind(status: str) -> str | None:
    return EVENT_KIND.get(status)


def cli_fingerprint(snapshot: dict[str, Any]) -> str:
    key = json.dumps([snapshot.get("status"), snapshot.get("finishedAt"), snapshot.get("summary")], ensure_ascii=False)
    return hashlib.sha256(key.encode()).hexdigest()

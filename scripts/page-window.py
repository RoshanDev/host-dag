#!/usr/bin/env python3
"""Run one local page window from its spec with the shared `pagewindow` library.

  page-window.py check  <spec.json>   validate spec, steps and candidate artifacts; no locks, no cluster writes
  page-window.py run    <spec.json>   launch the window as a systemd unit whose ExecStopPost always closes it
  page-window.py exec   <spec.json>   (inside the unit) claim, switch, steps, drain, restore, release/HOLD
  page-window.py finally <spec.json>  (ExecStopPost) close a window the unit left open
  page-window.py recover <spec.json>  re-close a RECOVERY_REQUIRED window after its cause was fixed
  page-window.py recover <spec.json> --rebaseline <decision.json>
                                      same, when a declared unit's recorded original ran from an older env
                                      file (only env may differ; this recover must restart it from the
                                      window's baseline): {"unit","owner","claimId","by","at","why",
                                      "newOriginal":{"exe","exeSha256"},
                                      "libraryChange":{"from","to"} (only if the library changed since open)}
exec, finally and recover log in through the real form, so call them as
python3 page-window.py ... (the file is not executable). Without
Playwright (e.g. /usr/bin/python3), exec and recover refuse (exit 3) before reading or claiming
anything; finally still restores identity and configs, then ends RECOVERY_REQUIRED naming the interpreter.
  page-window.py status <spec.json>   print result.json
  page-window.py add-hold <spec.json> <hold.json>
                                      record this ticket's unfinished operation(s) on the page after the fact
  page-window.py clear-hold <spec.json> <why> [operation ...]
                                      this ticket finished/cancelled its HOLD operation(s)
  page-window.py clear-review <spec.json> <decision.json>
                                      free a legacy DRIFT_REVIEW page after the coordinator's decision
  page-window.py clear-kept <spec.json> <decision.json>
                                      drop a candidateKept hold on the advisor's decision
                                      ({"unit", "by", "at", "why", "newBaseline": {"exeSha256", "exe"}},
                                      checked against the live unit, free page only); audited in clearedHolds

The spec names its steps module (`"steps": "steps.py"`, relative to the spec). See
scripts/local-dev/pagewindow/README.md.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from pagewindow.locks import read_registry  # noqa: E402
from pagewindow.sysutil import BASE_ENV, which  # noqa: E402
from pagewindow.window import Window, apply_extensions, load_module, load_steps  # noqa: E402

PYTHON = os.environ.get("HOST_DAG_PAGE_WINDOW_PYTHON") or sys.executable
BROWSER_COMMANDS = ("exec", "finally", "recover")
"""Commands whose close logs in through the real form (Playwright). `run` starts them under PYTHON;
called by hand with another interpreter exec/recover are refused before anything is read or claimed
(10-19 KV1804-RECOVER-INTERPRETER: /usr/bin/python3 recover failed at restore-login); finally, the
safety net, still restores and then ends RECOVERY_REQUIRED with this reason."""


def interpreter_problem(command: str) -> str | None:
    if command not in BROWSER_COMMANDS or importlib.util.find_spec("playwright") is not None:
        return None
    done = ("the close restores identity/configs but cannot log in, so it ends RECOVERY_REQUIRED"
            if command == "finally" else "nothing was changed")
    return (f"page-window.py {command} needs Playwright, which {sys.executable} cannot import; {done}. Run it "
            f"with the browser-use interpreter: {PYTHON} {Path(__file__).resolve()} {command} <spec.json>")


def steps_of(window: Window):
    return load_steps((window.spec_path.parent / window.spec.get("steps", "steps.py")).resolve())


def check(window: Window) -> dict:
    report = {"owner": window.owner, "evidence": str(window.evidence.root), "problems": []}
    for key in ("ticket", "owner"):
        if not window.spec.get(key):
            report["problems"].append(f"spec.{key} missing")
    try:
        steps_path = (window.spec_path.parent / window.spec.get("steps", "steps.py")).resolve()
        module = load_module(steps_path)
        report["extensions"] = [n for n in ("PREFLIGHT", "OPEN_CHECKS", "RESTORE_HOOKS", "BROWSER_SETUP", "CHECKS",
                                            "CLOSE_AUDITS")
                                if getattr(module, n, None)]
        apply_extensions(window, module)
        steps = steps_of(window)
        report["steps"] = [{"name": s.name, "seconds": s.seconds} for s in steps]
        total = sum(s.seconds for s in steps)
        if total > window.spec["budget"]["stopNewStepAfter"]:
            report["problems"].append(f"steps need {total:g}s but budget.stopNewStepAfter is "
                                      f"{window.spec['budget']['stopNewStepAfter']}s; split the window")
    except Exception as exc:  # noqa: BLE001
        report["problems"].append(f"steps: {exc}")
    try:
        window.check_candidate()
    except SystemExit as exc:
        report["problems"].append(str(exc))
    for name in ("kubectl", "systemctl", "systemd-run", "flock", "nginx", "curl"):
        try:
            which(name)
        except FileNotFoundError as exc:
            report["problems"].append(str(exc))
    from pagewindow.protect import effect_problems
    from pagewindow import foreign
    report["problems"] += effect_problems(window.spec)
    from pagewindow import units as unitsmod
    report["problems"] += unitsmod.spec_problems(window.spec)
    if unitsmod.declared(window.spec):
        report["unitRestarts"] = unitsmod.declared(window.spec)
    shape = foreign.option_problems(window.spec)
    report["problems"] += shape
    if foreign.options(window.spec) is not None and not shape:
        # Offline: what the ledgers would derive now; live uid matching happens at the real PREFLIGHT.
        try:
            report["foreign"] = foreign.offline_report(window.spec, window.claims_path())
            report["problems"] += [f"foreign: {p}" for p in report["foreign"]["problems"]]
        except Exception as exc:  # noqa: BLE001
            report["problems"].append(f"foreign: ledgers unreadable: {type(exc).__name__}: {exc}"[:400])
    report["ok"] = not report["problems"]
    return report


def systemd_quote(word: str) -> str:
    """One word for a unit Exec line: double-quoted, with systemd's % and $ expansion escaped."""
    escaped = word.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%").replace("$", "$$")
    return f'"{escaped}"'


def launch(window: Window, spec: Path) -> int:
    window.check_grant()
    unit = window.locks.prefix
    budget = window.spec["budget"]
    runtime = int(budget["maxSeconds"])
    target_env = {**BASE_ENV, 'HOST_DAG_TARGET_CONFIG': window.target['configPath']}
    env = [f"--setenv={k}={v}" for k, v in target_env.items()] + ["--setenv=PYTHONDONTWRITEBYTECODE=1"]
    # ExecStopPost is bound by TimeoutStopSec; drain + restore can take far longer, so it only
    # starts a separate oneshot unit that does the close.
    words = [which("systemd-run"), "--user", "--no-block", "--collect", f"--unit={unit}-finally-{int(time.time())}",
             "--property=Type=oneshot", f"--property=TimeoutStartSec={int(budget['drainSeconds']) + 900}",
             *(f"--setenv={k}={v}" for k, v in target_env.items()),
             PYTHON, "-u", str(Path(__file__).resolve()), "finally", str(spec)]
    finally_cmd = " ".join(systemd_quote(w) for w in words)
    argv = [which("systemd-run"), "--user", "--no-block", "--collect", f"--unit={unit}-run",
            "--property=Type=exec", f"--property=RuntimeMaxSec={runtime}", "--property=TimeoutStopSec=30",
            f"--property=ExecStopPost={finally_cmd}", *env,
            PYTHON, "-u", str(Path(__file__).resolve()), "exec", str(spec)]
    proc = subprocess.run(argv, text=True, capture_output=True, timeout=60,
                          env=os.environ | BASE_ENV)
    window.root.write("launch.json", {"argv": argv, "exit": proc.returncode, "stdout": proc.stdout,
                                          "stderr": proc.stderr, "unit": f"{unit}-run"})
    print(json.dumps({"unit": f"{unit}-run", "exit": proc.returncode, "stderr": proc.stderr[-300:]}))
    return proc.returncode


def main(argv: list[str]) -> int:
    if argv[1:2] == ["clear-review"] and len(argv) == 4:
        window = Window(Path(os.path.abspath(Path(argv[2]).expanduser())))
        review = (read_registry(Path(window.spec["registry"])).get("review") or {})
        if review.get("runDir"):  # the run that was reviewed, not whatever ran later
            window.use_run_dir(Path(review["runDir"]), point=False)
        decision = json.loads(Path(argv[3]).read_text(encoding="utf-8"))
        if not decision.get("by") or not decision.get("decision"):
            print("decision.json needs by and decision")
            return 2
        window.locks.clear_review(decision)
        window.evidence.write("review-cleared.json", decision)
        return 0
    if argv[1:2] == ["clear-kept"] and len(argv) == 4:
        window = Window(Path(os.path.abspath(Path(argv[2]).expanduser())))
        decision = json.loads(Path(argv[3]).read_text(encoding="utf-8"))
        try:
            window.clear_kept(decision)
        except RuntimeError as exc:
            print(f"clear-kept refused: {exc}")
            return 2
        print(json.dumps({"cleared": decision.get("unit")}))
        return 0
    if argv[1:2] == ["add-hold"] and len(argv) == 4:
        window = Window(Path(os.path.abspath(Path(argv[2]).expanduser())))
        window.locks.add_hold(json.loads(Path(argv[3]).read_text(encoding="utf-8")))
        print(json.dumps({"added": True}))
        return 0
    if argv[1:2] == ["clear-hold"] and len(argv) >= 4:
        window = Window(Path(os.path.abspath(Path(argv[2]).expanduser())))
        cleared = window.locks.clear_hold(argv[3], argv[4:] or None)
        print(json.dumps({"cleared": cleared}))
        return 0 if cleared else 1
    rebaseline = None
    if argv[1:2] == ["recover"] and len(argv) == 5 and argv[3] == "--rebaseline":
        rebaseline = json.loads(Path(argv[4]).read_text(encoding="utf-8"))
        argv = argv[:3]
    if len(argv) != 3 or argv[1] not in ("check", "run", "exec", "finally", "recover", "status"):
        print(__doc__)
        return 2
    problem = interpreter_problem(argv[1])
    if problem and argv[1] != "finally":
        print(problem, file=sys.stderr)
        return 3
    spec = Path(os.path.abspath(Path(argv[2]).expanduser()))
    window = Window(spec)
    if problem:  # finally still restores identity/configs; the login it cannot do ends RECOVERY_REQUIRED
        print(problem, file=sys.stderr)
        window.no_browser = problem
    if argv[1] == "check":
        report = check(window)
        window.root.write("check.json", report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["ok"] else 1
    if argv[1] == "run":
        return launch(window, spec)
    if argv[1] == "exec":
        steps_path = (window.spec_path.parent / window.spec.get("steps", "steps.py")).resolve()
        apply_extensions(window, load_module(steps_path))
        result = window.execute(steps_of(window))
        return 75 if result.get("status") == "WAIT_RESOURCE" else 0
    if argv[1] == "finally":
        window.finally_close()
        return 0
    if argv[1] == "recover":
        result = window.recover_close(rebaseline)
        print(json.dumps({k: result.get(k) for k in ("outcome", "status", "recoveryAttempt")}, ensure_ascii=False))
        return 0 if result.get("outcome") != "RECOVERY_REQUIRED" else 1
    print(json.dumps(window.evidence.read("result.json"), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

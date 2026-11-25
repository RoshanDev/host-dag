#!/usr/bin/env python3
"""Run one long command as its own systemd --user unit and leave a receipt (Fable advisor/log.jsonl#L3378 §1 (2)(3)(5)).

Executors must not hold a turn open while a cut, image materialisation, big copy, cold install or full
build/test runs. They start the command here, print the one-line JSON this prints, and end the turn.
When the unit finishes (success, non-zero or signal) it writes exit_code atomically and one board
blocker line via `host-dag ticket set`; the watcher wakes the coordinator from that board write.
This script never messages or wakes any thread itself.

Commands:
  start  --ticket T --build B --label L [--attempt N] [--setenv K=V ...] [--memory-max 8G] [--cwd DIR]
         [--run RUN] [--batch BATCH] -- /abs/cmd args...
         Launch (or, for an attempt that already has started.json, replay without a second unit).
  check  --receipt DIR     DONE / RUNNING / ORPHANED / NOT_STARTED / LAUNCH_FAILED (coordinator use;
                           executors must not use it to poll progress).
  _run / _finalize --receipt DIR   internal: unit main process / ExecStopPost.

Receipt layout: <run>/evidence/longrun/<ticket>/<label>-<attempt>-<stamp>/
  started.json  stdout.log  stderr.log  exit_code  exit.json  board.json
Attempt claims: <run>/evidence/longrun/<ticket>/.claims/<label>-<attempt> (O_EXCL; replay key).
"""
from __future__ import annotations

import argparse
import fcntl
import datetime as dt
import json
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT = Path(__file__).resolve()
HOST_VIEW = SCRIPT.with_name("host-dag-view.py")
SKIP_STATUSES = {"DONE", "ACCEPTED", "CANCELLED", "SUPERSEDED", "FAILED"}
SAFE = re.compile(r"[^A-Za-z0-9_.-]+")
ENV_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
FREEZE_PREFIX = "FREEZE-EXEMPT"


def now_iso() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def stamp() -> str:
    return dt.datetime.now().astimezone().strftime("%Y%m%dT%H%M%S")


def safe(text: str) -> str:
    out = SAFE.sub("-", str(text)).strip("-.")
    if not out:
        raise ValueError(f"empty name after sanitising {text!r}")
    return out


def write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_json(path: Path, data: dict[str, Any]) -> None:
    write_atomic(path, json.dumps(data, ensure_ascii=False, indent=2) + "\n")


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def find_run(arg: str | None) -> Path:
    for cand in (arg, os.environ.get("HOST_DAG_RUN")):
        if cand:
            path = Path(cand).expanduser().resolve()
            if (path / "run.json").is_file():
                return path
            raise ValueError(f"no run.json in {path}")
    raise ValueError("pass --run <run dir> or set HOST_DAG_RUN")


def ticket_batch(run_dir: Path, ticket: str) -> str | None:
    """Same rule as host-dag-cli-agent.ticket_batch: last batch holding the ticket open, else last holding it."""
    run = read_json(run_dir / "run.json") or {}
    hits: list[tuple[bool, str]] = []
    for name, body in (run.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        if isinstance(tickets, list):
            tickets = {t.get("id"): t for t in tickets if isinstance(t, dict)}
        rec = tickets.get(ticket)
        if isinstance(rec, dict):
            hits.append((str(rec.get("status") or "").upper() not in SKIP_STATUSES, name))
    live = [n for ok, n in hits if ok]
    return (live or [n for _, n in hits] or [None])[-1]


def current_blocker(run_dir: Path, batch: str, ticket: str) -> str:
    run = read_json(run_dir / "run.json") or {}
    tickets = ((run.get("batches") or {}).get(batch) or {}).get("tickets") or {}
    if isinstance(tickets, list):
        tickets = {t.get("id"): t for t in tickets if isinstance(t, dict)}
    return str((tickets.get(ticket) or {}).get("blocker") or "")


def default_board_cmd() -> list[str]:
    found = shutil.which("host-dag")
    if found:
        return [str(Path(found).absolute())]
    return [sys.executable, str(HOST_VIEW)]


# ---------------------------------------------------------------- start

def claim_attempt(root: Path, label: str, attempt: int | None) -> tuple[int, Path, bool]:
    """Return (attempt, claim file, fresh). fresh=False means this attempt was claimed before (replay)."""
    claims = root / ".claims"
    claims.mkdir(parents=True, exist_ok=True)
    if attempt is not None:
        claim = claims / f"{label}-{attempt}"
        try:
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            return attempt, claim, False
        os.close(fd)
        return attempt, claim, True
    pat = re.compile(rf"^{re.escape(label)}-(\d+)$")
    n = max([int(m.group(1)) for p in claims.iterdir() if (m := pat.match(p.name))] or [0]) + 1
    while True:
        claim = claims / f"{label}-{n}"
        try:
            fd = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            n += 1
            continue
        os.close(fd)
        return n, claim, True


def cmd_start(a: argparse.Namespace) -> int:
    argv = list(a.cmd)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        raise ValueError("missing command after --")
    if not argv[0].startswith("/"):
        raise ValueError(f"command must be an absolute path (systemd-run-absolute-paths): {argv[0]!r}")
    env: dict[str, str] = {}
    for item in a.setenv or []:
        key, sep, val = item.partition("=")
        if not sep or not ENV_KEY.match(key):
            raise ValueError(f"--setenv wants K=V, got {item!r}")
        env[key] = val
    run_dir = find_run(a.run)
    ticket, label = safe(a.ticket), safe(a.label)
    root = run_dir / "evidence" / "longrun" / ticket
    attempt, claim, fresh = claim_attempt(root, label, a.attempt)
    prefix = safe(os.environ.get("HOST_DAG_LONGRUN_UNIT_PREFIX") or "host-dag-longrun")
    unit = f"{prefix}-{ticket}-{label}-{attempt}"
    if not fresh:
        receipt = Path(claim.read_text(encoding="utf-8").strip() or "/nonexistent")
        started = read_json(receipt / "started.json") if receipt.is_absolute() else None
        print(json.dumps({"unit": unit, "receipt_dir": str(receipt), "attempt": attempt, "replay": True,
                          "started": False, "has_started_json": started is not None}, ensure_ascii=False))
        return 0
    receipt = root / f"{label}-{attempt}-{stamp()}"
    receipt.mkdir(parents=True)
    write_atomic(claim, str(receipt) + "\n")
    cwd = str(Path(a.cwd).resolve()) if a.cwd else os.getcwd()
    uid = os.getuid()
    bus_env = {"XDG_RUNTIME_DIR": f"/run/user/{uid}",
               "DBUS_SESSION_BUS_ADDRESS": f"unix:path=/run/user/{uid}/bus"}
    board_cmd = list(a.board_cmd) if a.board_cmd else default_board_cmd()
    started = {
        "schema": "host-dag-longrun/v1", "ticket": a.ticket, "build": a.build, "label": a.label,
        "attempt": attempt, "unit": unit, "argv": argv, "cwd": cwd, "env_keys": sorted(env),
        "run": str(run_dir), "batch": a.batch, "receipt_dir": str(receipt), "memory_max": a.memory_max,
        "board_cmd": board_cmd, "python": sys.executable, "script": str(SCRIPT), "started_at": now_iso(),
    }
    write_json(receipt / "started.json", started)
    py, me = sys.executable, str(SCRIPT)
    srun = [a.systemd_run, "--user", f"--unit={unit}", "--collect", "--quiet",
            f"--working-directory={cwd}", "--property=KillMode=mixed",
            f"--property=ExecStopPost={py} {me} _finalize --receipt {receipt}"]
    if a.memory_max:
        srun.append(f"--property=MemoryMax={a.memory_max}")
    for k, val in {**bus_env, **env}.items():
        srun.append(f"--setenv={k}={val}")
    srun += ["--", py, me, "_run", "--receipt", str(receipt)]
    launch_env = {**os.environ, **bus_env}
    try:
        p = subprocess.run(srun, capture_output=True, text=True, timeout=60, env=launch_env)
        rc, out, err = p.returncode, p.stdout, p.stderr
    except (OSError, subprocess.SubprocessError) as exc:
        rc, out, err = 127, "", str(exc)
    if rc != 0:
        write_json(receipt / "launch_failed.json", {"at": now_iso(), "argv": srun, "exit": rc,
                                                     "stdout": out[-2000:], "stderr": err[-2000:]})
        print(json.dumps({"unit": unit, "receipt_dir": str(receipt), "attempt": attempt,
                          "error": "LAUNCH_FAILED", "detail": err.strip()[-300:]}, ensure_ascii=False))
        return 2
    print(json.dumps({"unit": unit, "receipt_dir": str(receipt), "attempt": attempt}, ensure_ascii=False))
    return 0


# ---------------------------------------------------------------- inside the unit

def exit_from_returncode(rc: int) -> int:
    return 128 - rc if rc < 0 else rc


def record_exit(receipt: Path, code: int, source: str, extra: dict[str, Any] | None = None) -> bool:
    """Write exit_code once (atomic rename). Returns False when it was already written."""
    if (receipt / "exit_code").exists():
        return False
    write_json(receipt / "exit.json", {"exit_code": code, "source": source, "finished_at": now_iso(), **(extra or {})})
    write_atomic(receipt / "exit_code", f"{code}\n")
    return True


def compose_blocker(line: str, old: str) -> str:
    """New LONGRUN-DONE line plus the whole old blocker, never truncated (the board keeps no blocker history).

    A first line starting with FREEZE-EXEMPT stays first and the new line goes second; otherwise the new line
    goes first. Dropping every LONGRUN-DONE line gives back the original blocker byte for byte."""
    if not old:
        return line
    if old.startswith(FREEZE_PREFIX):
        head, sep, rest = old.partition("\n")
        return head + "\n" + line + (sep + rest if sep else "")
    return line + "\n" + old


def notify_board(receipt: Path) -> dict[str, Any]:
    """One board write per attempt: board.lock is created O_EXCL; the loser does nothing."""
    try:
        fd = os.open(receipt / "board.lock", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return {"status": "ALREADY_NOTIFIED"}
    os.close(fd)
    s = read_json(receipt / "started.json") or {}
    code = (receipt / "exit_code").read_text().strip() if (receipt / "exit_code").exists() else "?"
    run_dir = Path(s.get("run") or "/nonexistent")
    batch = s.get("batch") or ticket_batch(run_dir, s.get("ticket", ""))
    line = (f"LONGRUN-DONE ticket={s.get('ticket')} build={s.get('build')} attempt={s.get('attempt')} "
            f"label={s.get('label')} exit={code} unit={s.get('unit')} receipt={receipt}")
    result: dict[str, Any] = {"at": now_iso(), "line": line, "batch": batch}
    if not batch:
        result.update(status="NO_BATCH", reason=f"{s.get('ticket')} not in {run_dir}/run.json")
    else:
        # Serialise read-old-blocker + write across all longrun units of this run: two units ending in the
        # same second otherwise both read the same old blocker and the later write drops the earlier line.
        lock_path = run_dir / "evidence" / "longrun" / ".board.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            old = current_blocker(run_dir, batch, s["ticket"])
            blocker = compose_blocker(line, old)
            argv = list(s.get("board_cmd") or default_board_cmd()) + [
                "--run", str(run_dir), "ticket", "set", "--batch", batch, "--ticket", s["ticket"], "--blocker", blocker]
            try:
                p = subprocess.run(argv, capture_output=True, text=True, timeout=120)
                result.update(status="WRITTEN" if p.returncode == 0 else "BOARD_WRITE_FAILED", exit=p.returncode,
                              stdout=p.stdout[-1000:], stderr=p.stderr[-1000:], argv=argv[:-1] + ["<blocker>"])
            except (OSError, subprocess.SubprocessError) as exc:
                result.update(status="BOARD_WRITE_FAILED", error=str(exc))
    write_json(receipt / "board.json", result)
    return result


def cmd_run(a: argparse.Namespace) -> int:
    receipt = Path(a.receipt)
    s = read_json(receipt / "started.json")
    if s is None:
        print(f"no started.json in {receipt}", file=sys.stderr)
        return 2
    if (receipt / "exit_code").exists():
        print("exit_code already present; refusing to rerun this attempt", file=sys.stderr)
        return 2
    child: subprocess.Popen | None = None

    def forward(signum, _frame):
        if child is not None and child.poll() is None:
            child.send_signal(signum)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, forward)
    with (receipt / "stdout.log").open("ab") as out, (receipt / "stderr.log").open("ab") as err:
        try:
            child = subprocess.Popen(s["argv"], cwd=s.get("cwd") or None, stdout=out, stderr=err,
                                     stdin=subprocess.DEVNULL)
        except OSError as exc:
            err.write(f"host-dag-longrun: exec failed: {exc}\n".encode())
            err.flush()
            record_exit(receipt, 127, "exec-failed", {"error": str(exc)})
            notify_board(receipt)
            return 127
        while True:
            try:
                rc = child.wait()
                break
            except InterruptedError:
                continue
    code = exit_from_returncode(rc)
    record_exit(receipt, code, "wrapper", {"signal": -rc if rc < 0 else None})
    notify_board(receipt)
    return code


def cmd_finalize(a: argparse.Namespace) -> int:
    """ExecStopPost: covers a wrapper killed before it could record (SIGKILL, OOM, systemctl kill)."""
    receipt = Path(a.receipt)
    if not (receipt / "exit_code").exists():
        kind, status = os.environ.get("EXIT_CODE", ""), os.environ.get("EXIT_STATUS", "")
        if kind == "exited" and status.isdigit():
            code = int(status)
        else:
            try:
                code = 128 + int(getattr(signal.Signals, "SIG" + status) if not status.isdigit() else status)
            except (AttributeError, ValueError):
                code = 255
        record_exit(receipt, code, "ExecStopPost", {"systemd_exit_code": kind, "systemd_exit_status": status,
                                                    "service_result": os.environ.get("SERVICE_RESULT")})
    notify_board(receipt)
    return 0


# ---------------------------------------------------------------- check

def cmd_check(a: argparse.Namespace) -> int:
    receipt = Path(a.receipt)
    s = read_json(receipt / "started.json")
    if s is None:
        print(json.dumps({"state": "NOT_STARTED", "receipt_dir": str(receipt)}))
        return 5
    base = {"receipt_dir": str(receipt), "unit": s.get("unit"), "attempt": s.get("attempt")}
    if (receipt / "exit_code").exists():
        board = read_json(receipt / "board.json") or {}
        print(json.dumps({**base, "state": "DONE", "exit_code": int((receipt / "exit_code").read_text().strip()),
                          "board": board.get("status")}, ensure_ascii=False))
        return 0
    if (receipt / "launch_failed.json").exists():
        print(json.dumps({**base, "state": "LAUNCH_FAILED"}))
        return 6
    try:
        active = subprocess.run([a.systemctl, "--user", "is-active", s["unit"]], capture_output=True,
                                text=True, timeout=10).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        active = f"unknown: {exc}"
    if active in ("active", "activating", "deactivating", "reloading"):
        print(json.dumps({**base, "state": "RUNNING", "unit_state": active}))
        return 0
    print(json.dumps({**base, "state": "ORPHANED", "unit_state": active}))
    return 4


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="host-dag-longrun.sh", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)
    s = sub.add_parser("start")
    s.add_argument("--ticket", required=True)
    s.add_argument("--build", required=True, help="build number, or - when none")
    s.add_argument("--label", required=True)
    s.add_argument("--attempt", type=int, help="explicit attempt number; reusing one replays (no second unit)")
    s.add_argument("--setenv", action="append", metavar="K=V")
    s.add_argument("--memory-max", help="systemd MemoryMax, e.g. 8G")
    s.add_argument("--cwd", help="working directory (default: current)")
    s.add_argument("--run", help="run directory (default: $HOST_DAG_RUN)")
    s.add_argument("--batch", help="board batch (default: looked up from run.json at unit end)")
    s.add_argument("--board-cmd", nargs="+", help=argparse.SUPPRESS)  # tests: fake host-dag
    s.add_argument("--systemd-run", default="/usr/bin/systemd-run", help=argparse.SUPPRESS)
    s.add_argument("cmd", nargs=argparse.REMAINDER)
    c = sub.add_parser("check")
    c.add_argument("--receipt", required=True)
    c.add_argument("--systemctl", default="/usr/bin/systemctl", help=argparse.SUPPRESS)
    for name in ("_run", "_finalize"):
        x = sub.add_parser(name)
        x.add_argument("--receipt", required=True)
    return p


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0].startswith("--"):
        argv = ["start"] + argv  # `host-dag-longrun.sh --ticket ...` == `start --ticket ...`
    a = parser().parse_args(argv)
    try:
        return {"start": cmd_start, "check": cmd_check, "_run": cmd_run, "_finalize": cmd_finalize}[a.command](a)
    except ValueError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    sys.exit(main())

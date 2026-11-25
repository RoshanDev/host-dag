#!/usr/bin/env python3
"""Keep host-dag-host-watch alive; run every 10 minutes by a user systemd timer.

The watcher writes <state>.heartbeat each poll cycle. When its unit is gone,
failed (exit 1/2 are not auto-restarted) or alive but silent, the watchdog
recreates it with the recorded command and alerts the desktop. It also starts
the Codex app-server when its socket is missing (e.g. after a reboot), through the
codex-app-server.service user unit when present, else `codex app-server daemon start`;
`daemon start` reuses an instance that is already running.

`install` records the current watcher command and writes the timer units.

`check-memory` (its own 1-minute timer, `install-memory`) only alerts: when available memory
or the user service's memory pressure nears what makes systemd-oomd kill a user cgroup
(2026-10-08 it killed herdr at 63% > 50% for 20 s). It never pauses or changes dispatch.
"""
from __future__ import annotations

from hostdag_json import atomic_json, public_path


import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hostdag_self_heal import alert  # noqa: E402

STALE_SECONDS = 10 * 60
# Coordinators stop the watcher for a few seconds to track a ticket; recheck before acting.
RECHECK_SECONDS = 30
AUTO_RESTART_WAIT = 90  # > the unit's RestartSec=60
UNIT_DIR = Path.home() / ".config/systemd/user"
TIMER = "host-dag-watch-watchdog"
CODEX_UNIT = os.environ.get("HOST_DAG_CODEX_UNIT", "codex-app-server.service")
MEMORY_TIMER = "host-dag-memory-watch"
LOW_AVAILABLE_GIB = 4.0
# 2026-11-02 Fable workstation-memory-20261102: one-off avg10 spikes while swap refilled were noise.
SUSTAINED_PRESSURE_AVG60 = 25.0  # sustained pressure alerts at once
SPIKE_PRESSURE_AVG10 = 40.0      # ...or avg10 this high on two checks in a row
CRITICAL_PRESSURE_AVG10 = 60.0   # oomd DefaultMemoryPressureLimit; alert on the first check
# oomd's SwapUsedLimit only applies to cgroups with ManagedOOMSwap=kill; on this desktop none are
# (oomctl "Swap Monitored CGroups" is empty), so a full swap is a notice, never an OOM alert (11-02).
FULL_SWAP_USED = 0.90
LOW_SWAP_FREE_GIB = 0.5
MEMORY_REMIND_SECONDS = 30 * 60
MEMORY_LEVELS = {"swap-low": 1, "warn": 2, "critical": 3}
CGROUP_ROOT = Path("/sys/fs/cgroup")
CODEX_SOCKET = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "app-server-control/app-server-control.sock"


def systemctl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, check=False)


def unit_state(unit: str) -> str:
    return systemctl("is-active", unit).stdout.strip() or "unknown"


def heartbeat_age(state_path: Path, now: float) -> float | None:
    beat = state_path.with_name(state_path.name + ".heartbeat")
    try:
        return now - float(beat.read_text().strip())
    except (OSError, ValueError):
        return None


def start_watcher(conf: dict) -> subprocess.CompletedProcess[str]:
    systemctl("reset-failed", conf["unit"])
    return subprocess.run([
        "systemd-run", "--user", f"--unit={conf['unit']}", f"--property=WorkingDirectory={conf['workdir']}",
        "--property=Restart=always", "--property=RestartSec=60", "--property=StartLimitIntervalSec=0",
        *conf["argv"],
    ], capture_output=True, text=True, check=False)


def ensure_codex_daemon(alerts: dict) -> str:
    if CODEX_SOCKET.exists():
        return "ok"
    codex = shutil.which("codex")
    if not codex:
        alert(alerts, "codexdaemon", "Codex app-server 未运行且找不到 codex 命令", str(CODEX_SOCKET), time.time())
        return "missing"
    # Prefer the dedicated user unit: a daemon started from this oneshot unit would live in its
    # cgroup (2026-10-08 the app-server died with herdr's cgroup; see codex-app-server.service).
    if subprocess.run(["systemctl", "--user", "cat", CODEX_UNIT], capture_output=True, text=True,
                      check=False).returncode == 0:
        done = subprocess.run(["systemctl", "--user", "start", CODEX_UNIT], capture_output=True, text=True, check=False)
    else:
        done = subprocess.run([codex, "app-server", "daemon", "start"], capture_output=True, text=True, check=False)
    ok = done.returncode == 0
    alert(alerts, "codexdaemon", "Codex app-server 已自动启动" if ok else "Codex app-server 启动失败",
          (done.stdout + done.stderr)[-400:], time.time())
    return "started" if ok else "start_failed"


TICKET = re.compile(r"^[A-Z][A-Z0-9]*-\d+[A-Za-z]?$")  # KV8-01, KVF-09, KVR-06, LD-22


def ticket_of(path: Path, evidence: Path) -> str:
    """The first path segment that looks like a ticket id; the evidence root depth varies by batch."""
    parts = path.relative_to(evidence).parts
    return next((part for part in parts if TICKET.match(part)), parts[0])


def scan_milestones(conf: dict, seen: dict, now: float) -> list[tuple[str, str, str]]:
    """Check only registered round/ticket directories, with bounded filesystem probes.

    The first scan only records a starting point so history is not replayed.
    """
    since = seen.get("last_scan")
    seen["last_scan"] = now
    if since is None:
        return []
    limit = 2000
    try:
        evidence = Path(os.path.abspath(conf["evidence"]))
        state_path = Path(conf["state"])
        watch = json.loads(state_path.read_text())
        run = json.loads(state_path.with_name("run.json").read_text())
        if not isinstance(watch, dict) or not isinstance(run, dict):
            return []
        tracked, batches = watch.get("tracked"), run.get("batches")
        if not isinstance(tracked, dict) or not isinstance(batches, dict):
            return []
        sources = {}

        def add_record(ticket: str, record: dict, fields: tuple[str, ...]) -> bool:
            if not isinstance(record, dict):
                return False
            for field in fields:
                value = record.get(field)
                if value is None or value == "":
                    continue  # Codex records may only carry a thread id.
                if not isinstance(value, str) or "\0" in value:
                    return False
                path = Path(value)
                if not path.is_absolute():
                    return False
                # Normalize lexically: resolve() would probe the slow filesystem.
                path = Path(os.path.abspath(path))
                if path != evidence and path.is_relative_to(evidence) and len(sources) < limit:
                    sources.setdefault(path, ticket)
            return True

        # One odd record (old relative evidence path, missing field) must not hide every other milestone.
        for name, record in tracked.items():
            add_record(str(name).split("-review-")[0], record, ("run_file", "evidence"))
        for batch in batches.values():
            if not isinstance(batch, dict) or not isinstance(batch.get("tickets"), dict):
                continue
            for ticket, record in batch["tickets"].items():
                add_record(ticket, record, ("evidence",))
    except (OSError, ValueError, TypeError, KeyError):
        return []

    candidates = {}
    stats = {}
    for source, ticket in sources.items():
        if len(candidates) >= limit:
            break
        try:
            stats[source] = source.stat()
        except OSError:
            stats[source] = None
        info = stats[source]
        round_dir = source.parent if info and info.st_mode & 0o170000 == 0o100000 else source
        if round_dir == evidence:
            continue
        ticket_dir = None
        for directory in (round_dir, *round_dir.parents):
            if directory == evidence:
                break
            if directory.name == ticket:
                ticket_dir = directory
                break
        for directory in dict.fromkeys((round_dir, ticket_dir or round_dir)):
            for name in ("parent-acceptance.json", "result.json"):
                if len(candidates) >= limit:
                    break
                candidates.setdefault(directory / name, (ticket, ticket_dir or round_dir))

    found = []
    for path, (ticket, ticket_dir) in candidates.items():
        try:
            info = stats[path] if path in stats else path.stat()
            # S_IFMT/S_IFREG, avoiding an additional is_file() stat.
            if info is None or info.st_mode & 0o170000 != 0o100000 or info.st_mtime <= since:
                continue
            record = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "")
        if path.name == "parent-acceptance.json" and status.startswith("ACCEPTED"):
            if path.parent.name == ticket:
                found.append((f"accepted:{ticket}:{status}", f"{ticket} 已验收", f"{status}\n{path.parent}"))
            else:
                stage = path.parent.relative_to(ticket_dir).as_posix()
                found.append((f"stage:{ticket}:{stage}:{status}", f"{ticket} 阶段验收（{stage}）",
                              f"{status}；可能是中途修复或某一轮，整票以票目录顶层的验收为准\n{path.parent}"))
        elif path.name == "result.json" and status == "BLOCKED":
            found.append((f"blocked:{ticket}:{int(info.st_mtime)}", f"{ticket} 被阻塞，需要处理",
                          f"见 {path.parent}/report.md"))
    return found


def milestone_alert(alerts: dict, key: str, title: str, body: str, now: float) -> bool:
    """A BLOCKED result is the coordinator's to handle and is rewritten on every background step:
    log it under one key per ticket and never pop it on the desktop (user, 10-29: "又弹了两次了")."""
    if key.startswith("blocked:"):
        return alert(alerts, key.rsplit(":", 1)[0], title, body, now, desktop=False)
    return alert(alerts, key, title, body, now)


def cmd_check(args: argparse.Namespace) -> int:
    conf = json.loads(Path(args.config).read_text())
    state_path = Path(conf["state"])
    now = time.time()
    memo_path = Path(args.config).with_suffix(".alerts.json")
    memo = json.loads(memo_path.read_text()) if memo_path.exists() else {}
    alerts = {"state_path": str(state_path), "alerts_sent": memo}
    report = {"at": now, "codex": ensure_codex_daemon(alerts), "unit": unit_state(conf["unit"])}
    stale = conf.get("stale", STALE_SECONDS)

    def is_healthy(at: float) -> tuple[bool, float | None]:
        beat = heartbeat_age(state_path, at)
        return report["unit"] == "active" and beat is not None and beat < stale, beat

    healthy, age = is_healthy(now)
    if not healthy:
        args.sleep(RECHECK_SECONDS)
        now = time.time()
        report["unit"] = unit_state(conf["unit"])
        healthy, age = is_healthy(now)
        report["rechecked"] = True
    if not healthy and report["unit"] == "activating":
        # systemd's own Restart=always (RestartSec=60) is pending, e.g. after a host-CLI
        # TIMEOUT exit 3; racing it made start_watcher fail and popped a false "重启失败" (11-03).
        for _ in range(AUTO_RESTART_WAIT // RECHECK_SECONDS):
            args.sleep(RECHECK_SECONDS)
            report["unit"] = unit_state(conf["unit"])
            if report["unit"] != "activating":
                break
        now = time.time()
        if report["unit"] == "active":
            report["autoRestarted"] = True
            alert(alerts, "watcher", "Host DAG watcher 已由 systemd 自动重启",
                  "unit was activating (Restart=always); no action taken", now, desktop=False)
            healthy, age = True, heartbeat_age(state_path, now)
    report["heartbeatAge"] = age
    if not healthy:
        reason = f"unit={report['unit']} heartbeat_age={'none' if age is None else int(age)}s"
        if report["unit"] == "active":
            systemctl("stop", conf["unit"])  # alive but silent: a hung loop, not a slow one
        tail = subprocess.run(["journalctl", "--user", "-u", conf["unit"], "-n", "5", "--no-pager"],
                              capture_output=True, text=True, check=False).stdout[-600:]
        started = start_watcher(conf)
        # A coordinator may have restarted it in the meantime; the unit being up is what counts.
        report["restarted"] = started.returncode == 0 or unit_state(conf["unit"]) == "active"
        alert(alerts, "watcher", "Host DAG watcher 已自动重启" if report["restarted"] else "Host DAG watcher 重启失败",
              f"{reason}\n{tail}\n{started.stderr[-200:]}", now)
    if conf.get("evidence"):
        seen_path = Path(args.config).with_suffix(".milestones.json")
        seen = json.loads(seen_path.read_text()) if seen_path.exists() else {}
        report["milestones"] = []
        for key, title, body in scan_milestones(conf, seen, now):
            milestone_alert(alerts, key, title, body, now)
            report["milestones"].append(title)
        atomic_json(seen_path, seen)
    atomic_json(memo_path, alerts["alerts_sent"])
    print(json.dumps(report, ensure_ascii=False))
    return 0


def user_service_cgroup() -> Path:
    return CGROUP_ROOT / f"user.slice/user-{os.getuid()}.slice/user@{os.getuid()}.service"


def memory_status(meminfo: str, pressure: str) -> dict:
    """Available GiB, swap-free GiB and the 'some' avg10/avg60 pressure of the user service."""
    kib = {m.group(1): int(m.group(2)) for m in re.finditer(r"^(\w+):\s+(\d+) kB", meminfo, re.M)}
    some = re.search(r"^some avg10=([\d.]+) avg60=([\d.]+)", pressure, re.M)
    return {"available_gib": round(kib.get("MemAvailable", 0) / 2**20, 1),
            "swap_free_gib": round(kib.get("SwapFree", 0) / 2**20, 1),
            "swap_total_gib": round(kib.get("SwapTotal", 0) / 2**20, 1),
            "pressure_avg10": float(some.group(1)) if some else 0.0,
            "pressure_avg60": float(some.group(2)) if some else 0.0}


def top_cgroups(root: Path, limit: int = 5) -> list[str]:
    """Largest leaf-ish cgroups under the VM slice and the user's app/session slices."""
    found = []
    for parent in (root / "machine.slice", user_service_cgroup() / "app.slice", user_service_cgroup() / "session.slice"):
        for child in parent.glob("*"):
            try:
                found.append((int((child / "memory.current").read_text()), child.name))
            except (OSError, ValueError):
                continue
    return [f"{size / 2**30:.1f}G {name.replace(chr(92) + 'x2d', '-')}"
            for size, name in sorted(found, reverse=True)[:limit]]


def check_memory(alerts: dict, memo: dict, status: dict, now: float, top: list[str]) -> str | None:
    """Alert on entering a risky state, at once on escalation, else at most every 30 minutes.

    Returns the reason or None."""
    state = memo.setdefault("memory", {})
    p10, p60 = status["pressure_avg10"], status["pressure_avg60"]
    spike = p10 >= SPIKE_PRESSURE_AVG10
    total = status.get("swap_total_gib", 0)
    swap_used = (total - status["swap_free_gib"]) / total if total else 0.0
    critical, warn, info = [], [], []
    if p10 >= CRITICAL_PRESSURE_AVG10:
        critical.append(f"用户服务内存压力 avg10 {p10}% ≥ {CRITICAL_PRESSURE_AVG10:g}%（oomd 持续 20 秒即杀进程）")
    if status["available_gib"] < LOW_AVAILABLE_GIB:
        warn.append(f"可用内存 {status['available_gib']}G < {LOW_AVAILABLE_GIB:g}G")
    if p60 >= SUSTAINED_PRESSURE_AVG60:
        warn.append(f"用户服务内存压力 avg60 {p60}% ≥ {SUSTAINED_PRESSURE_AVG60:g}%")
    elif spike and state.get("spike"):
        warn.append(f"用户服务内存压力 avg10 连续两次 ≥ {SPIKE_PRESSURE_AVG10:g}%（{p10}%）")
    if total and (swap_used >= FULL_SWAP_USED or status["swap_free_gib"] < LOW_SWAP_FREE_GIB):
        info.append(f"swap 已用 {swap_used:.0%}，剩余 {status['swap_free_gib']}G（内存压力正常时不影响使用）")
    level = "critical" if critical else "warn" if warn else "swap-low" if info else None
    reasons = critical + warn + info
    if level is None:
        if state.get("since"):
            alert(alerts, "memory-recovered", "内存已恢复", f"可用 {status['available_gib']}G，"
                  f"压力 avg10 {p10}%", now, desktop=False)
        memo["memory"] = {"spike": spike} if spike else {}
        return None
    escalated = MEMORY_LEVELS[level] > MEMORY_LEVELS.get(state.get("level"), 0)
    if escalated or now - float(state.get("last", 0)) >= MEMORY_REMIND_SECONDS:
        tail = f"\nswap 剩余 {status['swap_free_gib']}G\n占用最多：" + "，".join(top) + \
            "\n只告警，不暂停派工；需要时关闭桌面应用或挂起不用的 VM。"
        if level == "swap-low":
            # 11-02 user: full swap with free RAM is normal (cold pages), so it is logged, never a desktop notice;
            # swap only matters once pressure rises, and then the warn/critical alerts below fire.
            alert(alerts, "memory-swap-low", "swap 快用完", "；".join(reasons) + tail, now, desktop=False)
        else:
            key = "memory-critical" if level == "critical" else "memory"
            alert(alerts, key, "内存紧张，可能触发 OOM", "；".join(reasons) + tail, now)
        state["last"] = now
    state["level"], state["spike"] = level, spike
    state.setdefault("since", now)
    return "；".join(reasons)


def cmd_check_memory(args: argparse.Namespace) -> int:
    conf = json.loads(Path(args.config).read_text())
    memo_path = Path(args.config).with_suffix(".memory.json")
    memo = json.loads(memo_path.read_text()) if memo_path.exists() else {}
    alerts_memo_path = Path(args.config).with_suffix(".alerts.json")
    sent = json.loads(alerts_memo_path.read_text()) if alerts_memo_path.exists() else {}
    alerts = {"state_path": conf["state"], "alerts_sent": sent}
    try:
        pressure = (user_service_cgroup() / "memory.pressure").read_text()
    except OSError:
        pressure = Path("/proc/pressure/memory").read_text()
    status = memory_status(Path("/proc/meminfo").read_text(), pressure)
    now = time.time()
    reason = check_memory(alerts, memo, status, now, top_cgroups(CGROUP_ROOT))
    atomic_json(memo_path, memo)
    atomic_json(alerts_memo_path, alerts["alerts_sent"])
    print(json.dumps({**status, "alert": reason}, ensure_ascii=False))
    return 0


def cmd_install_memory(args: argparse.Namespace) -> int:
    config = public_path(args.config)
    UNIT_DIR.mkdir(parents=True, exist_ok=True)
    (UNIT_DIR / f"{MEMORY_TIMER}.service").write_text(
        "[Unit]\nDescription=Alert before systemd-oomd kills a user cgroup\n\n[Service]\nType=oneshot\n"
        f"ExecStart=/usr/bin/python3 {Path(__file__).resolve()} check-memory --config {config}\n")
    (UNIT_DIR / f"{MEMORY_TIMER}.timer").write_text(
        "[Unit]\nDescription=Check memory every minute (alert only)\n\n[Timer]\nOnBootSec=2min\n"
        "OnUnitActiveSec=1min\nAccuracySec=5s\n\n[Install]\nWantedBy=timers.target\n")
    systemctl("daemon-reload")
    enabled = systemctl("enable", "--now", f"{MEMORY_TIMER}.timer")
    print(json.dumps({"ok": enabled.returncode == 0, "config": str(config), "timer": f"{MEMORY_TIMER}.timer",
                      "stderr": enabled.stderr.strip()}, ensure_ascii=False))
    return 0 if enabled.returncode == 0 else 1


def cmd_install(args: argparse.Namespace) -> int:
    props = systemctl("show", args.unit, "-p", "ExecStart", "-p", "WorkingDirectory").stdout
    argv_text = next((line for line in props.splitlines() if line.startswith("ExecStart=")), "")
    start = argv_text.find("argv[]=")
    if start < 0:
        print(json.dumps({"ok": False, "error": f"cannot read ExecStart of {args.unit}; is it running?"}))
        return 2
    argv = argv_text[start + 7:].split(" ; ")[0].split()
    workdir = next(line.split("=", 1)[1] for line in props.splitlines() if line.startswith("WorkingDirectory="))
    state = argv[argv.index("--state") + 1]
    config = Path(args.config or Path(state).with_name("watch-unit.json"))
    atomic_json(config, {"unit": args.unit, "workdir": workdir, "argv": argv, "state": state,
                                  "stale": args.stale, "evidence": args.evidence})
    UNIT_DIR.mkdir(parents=True, exist_ok=True)
    (UNIT_DIR / f"{TIMER}.service").write_text(
        "[Unit]\nDescription=Keep host-dag-host-watch alive\n\n[Service]\nType=oneshot\n"
        f"ExecStart=/usr/bin/python3 {Path(__file__).resolve()} check --config {config}\n")
    (UNIT_DIR / f"{TIMER}.timer").write_text(
        "[Unit]\nDescription=Check host-dag-host-watch every 10 minutes\n\n[Timer]\nOnBootSec=3min\n"
        "OnUnitActiveSec=10min\nPersistent=true\n\n[Install]\nWantedBy=timers.target\n")
    systemctl("daemon-reload")
    enabled = systemctl("enable", "--now", f"{TIMER}.timer")
    print(json.dumps({"ok": enabled.returncode == 0, "config": str(config), "argv": argv,
                      "timer": f"{TIMER}.timer", "stderr": enabled.stderr.strip()}, ensure_ascii=False))
    return 0 if enabled.returncode == 0 else 1


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    check = sub.add_parser("check")
    check.add_argument("--config", required=True)
    check.set_defaults(func=cmd_check, sleep=time.sleep)
    install = sub.add_parser("install")
    install.add_argument("--unit", required=True, help="running watcher unit, e.g. host-dag-watch-20260926")
    install.add_argument("--config")
    install.add_argument("--stale", type=int, default=STALE_SECONDS)
    install.add_argument("--evidence", help="run evidence root to announce ticket acceptances and BLOCKED results")
    install.set_defaults(func=cmd_install)
    mem = sub.add_parser("check-memory", help="alert only: low available memory or high user memory pressure")
    mem.add_argument("--config", required=True)
    mem.set_defaults(func=cmd_check_memory)
    imem = sub.add_parser("install-memory", help="write and enable the 1-minute memory alert timer")
    imem.add_argument("--config", required=True, help="watch-unit.json written by install")
    imem.set_defaults(func=cmd_install_memory)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())

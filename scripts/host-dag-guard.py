#!/usr/bin/env python3
"""Watch a production host during a risky test window and page the user when it stops answering.

Safety guard for risky tests on a managed host: no step-by-step approvals, but
Telegram must call the user when the host needs a physical or BMC reboot. Checks the
host (ICMP + SSH port) and one guest that lives on it; pages with an `urgent:` alert,
which Telegram delivers even during quiet hours.

usage: host-dag-guard.py --state <run>/watch.json --host <host-ip> \\
           --guest <guest-ip-on-that-host> --bmc <bmc-ip> [--interval 30] [--down-after 180]
"""
from __future__ import annotations

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hostdag_self_heal import alert  # noqa: E402


def reachable(ip: str) -> bool:
    ping = subprocess.run(["ping", "-c1", "-W2", ip], capture_output=True, check=False).returncode == 0
    try:
        with socket.create_connection((ip, 22), timeout=3):
            return True
    except OSError:
        return ping


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", required=True)
    p.add_argument("--host", required=True)
    p.add_argument("--guest", required=True, help="a VM running on the host, as a liveness proxy")
    p.add_argument("--bmc", default="")
    p.add_argument("--interval", type=int, default=30)
    p.add_argument("--down-after", type=int, default=180)
    args = p.parse_args(argv)
    memo = {"state_path": args.state, "alerts_sent": {}}
    host_down_since: float | None = None
    guest_down_since: float | None = None
    paged = False
    while True:
        now = time.time()
        host_ok, guest_ok = reachable(args.host), reachable(args.guest)
        print(time.strftime("%H:%M:%S"), f"host={host_ok} guest={guest_ok}", flush=True)
        if not host_ok:
            host_down_since = host_down_since or now
            if now - host_down_since >= args.down_after:
                minutes = int((now - host_down_since) / 60)
                alert(memo, f"urgent:{args.host}-down", f"{args.host} 宿主已 {minutes} 分钟无响应，可能需要去机房重启",
                      f"ping 和 SSH 都不通。kernel.panic=30 本应自动重启，超过 {args.down_after // 60} 分钟仍未恢复，"
                      f"多半是卡死。请通过 BMC {args.bmc or '(未知)'} 或到机房断电重启。恢复后请检查宿主机上的虚机状态。",
                      now)
                paged = True
        else:
            if paged:
                alert(memo, f"urgent:{args.host}-back", f"{args.host} 宿主已恢复响应",
                      "宿主已经可以 ping 和 SSH。请核对宿主机、虚机和测试残留。", now)
                paged = False
            host_down_since = None
            if not guest_ok:
                guest_down_since = guest_down_since or now
                if now - guest_down_since >= args.down_after:
                    alert(memo, f"urgent:{args.host}-guest-down", f"{args.host} 上的云主机 {args.guest} 无响应",
                          "宿主在线，但云主机连不上，可能是宿主网络或虚拟化层出了问题，请留意。", now)
            else:
                guest_down_since = None
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())

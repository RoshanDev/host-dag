#!/usr/bin/env python3
"""Tell the user a DAG coordinator stopped and needs a person.

Coordinators call this whenever they stop dispatching for a reason only a
person can clear (unreadable quota, a decision, missing credentials), so the
stop is never silent. Logged to <run>/alerts.jsonl and shown on the desktop
every time it is called.

usage: host-dag-alert.py --state <run>/watch.json --key KV7-01-quota "标题" "说明"
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hostdag_self_heal import alert  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", required=True, help="the run's watch.json; alerts.jsonl is written beside it")
    p.add_argument("--key", required=True, help="dedupe key, e.g. <ticket>-<reason>")
    p.add_argument("title")
    p.add_argument("body")
    args = p.parse_args(argv)
    memo = {"state_path": args.state, "alerts_sent": {}}
    shown = alert(memo, f"coordinator:{args.key}", args.title, args.body, time.time())
    print(f'{{"ok": true, "desktop": {str(shown).lower()}}}')
    return 0


if __name__ == "__main__":
    sys.exit(main())

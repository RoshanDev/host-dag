#!/usr/bin/env python3
"""Run one Antigravity CLI (agy) executor turn so the host-dag board can follow it.

11-19: agy executors were invisible on the board (no duration, last reply or log). This wrapper runs
`agy -p --output-format stream-json`, stamps every record with `ts` (epoch seconds; agy records carry
none) into <out>/cli.ndjson, and writes <out>/cli-run.json naming the systemd unit, so the board's CLI
lookup (cli-run.json -> log) and AgyTimeline work like they do for Cursor/Claude/Grok executors.

Usage (inside a systemd user unit named host-dag-cli-agy-<TICKET>-MMDDHHMMSS):
  host-dag-agy-run.py --unit <unit> --out <evidence dir> --cwd <worktree> --model gemini-4-neon-high \
      [--add-dir DIR ...] --prompt-file <file>
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

AGY = os.environ.get("HOST_DAG_AGY", str(Path.home() / ".local/bin/agy"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--unit", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--cwd", required=True, type=Path)
    ap.add_argument("--model", required=True)
    ap.add_argument("--add-dir", action="append", default=[])
    ap.add_argument("--prompt-file", required=True, type=Path)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    log = args.out / "cli.ndjson"
    cmd = [AGY, "-p", args.prompt_file.read_text(), "--model", args.model, "--dangerously-skip-permissions",
           "--output-format", "stream-json", "--log-file", str(args.out / "agy.log")]
    for d in args.add_dir:
        cmd += ["--add-dir", d]
    run = {"unit": args.unit, "provider": "agy", "model": args.model, "cwd": str(args.cwd), "log": str(log),
           "prompt_file": str(args.prompt_file), "started": time.time()}
    (args.out / "cli-run.json").write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")

    with open(log, "a", encoding="utf-8") as sink, open(args.out / "cli.ndjson.stderr", "ab") as err:
        proc = subprocess.Popen(cmd, cwd=args.cwd, stdout=subprocess.PIPE, stderr=err, text=True, bufsize=1)
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if isinstance(rec, dict):
                    rec["ts"] = time.time()
                    line = json.dumps(rec, ensure_ascii=False)
            except json.JSONDecodeError:
                line = json.dumps({"event": "raw", "text": line, "ts": time.time()}, ensure_ascii=False)
            sink.write(line + "\n")
            sink.flush()
        code = proc.wait()
    run.update(finished=time.time(), exit_code=code)
    (args.out / "cli-run.json").write_text(json.dumps(run, ensure_ascii=False, indent=2) + "\n")
    return code


if __name__ == "__main__":
    sys.exit(main())

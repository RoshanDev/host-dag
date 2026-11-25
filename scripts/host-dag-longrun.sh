#!/bin/sh
# Long commands as their own systemd --user unit; prints {unit, receipt_dir, attempt} and returns at once.
# Usage: host-dag-longrun.sh --ticket T --build B --label L [--attempt N] [--setenv K=V ...] [--memory-max 8G]
#                           [--run RUN] -- /abs/cmd args...
#        host-dag-longrun.sh check --receipt DIR
# Contract: Fable advisor/log.jsonl#L3378; AGENTS.md「长命令一律 longrun」. Details: scripts/hostdag_longrun.py.
set -eu
uid=$(id -u)
export XDG_RUNTIME_DIR="/run/user/$uid"
export DBUS_SESSION_BUS_ADDRESS="unix:path=/run/user/$uid/bus"
here=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
py=${HOST_DAG_LONGRUN_PYTHON:-$(command -v python3)}
exec "$py" "$here/hostdag_longrun.py" "$@"

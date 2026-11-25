# Example coordinator dispatch helpers for host-dag.
# Source this file after exporting:
#   HOST_DAG_RUN  - absolute path to the DAG run directory (contains run.json, watch.json)
#   DAG_BATCH    - batch name in run.json
#   DAG_TOOLS    - path to host-dag scripts directory (or repo root containing scripts/)
: "${HOST_DAG_RUN:?HOST_DAG_RUN must be set}"
: "${DAG_BATCH:?DAG_BATCH must be set}"
: "${DAG_TOOLS:?DAG_TOOLS must be set}"

R="$HOST_DAG_RUN"
if [ -f "$DAG_TOOLS/host-dag-view.py" ]; then
  _DAG_SCRIPTS="$DAG_TOOLS"
elif [ -f "$DAG_TOOLS/scripts/host-dag-view.py" ]; then
  _DAG_SCRIPTS="$DAG_TOOLS/scripts"
else
  _DAG_SCRIPTS="$DAG_TOOLS"
fi

bset() { timeout 60 python3 "$_DAG_SCRIPTS/host-dag-view.py" --run "$R" ticket set "$@"; }
review_axis() { case "$1" in *[Ss][Tt][Aa][Nn][Dd][Aa][Rr][Dd]*|*-CODE*|*-code*) echo standards;; *) echo spec;; esac; }
codex_go() { # ticket cwd outdir effort trackkey; one stable evidence directory per dispatch intent
  local ROLE=executor AXIS=spec
  case "$5" in *-REVIEW*|*-review*) ROLE=reviewer; AXIS=$(review_axis "$5");; esac
  HOST_DAG_RUN=$R "${HOST_DAG_CODEX_HOST:-$HOME/.local/bin/host-dag-codex-host}" thread start --service-tier default --cwd "$2" --model gpt-6.2-sol --effort "$4" --sandbox danger-full-access --approval-policy never --guard-run "$R" --guard-ticket "$1" --dispatch-role "$ROLE" --review-axis "$AXIS" --review-token "$3" --message-file "$3/contract.md" > "$3/dispatch.json" 2>"$3/dispatch.err" || return $?
  local T TOKEN
  T=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["thread"]["id"])' "$3/dispatch.json") || return $?
  TOKEN=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("reviewBudget",{}).get("token",""))' "$3/dispatch.json") || return $?
  python3 "$_DAG_SCRIPTS/host-dag-watch.py" --state "$R/watch.json" track --ticket "$5" --thread "$T" --evidence "$3" >/dev/null 2>&1 || return $?
  [ "$ROLE" != reviewer ] || rv_register "$1" "$T" "$TOKEN" "$AXIS" || return $?
  echo "$T"; }
rv_register() { # ticket thread-or-unit [delivered-token] [axis]; registration cannot mint budget
  local B
  B=$(python3 -c 'import json,sys;d=json.load(open(sys.argv[1]));m=[n for n,b in d["batches"].items() if sys.argv[2] in (b.get("tickets") or {})];assert len(m)==1,"ambiguous/missing ticket";print(m[0])' "$R/run.json" "$1") || return $?
  if [ -n "$3" ]; then
    bset --batch "$B" --ticket "$1" --review "${4:-spec}=$2" --review-token "$3" >/dev/null
  else
    bset --batch "$B" --ticket "$1" --review "${4:-spec}=$2" >/dev/null
  fi; }
opus_rv() { # ticket cwd outdir trackkey
  local U TOKEN RUNFILE AXIS
  AXIS=$(review_axis "$4")
  python3 "$_DAG_SCRIPTS/host-dag-cli-agent.py" run --provider claude --role reviewer --model claude-opus-5-5 --effort high --ticket "$1" --cwd "$2" --prompt-file "$3/contract.md" --out "$3/cli" --run "$R" --batch "$DAG_BATCH" --no-register --review-token "$3" --review-axis "$AXIS" > "$3/cli-start.json" 2> "$3/cli-start.err" || return $?
  U=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["unit"])' "$3/cli-start.json") || return $?
  TOKEN=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["reviewBudget"]["token"])' "$3/cli-start.json") || return $?
  RUNFILE=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["runFile"])' "$3/cli-start.json") || return $?
  python3 "$_DAG_SCRIPTS/host-dag-watch.py" --state "$R/watch.json" track-cli --ticket "$4" --provider claude --run-file "$RUNFILE" --evidence "$3" >/dev/null 2>&1 || return $?
  rv_register "$1" "$U" "$TOKEN" "$AXIS" || return $?
  echo "$U"; }
advisor_go() { # key title questionfile [extra args] — absolute paths; systemd-run cwd is $HOME otherwise
  local G
  G=$(dirname "$_DAG_SCRIPTS")
  systemctl --user reset-failed "host-dag-advisor-$1" 2>/dev/null
  systemd-run --user --working-directory="$G" --unit "host-dag-advisor-$1" sh -c "python3 $_DAG_SCRIPTS/host-dag-advisor.py --state $R/watch.json --key $1 --title '$2' --question-file $3 ${*:4} > $3.out.json"; }

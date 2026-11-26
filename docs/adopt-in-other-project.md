# 在任意项目中使用 host-dag（含空项目最小端到端演示）

本文提供一套可直接复制执行的最小端到端演示：在独立临时目录中初始化一个空 Git 仓库，跑通完整链路：

`建批建票 → 依赖守卫 → worktree 派发 → host-dag-longrun.sh 托管执行与回执 → host-dag-watch.py 唤醒协调者 → 合并验收 → 导出依赖图 → 自动清场`

## 隔离边界说明

- 演示通过正式环境变量隔离，不会污染你正在运行的任何 DAG 或全局状态：
  - `HOST_DAG_RUNS_INDEX="$DEMO/runs-index.json"`：不写入 `~/.local/state/host-dag/runs.json`
  - `HOST_DAG_LONGRUN_UNIT_PREFIX="hostdag-demo-longrun"`：所有 longrun 单元均带 `hostdag-demo-` 前缀
  - `HOST_DAG_WATCH_NOTIFY=0` 与 `HOST_DAG_TELEGRAM_CONFIG=/dev/null`：不弹桌面通知、不发 Telegram
  - `HOST_DAG_CODEX_SERVER_PID_FILE="$DEMO/nonexistent.pid"`：不探测宿主机现网 app-server
- 命令包裹在子 shell `(...)` 中并注册 `EXIT`/`INT`/`TERM` `trap`：无论正常完成、超时还是中途失败，均会自动停止并清理全部 `hostdag-demo-*` 单元和 `$DEMO` 目录并读回确认。

## 一键运行空项目端到端演示

请在 `host-dag` 仓库根目录使用 `bash` 执行（或保存为 `.sh` 文件后 `bash <file>` 执行）：

```bash
(
set -euo pipefail
TS=$(date +%Y%m%dT%H%M%S)
export DEMO="$HOME/.cache/hostdag-demo-$TS"
export REPO="$DEMO/repo" HOST_DAG_RUN="$DEMO/run" DAG_BATCH="DEMO-B1"
export DAG_TOOLS="${DAG_TOOLS:-$PWD/scripts}"
DISPATCH_LIB="${DISPATCH_LIB:-$DAG_TOOLS/dag-dispatch-lib.example.sh}"
DEMO_TIMEOUT="${DEMO_TIMEOUT:-30}"

if [ ! -f "$DAG_TOOLS/host-dag-longrun.sh" ]; then
  echo "ERROR: DAG_TOOLS=$DAG_TOOLS 缺少 host-dag-longrun.sh（请在 host-dag 仓库根目录执行或显式 export DAG_TOOLS）" >&2
  exit 1
fi
if [ ! -f "$DISPATCH_LIB" ]; then
  echo "ERROR: DISPATCH_LIB=$DISPATCH_LIB 不存在" >&2
  exit 1
fi

export HOST_DAG_RUNS_INDEX="$DEMO/runs-index.json"
export HOST_DAG_LONGRUN_UNIT_PREFIX="hostdag-demo-longrun"
export HOST_DAG_WATCH_NOTIFY=0
export HOST_DAG_TELEGRAM_CONFIG=/dev/null
export HOST_DAG_CODEX_SERVER_PID_FILE="$DEMO/nonexistent.pid"
export TMPDIR="$DEMO/tmp"

cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  set +e
  local u rem_units="" clean_deadline=$((SECONDS + 10))
  while :; do
    while read -r u _; do
      [ -n "$u" ] && systemctl --user stop "$u" 2>/dev/null || true
    done <<< "$(systemctl --user list-units --all --full --no-legend --plain 'hostdag-demo-*')"
    systemctl --user reset-failed 'hostdag-demo-*' 2>/dev/null || true
    rem_units=$(systemctl --user list-units --all --full --no-legend --plain 'hostdag-demo-*')
    [ -z "$rem_units" ] && break
    [ "$SECONDS" -ge "$clean_deadline" ] && break
    sleep 0.2
  done
  rm -rf "$DEMO"
  local cleanup_err=0
  if [ -n "$rem_units" ]; then
    echo "ERROR: 残留单元: $rem_units" >&2
    cleanup_err=1
  fi
  if [ -e "$DEMO" ]; then
    echo "ERROR: 演示目录未删净: $DEMO" >&2
    cleanup_err=1
  fi
  if [ "$cleanup_err" -eq 0 ]; then
    echo "cleanup readback OK: hostdag-demo-* empty, $DEMO absent (exit=$rc)"
  else
    rc=1
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -p "$REPO" "$HOST_DAG_RUN/evidence" "$DEMO/fake-host/threads" "$TMPDIR"

# 1) 初始化空 Git 仓库与测试协调者桩
git -C "$REPO" init -b main
git -C "$REPO" config user.email "demo@example.com"
git -C "$REPO" config user.name "Demo User"
printf "# demo\n" > "$REPO/README.md"
git -C "$REPO" add README.md && git -C "$REPO" commit -m "init"

cat > "$DEMO/fake-host/coordinator-stub.py" <<'PY'
#!/usr/bin/env python3
import datetime as dt, json, os, sys
from pathlib import Path
root = Path(os.environ["DEMO_FAKE_HOST_DIR"]); argv = sys.argv[1:]
if argv[:1] == ["goal"]:
    print(json.dumps({"changed": False, "goal": None})); sys.exit(0)
if len(argv) >= 3 and argv[0] == "thread":
    act, tid = argv[1], argv[2]
    if act == "read":
        print(json.dumps({"threadId": tid, "messages": [], "nextCursor": None} if "--view" in argv else json.loads((root / "threads" / f"{tid}.json").read_text())))
        sys.exit(0)
    if act == "send":
        msg = argv[argv.index("--message") + 1] if "--message" in argv else Path(argv[argv.index("--message-file") + 1]).read_text()
        with (root / "wakeups.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": dt.datetime.now().astimezone().isoformat(), "thread": tid, "message": msg}, ensure_ascii=False) + "\n")
        print(json.dumps({"threadId": tid, "status": "completed", "delivered": True})); sys.exit(0)
    if act == "cancel":
        print(json.dumps({"threadId": tid, "status": "cancelled"})); sys.exit(0)
sys.exit(1)
PY
chmod +x "$DEMO/fake-host/coordinator-stub.py"
printf '{"threadId":"parent-coord","status":"completed","result":{"turnId":"t0","text":"ready"}}\n' > "$DEMO/fake-host/threads/parent-coord.json"

# 2) 加载配置驱动派发库，初始化 run.json，建批并登记 2 张依赖票（DEMO-B 依赖 DEMO-A）
printf '{"batches":{}}\n' > "$HOST_DAG_RUN/run.json"
printf '{}\n' > "$HOST_DAG_RUN/watch.json"
# shellcheck disable=SC1090
source "$DISPATCH_LIB"
python3 "$DAG_TOOLS/host-dag-view.py" --run "$HOST_DAG_RUN" batch set --batch "$DAG_BATCH" --title "Empty Project Demo"
bset --batch "$DAG_BATCH" --ticket DEMO-A --title "Module A" --kind tool --state CREATED
bset --batch "$DAG_BATCH" --ticket DEMO-B --title "Module B depends on A" --kind tool --dep DEMO-A --state WAITING_DEPENDENCY

# 验证依赖守卫：DEMO-A 未验收前，DEMO-B 拒绝 ACCEPTED
if bset --batch "$DAG_BATCH" --ticket DEMO-B --kind tool --state ACCEPTED --candidate "DEMO:invalid" 2>/dev/null; then
  echo "ERROR: dependency guard did not reject DEMO-B before DEMO-A" >&2
  exit 1
fi
echo "dependency guard OK: DEMO-B rejected while DEMO-A not ACCEPTED"

# 3) 用官方 init 初始化 watch.json 并启动隔离 watcher
python3 "$DAG_TOOLS/host-dag-watch.py" --state "$HOST_DAG_RUN/watch.json" init \
  --parent-thread parent-coord --host-cli "$DEMO/fake-host/coordinator-stub.py" \
  --host-cwd "$REPO" --poll-interval 2 --direction-interval 360000 --settle-seconds 0 --run-receipt "$HOST_DAG_RUN/run.json"

systemd-run --user --unit=hostdag-demo-watch --collect --working-directory="$REPO" \
  --setenv="DEMO_FAKE_HOST_DIR=$DEMO/fake-host" --setenv="HOST_DAG_RUN=$HOST_DAG_RUN" \
  --setenv="HOST_DAG_RUNS_INDEX=$HOST_DAG_RUNS_INDEX" --setenv="HOST_DAG_WATCH_NOTIFY=0" \
  --setenv="HOST_DAG_TELEGRAM_CONFIG=/dev/null" --setenv="TMPDIR=$TMPDIR" \
  --setenv="HOST_DAG_CODEX_SERVER_PID_FILE=$HOST_DAG_CODEX_SERVER_PID_FILE" \
  -- /usr/bin/python3 "$DAG_TOOLS/host-dag-watch.py" --state "$HOST_DAG_RUN/watch.json" watch

# 4) 按依赖顺序执行单票：派发 -> longrun 执行 -> cli-result.json -> watcher 唤醒 -> 合并验收
run_demo_ticket() {
  local T=$1 WT=$2 BR=$3 LBL=$4 CODE_GEN=$5 U="hostdag-demo-exec-$1" E="$HOST_DAG_RUN/evidence/$1"
  mkdir -p "$E"; git -C "$REPO" worktree add "$WT" -b "$BR" main
  bset --batch "$DAG_BATCH" --ticket "$T" --state RUNNING --worktree "$WT" --executor "$U" --evidence "$E"
  printf '{"provider":"claude","ticket":"%s","unit":"%s","status":"STARTING","result":"%s/cli-result.json"}\n' "$T" "$U" "$E" > "$E/cli-run.json"
  cat > "$E/exec.sh" <<EXEC
#!/usr/bin/env bash
set -euo pipefail
cd "$WT"; $CODE_GEN
SJ=\$("$DAG_TOOLS/host-dag-longrun.sh" start --ticket "$T" --build demo --label "$LBL" --cwd "$WT" --run "$HOST_DAG_RUN" --batch "$DAG_BATCH" --board-cmd "$DAG_TOOLS/host-dag-view.py" --setenv "HOST_DAG_RUNS_INDEX=$HOST_DAG_RUNS_INDEX" --setenv "HOST_DAG_LONGRUN_UNIT_PREFIX=$HOST_DAG_LONGRUN_UNIT_PREFIX" --setenv "TMPDIR=$TMPDIR" -- /usr/bin/python3 -m unittest discover -s "$WT" -p "test_*.py")
RC=\$(python3 -c 'import json,sys;print(json.loads(sys.argv[1])["receipt_dir"])' "\$SJ")
DEADLINE=\$((SECONDS + $DEMO_TIMEOUT))
until [ "\$(python3 -c 'import json,sys;print(json.loads(sys.argv[1]).get("state",""))' "\$("$DAG_TOOLS/host-dag-longrun.sh" check --receipt "\$RC" || true)")" = "DONE" ]; do
  if [ "\$SECONDS" -ge "\$DEADLINE" ]; then
    echo "ERROR: longrun timeout waiting for DONE (\$RC)" >&2
    exit 1
  fi
  sleep 0.3
done
EC=\$(tr -d '[:space:]' < "\$RC/exit_code")
python3 -c 'import datetime as dt,json,sys;from pathlib import Path;Path(sys.argv[1]).write_text(json.dumps({"status":"COMPLETED" if sys.argv[3]=="0" else "FAILED","exitCode":int(sys.argv[3]),"finishedAt":dt.datetime.now().astimezone().isoformat(timespec="seconds"),"resultText":f"{sys.argv[2]} exit={sys.argv[3]}"},indent=2)+"\n")' "$E/cli-result.json" "$T" "\$EC"
EXEC
  chmod +x "$E/exec.sh"
  systemd-run --user --unit="$U" --collect --working-directory="$WT" \
    --setenv="HOST_DAG_RUN=$HOST_DAG_RUN" --setenv="HOST_DAG_RUNS_INDEX=$HOST_DAG_RUNS_INDEX" \
    --setenv="HOST_DAG_LONGRUN_UNIT_PREFIX=$HOST_DAG_LONGRUN_UNIT_PREFIX" --setenv="TMPDIR=$TMPDIR" \
    -- "$E/exec.sh"
  python3 "$DAG_TOOLS/host-dag-watch.py" --state "$HOST_DAG_RUN/watch.json" track-cli --ticket "$T" --provider claude --run-file "$E/cli-run.json" --evidence "$E"
  local WAKE_DEADLINE=$((SECONDS + DEMO_TIMEOUT))
  until grep -q "$T" "$DEMO/fake-host/wakeups.jsonl" 2>/dev/null; do
    if [ "$SECONDS" -ge "$WAKE_DEADLINE" ]; then
      echo "ERROR: watcher wakeup timeout waiting for $T" >&2
      exit 1
    fi
    sleep 0.5
  done
  git -C "$REPO" merge --no-ff "$BR" -m "merge $BR ($T)"
  bset --batch "$DAG_BATCH" --ticket "$T" --kind tool --state ACCEPTED --candidate "DEMO:$(git -C "$WT" rev-parse HEAD)"
}

run_demo_ticket DEMO-A "$DEMO/wt-a" feat/demo-a unit-a 'printf "def g(x): return f\"hi {x}\"\n" > mod_a.py; printf "import unittest, mod_a\nclass T(unittest.TestCase):\n def test_g(self): self.assertEqual(mod_a.g(\"dag\"), \"hi dag\")\n" > test_a.py; git add .; git commit -m "DEMO-A"'
run_demo_ticket DEMO-B "$DEMO/wt-b" feat/demo-b unit-b 'printf "import mod_a\ndef s(x): return mod_a.g(x).upper()\n" > mod_b.py; printf "import unittest, mod_b\nclass T(unittest.TestCase):\n def test_s(self): self.assertEqual(mod_b.s(\"dag\"), \"HI DAG\")\n" > test_b.py; git add .; git commit -m "DEMO-B"'

python3 "$DAG_TOOLS/host-dag-view.py" --run "$HOST_DAG_RUN" graph -o -
cat "$DEMO/fake-host/wakeups.jsonl"
)
```

## 接入真实项目的建议步骤

1. 将本仓库安装为 Agent Skill（保留 `SKILL.md`、`references/`、`scripts/`），或直接把 `scripts/` 加入项目的 `DAG_TOOLS`。
2. 在协调者会话中导出 `HOST_DAG_RUN`、`DAG_BATCH`、`DAG_TOOLS`，并 `source scripts/dag-dispatch-lib.example.sh`。
3. 如需接入真实 Codex app-server、Claude CLI、Antigravity ACP 或 Grok CLI，参见 `SKILL.md`、`references/codex-app-server.md` 与 `references/observer.md`。
4. 如需手机端接收验收/紧急告警并直接回复协调者，配置 `~/.config/host-dag/telegram.env`（或设置 `HOST_DAG_TELEGRAM_CONFIG`）并启用 `scripts/host-dag-telegram-bridge.py`。

## 按项目配置的部分

下面三处由各项目自己声明，公开版默认不带任何主机名或发布脚本：

- **冻结环境标记**（`scripts/local-dev/board_freeze.py`）：`Board-Freeze-Environment:` / `Target-Environment:` 可以写任意 `[A-Za-z0-9_.-]+` 标签（如 `local`、`staging`、`host-a`）。冻结标记和票的环境不同就互不阻塞。
- **遗留资源按主机分组**（`scripts/hostdag_leftover_ledger.py`）：资源对象里写 `host:<名字>` 就按该名分组；也可以设环境变量 `HOST_DAG_LEFTOVER_HOSTS=alpha,beta`，按子串匹配。都没有的归为 `host needs review`。
- **降级方案 b 的硬钩子**（`scripts/hostdag_lint_actions.py`）：在 `<run>/control/degrade-hooks.json`（或 `HOST_DAG_DEGRADE_HOOKS` 指向的文件）里声明允许直接执行的入口脚本和它们接受的参数：

  ```json
  {
    "entries_root": "/abs/path/to/your/tools",
    "entries": {
      "release.py": {"positionals": ["package"], "options": {"--run": 1, "--dry-run": 0}, "run_flag": "--run"}
    },
    "heads_repos": ["APP"],
    "materials": ["manifest"],
    "lock": "local-heavy-admission"
  }
  ```

  `options` 的值是参数个数（0 或 1），没声明的参数一律拒绝。`run_flag` 必须指向当前 run 目录。`heads_repos` 列出 inputs 里要和 `control/release-heads.json` 对上的仓库键，`materials` 列出要算 sha256 进证明的文件键。没有这个文件时，方案 b 直接拒绝，a/c/NONE 照常可用。

## 执行前守卫（可选）

记忆或文档里的「不要做 X」反复被违反时，可以改成执行前拦截。`scripts/hostdag_pretool_guard.py` 是 Claude Code / Codex 通用的 PreToolUse hook：放行时 exit 0、不输出；拒绝时 exit 2，原因写到 stderr；脚本自身出错也按拒绝处理（故障即阻断）。它不改写命令，可以和 rtk 这类改写 hook 并存。

- **规则文件**：按 `--config PATH` > `$HOST_DAG_GUARD_CONFIG` > `~/.config/host-dag/pretool-guard.json` 的顺序找。没有规则文件时全部放行；规则文件读不了或格式错算守卫故障。样例见 `scripts/pretool-guard.example.json`：
  - `git`：`{"subcommand": "stash", "allow": ["list", "show"]}` 只放行这两个子命令；`{"subcommand": "push", "deny_if_args_any": ["--force", "-f"]}` 只在带这些参数时拦；只写 `subcommand` 时整条拦。能识别 `git -C`、绝对路径的 git、`cd … &&`、`timeout`/`env`/`rtk` 前缀和一层 `bash -c`；`echo`、提交信息、heredoc 正文里出现的不拦。
  - `search`：`paths` 是绝对路径，每段可用 `*`。递归内容搜索（`grep -r/-R/--recursive`、`rg`、Claude 的 Grep 工具）的目标正好是其中某一项就拦；`deny_ancestors`（默认 true）把这些目录的上级目录也算进去。更深的子目录和单个文件不拦。
  - `message` 写替代做法，`{target}` 会换成被拦的路径。拒绝记录写到 `log`（JSONL，设为空串则不记）。
- **安装**：把脚本复制到不会随切分支消失的位置（如 `~/.local/libexec/`），配置里写绝对路径。故障即阻断的 hook 一旦脚本文件不见了，所有命令都会被拦。
  - Claude：`~/.claude/settings.json` → `hooks.PreToolUse` 加 `{"matcher": "Bash|Grep", "hooks": [{"type": "command", "command": "/usr/bin/python3 /abs/hostdag_pretool_guard.py --config /abs/pretool-guard.json", "timeout": 10}]}`。
  - Codex：`~/.codex/hooks.json` → `hooks.PreToolUse` 加同样的条目（matcher 用 `Bash`）。**追加到已有条目之后**：Codex 按「文件:事件:条目序号:hook 序号」记信任，插到前面会让后面的 hook 全部失去信任。随后在 Codex 里登记信任（TUI 里确认，或用 app-server `hooks/list` 读出 `currentHash` 写进 `config.toml` 的 `hooks.state."<key>".trusted_hash`）。
- **自检**：`scripts/hostdag_guard_selfcheck.py` 检查三件事：Claude 配置里有这个 hook；Codex `hooks/list` 显示它已启用且已信任；拿它要拦的命令（默认 `git stash`）实际跑一遍确实被拒，拿 `true` 跑一遍确实放行。**Codex 不会运行未信任的 hook，而且跳过时不报错**：改条目顺序、改命令行或 Codex 自动升级都可能让守卫失效。所以每次改 hook 配置或升级之后都要跑一次自检，最好再挂一个定时器定期跑（失败时 exit 1）。
- **覆盖不到的地方**：脚本内部的命令、`xargs`、`find -exec`、用变量拼出的路径、嵌套 3 层以上的 shell，以及不走这两个客户端 hook 的执行者（如 ACP）。

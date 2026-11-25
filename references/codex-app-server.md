# Codex 执行者与 app-server 托管

适用于用原生 Codex app-server 承载协调者和执行者会话的项目。附带的 `scripts/host-dag-codex-host.py` 是该 app-server 的命令行适配器，也是观察器的 `--host-cli`。

## 适配器

依赖 `websockets`。入口脚本 `scripts/host-dag-codex-host` 默认使用 `${XDG_DATA_HOME:-~/.local/share}/host-dag-codex-host/venv/bin/python`，可用 `HOST_DAG_CODEX_PYTHON` 覆盖：

```sh
python3 -m venv ~/.local/share/host-dag-codex-host/venv
~/.local/share/host-dag-codex-host/venv/bin/pip install websockets
ln -s <skill-root>/scripts/host-dag-codex-host ~/.local/bin/host-dag-codex-host
host-dag-codex-host thread read <parent-thread-id>
```

派工与续接：

```sh
host-dag-codex-host thread start --cwd <ticket-worktree> --model <id> --effort <id> \
  --sandbox danger-full-access --approval-policy never --message-file <contract>
host-dag-codex-host thread send <thread-id> --message-file <rework>
host-dag-codex-host thread cancel <thread-id> --turn-id <active-turn-id>
```

- `thread start` 总会附带 `EXECUTOR_CONFIG`：关闭 `chrome@openai-bundled`、`browser@openai-bundled`、`unified-computer-use@openai-bundled` 插件和 `node_repl` MCP。执行者会话里因此没有能接管用户桌面浏览器的工具。不要在派工时去掉它。
- 只有用户对某票要求限制写入时才用 `--sandbox workspace-write --writable-root <path>`（可重复），之后每次 `thread send` 都要带同样的 `--writable-root`。`thread send --sandbox danger-full-access` 把已受限的会话一次性切为不受限。
- `THREAD_BUSY` 表示提交被拒，不会排队。等对方空闲后重发。

## app-server 必须独立托管

app-server 如果由终端复用器（tmux、herdr 等）里的 Codex TUI 拉起，就和该复用器在同一个 cgroup。systemd-oomd 按内存压力杀掉那个 cgroup 时，app-server 会一起死，所有运行中的回合都变成 `cancelled`。`codex app-server daemon bootstrap` 不使用 systemd，不能解决这个问题。

用一个用户单元持有它：

`~/.local/libexec/codex-app-server-supervise`（可执行）：

```sh
#!/bin/sh
codex=$HOME/.local/bin/codex
pidfile=$HOME/.codex/app-server-daemon/app-server.pid
alive() {
  pid=$(sed -n 's/^{"pid":\([0-9]*\).*/\1/p' "$pidfile" 2>/dev/null)
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}
"$codex" app-server daemon start || exit 1
# An auto-update swaps the pid; only give up after 45s without a live server.
misses=0
while [ "$misses" -lt 3 ]; do
  if alive; then misses=0; else misses=$((misses + 1)); fi
  sleep 15
done
exit 1
```

`~/.config/systemd/user/codex-app-server.service`：

```ini
[Unit]
Description=Codex managed app-server (own cgroup)

[Service]
Type=simple
ExecStart=%h/.local/libexec/codex-app-server-supervise
ExecStop=%h/.local/bin/codex app-server daemon stop
KillMode=mixed
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

切换步骤：选在没有回合运行时进行。先 `codex app-server daemon stop`，确认没有残留的 `codex app-server` 与 `pid-update-loop` 进程（残留的要停掉，否则它们以后会在旧 cgroup 里重新拉起服务），然后 `systemctl --user enable --now codex-app-server.service`。核对：

```sh
for p in $(pgrep -f "codex app-server"); do echo "$p $(sed 's#.*/##' /proc/$p/cgroup)"; done
codex app-server daemon version
```

执行者启动的命令是 app-server 的子进程，与它同一 cgroup；重型构建和全量测试用 `systemd-run --user -p MemoryMax=<limit>` 单独托管，避免把这个单元推成 oomd 的首选目标。

## 服务重启与自动续跑

Codex 的自动更新和 oomd 都会让 app-server 换进程。观察器每轮读取 `~/.codex/app-server-daemon/app-server.pid` 里的身份（pid、启动 tick、bootId；测试或非默认位置用 `HOST_DAG_CODEX_SERVER_PID_FILE`）。在旧服务上运行过的回合，到新服务上变成 `cancelled`，就按服务重启处理：最多自动续跑 2 次，并向父协调者说明这不是用户取消。只有被取消的正是之前看到在运行的那一回合，才这样判断；新回合被取消仍交父协调者。

看门狗（`scripts/host-dag-watchdog.py`）发现控制 socket 不在时，存在 `codex-app-server.service`（可用 `HOST_DAG_CODEX_UNIT` 改名）就用 `systemctl --user start` 拉起，否则退回 `codex app-server daemon start`。

---
name: host-dag
description: 用原生 Codex app-server 或已登录的 Grok Build CLI 执行按 DAG 拆分的任务，固定模型、票据工作树和会话身份，并用附带的观察器、自愈与看门狗唤醒原协调者。
---

# Host DAG

多 Agent 任务的本地优先 DAG 编排接线：用原生 Codex app-server、Claude CLI、Antigravity ACP 或 Grok CLI 执行按 DAG 拆分的票据，固定工作树与会话身份，并通过 systemd 长命令托管、观察器、自愈与看门狗唤醒原协调者。

## 准备与归属

- 先读目标项目的 AGENTS、有效任务合同和原 run receipt，核对主机、票据、工作树、Base、活动写入者和最新用户决定。继续原任务，不恢复无关暂停任务。
- 使用已安装的 `dag` 技能维护依赖、固定 Base、单写者、候选审查和验收；本技能不创建第二个调度器。DAG、合同及派工由协调者完成，执行者只写其票据范围。
- 新任务只传精简合同、文件指针和验收条件，不带协调者历史。执行者默认不再派生子代理；独立审查需要时由协调者统一派发。
- 不因使用本技能创建 Goal。只有用户要求创建或暂停 Goal 时才执行相应动作。没有 Goal 的任务照常派工。

## Grok 入口

先执行 `grok models`、`grok --version` 和目标目录的 `inspect --json`，确认已登录、模型可用、技能可见。具体模型以当前用户要求为准，不从项目旧示例或全局 default 推断。用户指定 `grok-4.7` 就显式传该 ID；服务不支持时返回证据，不降级到 fast 或旧模型。

本技能附带 `scripts/host-dag-grok.py`，保留文件名以兼容现有观察器。直接用 Python 执行，不依赖它是否有可执行位，也不依赖原项目目录。它只通过官方 `grok agent ... stdio` 的 ACP 派工，不用 `grok -p` 代替。

```sh
python3 <skill-root>/scripts/host-dag-grok.py run \
  --cwd <ticket-worktree> --model <explicit-model> --effort <explicit-effort> \
  --permission-mode <authorized-mode> \
  --prompt-file <ticket-contract> --log <original-run/ticket.ndjson>
```

`--model`、`--effort` 必填。初始化时核对模型广告的 effort；session/new 或 load 后读回 reasoning_effort，不符则显式设置并再次读回。`bypassPermissions` 映射为 `--always-approve`，只在当前任务已有该权限授权时使用；它不扩大业务写入范围。

目录未信任时，仅在当前已授权工作目录用官方 `grok --trust --cwd <path> inspect --json` 完成信任并确认 `projectTrusted=true`；不批量信任其他目录。不得修改模型默认、登录、全局 hooks 或旧项目设置。

记录 `WRAPPER_PID`、Grok `sessionId`、实际 model/effort、日志、进程持有者与退出状态。Grok sessionId 与 Codex threadId 分开。结果不明先读回，不重复派工。同票返工用 `resume --session-id <original-id>` 和原工作树；取消只向 `WRAPPER_PID` 发 SIGINT，等 ACP 的真实 stopReason。

## Codex 入口

Codex 执行者经附带的 app-server 适配器派工，接线、权限参数和服务托管见 [references/codex-app-server.md](references/codex-app-server.md)。

- 新票用 `thread start`，返工用原会话 `thread send`，不新开同范围写入者。读回 threadId、turn.id、cwd、model、reasoningEffort、sandbox、approvalPolicy；结果不明先读回，不重派。
- 执行者权限与用户全局 Codex 配置保持一致。全局若是不受限且 approval never，派工也用 `danger-full-access` / `never`：续接时会套用全局配置，受限派工只会让首回合卡在没人处理的审批上。边界写在票据合同里。用户对某票明确要求限制时才用 `workspace-write` 加 `--writable-root`，之后每次续接都要带上。
- 执行者会话不带任何能接管用户桌面浏览器的工具（适配器的 `EXECUTOR_CONFIG`）。用户自己的浏览器装着密码管理器等扩展，会在密码框上弹出菜单，让自动化被拒、票据停下等人。只写规则拦不住，需要从工具层面关掉。浏览器验收用项目提供的无扩展浏览器（系统 Chrome、临时 profile、`--disable-extensions`），仍走真实登录表单。
- 不要从另一个 TUI 或客户端同时向协调者会话发消息；原生 `turn/start` 没有“仅空闲时提交”的保护。

## 后台与事件

- 长操作由现有进程宿主或 systemd 持有，日志存原 run。确认启动身份后结束派工回合，不循环读日志或调用模型等待。
- 完成、失败、待处理事件由普通观察器报告；正常运行不发“继续”。每份 run 仅一个观察器，观察器不验收、不解锁票据。
- 只有父线程已通过当前 Codex 宿主读回时才能绑定自动唤醒。不要创建替代协调者，不把桌面或其他客户端线程冒充该宿主线程。
- 可用时复用附带的 `scripts/host-dag-watch.py`（依赖同目录的 `hostdag_grok_watch.py`、`hostdag_self_heal.py`、`hostdag_cli_watch.py`）和看门狗 `host-dag-watchdog.py`，接入、自愈与告警见 [references/observer.md](references/observer.md)。没有可用唤醒入口时，记录后台句柄与此限制，由用户下次消息续接原 run；不假称自动续接已接通。
- 每次唤醒只处理原 run 的新事件；已处理或过期通知去重登记后结束。避免反复读取完整历史、全仓 diff 或实时日志。

## 无人值守与宿主稳定

- 用户不在电脑前时，没人能点审批卡片。执行者若仍卡在审批上，宿主读出请求的精确命令，确认在票据既有授权内就代为执行，再通知父协调者、取消卡住的回合，让原执行者从断点续跑。扩大授权的（推共享仓库、写生产环境、删除真实数据）才交用户。
- 取消只停回合，不停它已经启动的进程、虚拟机或后台任务。改票时先把决定告诉父协调者再取消，否则它会按旧合同自动续跑；取消后检查并清理该票已启动的副作用，再把新合同发给原会话。
- 协调者因只有人能解决的原因停止派工（缺决定、缺凭据、额度读不到），停前必须发桌面告警，不能静默等待。
- 内存预警只告警：看门狗的 `check-memory`（由 `install-memory` 装成每分钟一次的定时器）在可用内存低于 4G，或用户服务内存压力 avg10 ≥ 25% 时告警，systemd-oomd 在 50% 持续 20 秒时会杀用户 cgroup。它不暂停派工，因为占内存的 VM 和桌面应用不会自己释放，由人决定关什么。
- app-server 必须由独立的用户服务持有，不能挂在终端复用器的 cgroup 里；重型构建与全量测试用 `systemd-run` 加内存上限单独托管。服务重启打断的回合由观察器自动续跑。细节见 [references/codex-app-server.md](references/codex-app-server.md) 与 [references/observer.md](references/observer.md)。

## 候选与收口

执行者交付固定 Base/head/tree、范围内验证、完整审查结果及剩余限制。完成回合不等于票据验收成功；rejected、unanswered、cancelled 和不明终态都不是完成。协调者核对候选，验收后才解锁消费该输出的后继票。按原票修复，不另开一个同范围写入者。

- 按原票验收条件收口。只有影响结论的新源码、配置或运行变化、具体反证或用户新增要求，才重开受影响范围的审查。
- 已审候选的补证只检查争议点；等价原始证据足够时复用，不因缺少某种截图、字段或报告格式重复操作。非阻塞建议记录处置，不自动追加实施轮次。
- 业务验证、必要审查和约定采用均满足后，由协调者直接更新接收记录。仅缺摘要或引用时补齐已有事实，不再派作者补最终回执、再审回执。确需采用时合并采用与交接派工。
- 验收后停止该票及审查会话的观察跟踪。有已授权后继才解锁；全部完成就报告业务结果、延期范围及无活动开发票并结束，不创造复审任务。

## 票据状态与可见性

- 协调者只用附带的 `scripts/host-dag-view.py` 记录新批次：`batch set` 建批次（DAG、集成分支、已验收 tip、远端回读），`ticket set` 写每票的依赖、状态、执行者 thread、模型、轮次、审查结论、候选和阻塞说明。这些信息写在 run receipt 的 `batches.<批次>.tickets` 下。工具会加锁并原子替换文件，不改回执里的其他键。
- 状态沿用 `dag`：status 只取 `CLAIMED`、`ACCEPTED`、`SUPERSEDED`，未认领的票不写；outcome 只取 `READY_FOR_ACCEPTANCE`、`NEEDS_DECISION`、`EXTERNALLY_BLOCKED`，并且只能用于 CLAIMED 的票。前置票没有全部 ACCEPTED 时，工具拒绝写 ACCEPTED。集成完成后先更新 `accepted_tip`，再把票写成 ACCEPTED。
- 用户用 `board` 看只读 TUI，内容包括已实现和已验收进度、依赖树和告警，按 `c` 看协调者的过程、按 `r` 切换 run（路径显示为 `$RUN`、`$WT`、`~`，长行最多折 3 行，按 `w` 切换为单行）`runs` 列出已登记的 run，`--run` 接受 run ID 前缀、目录名或批次名（`batch set` 自动登记，旧 run 用 `runs --add` 或 `--scan`）；补录历史验收用 `ticket set --accepted-at`；；用 `peek <票|thread> [-f] [--tools]` 读该会话的 Codex rollout 文件。工具输出要等命令结束才写进记录，运行中的命令只显示 ⏳ 和已运行时长。run 目录取自 `--run`，其次是 `HOST_DAG_RUN`，最后是正在运行的观察器单元。

## 通知、漏派防护与无人值守接管

- **Telegram**：配置写在 `~/.config/host-dag/telegram.env`（`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`、可选的 `TELEGRAM_PROXY`；也可以用 `HOST_DAG_TELEGRAM_CONFIG` 指定别的路径）。桌面告警里只有这几类会推到 Telegram：`accepted:`（整票验收）、标题含「已验收」的协调者告警、`advisor:`（需要用户决定的问题）、`result:`（批次结果）、`urgent:`（紧急）。同一张票的验收只推一次。22:00–08:00 先存起来，08:00 后合成一条「夜间汇总」再推；`urgent:` 不受这个限制。`scripts/host-dag-telegram-bridge.py` 把用户在指定 chat 里的回复转给父协调者（只认这一个 chat），再把协调者这一轮的结论推回 Telegram；`/status` 查看协调者最新的回复。
- **漏派防护**：观察器每轮读 run receipt。一张票满足以下条件时，向父协调者投递 `attention/idle` 事件，并弹桌面提醒，每 30 分钟最多催一次：未关闭；outcome 不是 NEEDS_DECISION 或 EXTERNALLY_BLOCKED；依赖都已 ACCEPTED；没有在跑的执行者或审查；超过 20 分钟没更新。有意等待时，写 `ticket set --blocker "WAIT-UNTIL:<ISO时间> <原因>"`。
- **生产宿主守卫**：在生产宿主上做有风险的测试期间，运行 `scripts/host-dag-guard.py --state <run>/watch.json --host <ip> --guest <宿主上一台虚机的 ip> --bmc <ip>`，用 systemd 托管。宿主连续 3 分钟不通，就发 `urgent:`，请人到机房或通过 BMC 重启。
- **协调者换到 Claude**：`scripts/host-dag-claude-host.py` 实现了和 Codex 宿主相同的 `thread read|send|cancel` 接口。`claude-*` 线程以无头方式运行 `claude -p --resume`，每一轮一个独立进程；其他线程照常转给 Codex 宿主。接管方式：停掉观察器，把 watch.json 的 `host_cli` 和 `parent_thread` 改成它（旧值保存在 `previous_*` 里），发交接说明，再启动观察器。什么时候接管、选哪个模型，属于项目策略，不在本技能里。


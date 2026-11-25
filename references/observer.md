# 观察器接入

先确认已有宿主 CLI 能读取当前父线程：

```sh
<existing-host-cli> thread read <actual-parent-thread-id> --view result
```

仅文件存在、daemon 在运行或环境变量有 threadId 都不足以证明该线程可由此宿主唤醒。无法读回时不启动投递观察器、不新建父线程。

读回成功后，在原 run 中初始化一份状态并交给现有进程宿主持有：

```sh
python3 <skill-root>/scripts/host-dag-watch.py --state <run/watch.json> init \
  --parent-thread <actual-parent-thread-id> --host-cli <absolute-existing-host-cli> \
  --host-cwd <target-project> --run-receipt <run/run.json> \
  --poll-interval 30 --direction-interval 3600
python3 <skill-root>/scripts/host-dag-watch.py --state <run/watch.json> watch \
  --stop-file <run/watch.stop>
```

不默认传 `--pause-parent-goal`：该选项会修改父线程 Goal，必须有用户明确暂停授权；无 Goal 时不需要它。

回执的 `claims` 按票据 ID 建对象，Grok 活动票至少包含：

```json
{
  "claims": {
    "TICKET-ID": {
      "executor": {"provider": "grok-cli"},
      "sessionId": "actual-grok-session-id",
      "log": "/absolute/run/ticket.ndjson",
      "wrapper_pid": 12345,
      "evidence": "/absolute/run/outcome.json"
    }
  }
}
```

`tickets` 中 Accepted/Superseded 等关闭状态会让观察器停止跟踪对应票。续接只更新原票实际 session/log/PID，观察器不重启 worker。已有观察器时复用原状态；不删除其状态重建。

## Codex 父协调者与执行者

Codex app-server 作为宿主时，`--host-cli` 用附带适配器的绝对路径（见 [codex-app-server.md](codex-app-server.md)）。Codex 执行者用 `track --ticket <ID> --thread <threadId>` 登记；Grok 执行者按上面的回执登记。

## 自愈、告警与看门狗

- 只有审查结果会等静默期（`--settle-seconds`），用来把成对的审查轴合并成一次唤醒；执行者的结果下一轮轮询就送达，紧急事件不等待。`board` 标题栏的“待唤醒”显示还没送达的事件及原因。
- 等在工具调用上的回合永不中断，静默 3 小时只告警；等在模型上的回合静默 20 分钟视为卡死。父协调者卡死时，观察器中断该轮再投递积压事件；执行者瞬时失败（连接、流中断、5xx、超时、服务重启）时，观察器自己向原会话发“从中断点继续”，最多 2 次，退避 2 分钟和 10 分钟。产品失败和审查结论一律交父协调者。
- 投递时父会话报 “already has an active writer”，表示另一个客户端（如桌面 App）持有该会话，消息肯定没有投出，按忙处理稍后重投，不算投递状态不明。真正状态不明时，先到父会话的本机 rollout 文件里找 marker，找到算已送达，父空闲且没找到就重投，仍无法判断才弹告警。
- 告警写在 state 同目录的 `alerts.jsonl`，需要人处理的同时 `notify-send` 到桌面，同一原因每小时最多一次。协调者因只有人能解决的原因停止派工时，停前运行：

  ```sh
  python3 <skill-root>/scripts/host-dag-alert.py --state <run/watch.json> --key <ticket>-<reason> "<标题>" "<需要人做什么>"
  ```

- 看门狗由用户 timer 每 10 分钟运行一次：观察器单元不在、失败或心跳超过 10 分钟就按记录的命令重建，Codex socket 不在时拉起 app-server。安装（观察器单元已在运行时）：

  ```sh
  python3 <skill-root>/scripts/host-dag-watchdog.py install --unit <watcher-unit> [--evidence <run>/evidence]
  ```

  `--evidence` 会让看门狗为新的 `parent-acceptance.json`（ACCEPTED*）和 `result.json`（BLOCKED）各弹一次桌面通知。

## 测试

观察器测试会触发真实告警路径；任何测试环境都要设 `HOST_DAG_WATCH_NOTIFY=0`（附带测试已在 setUp 中设置），否则用户桌面会弹出测试用的假通知。`test_hostdag_host_watch.py` 要求 `TMPDIR` 指向名字含 `hd01-test` 的目录，且不能是 `/tmp` 或家目录。

"""Host DAG views: rollout timelines, ticket states, the coordinator writer, peek and board rendering."""
from __future__ import annotations

from datetime import datetime, timedelta
import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"
CLI = SCRIPTS / "host-dag-view.py"
sys.path.insert(0, str(SCRIPTS))
import hostdag_dag_view as v  # noqa: E402
sys.path.insert(0, str(Path(__file__).resolve().parent))
from acceptance_fixture import fable_candidate_ref  # noqa: E402

spec = importlib.util.spec_from_file_location("host_view", CLI)
assert spec is not None and spec.loader is not None
hv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hv)

THREAD = "01a11e85-678d-7d20-a190-449498748e6c"
REVIEW = "01a11f02-0000-7000-8000-000000000002"


def rec(ts: str, kind: str, payload: dict) -> str:
    return json.dumps({"timestamp": f"2026-10-09T{ts}Z", "type": kind, "payload": payload})


def call(ts: str, call_id: str, js: str) -> str:
    return rec(ts, "response_item", {"type": "custom_tool_call", "name": "exec", "call_id": call_id, "input": js})


def output(ts: str, call_id: str, *texts: str, fn: bool = False) -> str:
    kind = "function_call_output" if fn else "custom_tool_call_output"
    body = texts[0] if fn else [{"type": "input_text", "text": t} for t in texts]
    return rec(ts, "response_item", {"type": kind, "call_id": call_id, "output": body})


START = [
    rec("02:37:00", "session_meta", {"id": THREAD, "cwd": "/wt/T13-03/host-dag-container"}),
    rec("02:37:00", "turn_context", {"model": "gpt-6.1-sol", "effort": "medium"}),
    rec("02:37:05", "event_msg", {"type": "task_started", "started_at": 0}),
    rec("02:37:05", "event_msg", {"type": "item_completed", "item": {
        "type": "UserMessage", "content": [{"type": "text", "text": "实现 VM 电源操作 API 并补测试\n第二行"}]}}),
]
GREP = call("02:37:41", "c1", 'text(await tools.exec_command({cmd:"rg -n \\"PowerAction\\" pkg/api"}));')
GREP_OK = output("02:37:42", "c1", "Script completed\nWall time 0.4 seconds\nOutput:\n",
                 '{"chunk_id":"a","exit_code":0,"output":"power.go:12"}')
TEST = call("02:39:00", "c2", "text(await tools.exec_command({cmd:\"python3 - <<'PY'\\nimport json\\nrun(['go','test'])\\nPY\"}));")
TEST_RUNNING = output("02:39:01", "c2", "Script running with cell ID 7\nWall time 1.0 seconds\nOutput:\n")
WAIT = rec("02:39:50", "response_item", {"type": "function_call", "name": "wait", "call_id": "w1",
                                          "arguments": json.dumps({"cell_id": "7"})})
WAIT_FAIL = output("02:39:50", "w1", "Script completed\nWall time 50 seconds\nOutput:\n"
                   + json.dumps({"exit_code": 1, "output": "--- FAIL: TestPowerStop (0.02s)\n"
                                                        "    power_test.go:44: want 202, got 409\n"}), fn=True)
MSG = rec("02:41:03", "response_item", {"type": "message", "role": "assistant",
                                        "content": [{"type": "output_text", "text": "409 来自 VMI 未就绪"}]})
TOKENS = rec("02:41:04", "event_msg", {"type": "token_count", "info": {"total_token_usage": {
    "input_tokens": 1000, "cached_input_tokens": 900, "total_tokens": 412000}}})
DONE = rec("02:42:00", "event_msg", {"type": "task_complete", "last_agent_message": "已提交 4807965b3\n细节"})


def write_rollout(root: Path, thread: str, lines: list[str]) -> Path:
    day = root / "2026" / "10" / "09"
    day.mkdir(parents=True, exist_ok=True)
    path = day / f"rollout-2026-10-09T10-37-02-{thread}.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def batch_fixture() -> dict:
    return {"tickets": {
        "KV13-01": {"status": "ACCEPTED", "deps": []},
        "KV13-02": {"status": "ACCEPTED", "deps": ["KV13-01"]},
        "T13-03": {"status": "CLAIMED", "round": 2, "deps": ["KV13-01"], "executor": THREAD},
        "KV13-04": {"status": "CLAIMED", "deps": ["KV13-02"],
                    "reviews": {"spec": {"thread": REVIEW, "verdict": "PASS"}, "standards": {"thread": REVIEW}}},
        "KV13-07": {"status": "CLAIMED", "outcome": "READY_FOR_ACCEPTANCE", "deps": ["KV13-02"]},
        "KV13-05": {"status": "CLAIMED", "outcome": "NEEDS_DECISION", "deps": ["T13-03", "KV13-04"],
                    "blocker": "镜像源选择"},
        "KV13-08": {"deps": ["KV13-02"]},
        "KV13-06": {"deps": ["KV13-05"]},
        "KV13-09": {"status": "SUPERSEDED", "deps": ["KV13-02"]},
    }}


class TimelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_finished_turn_reads_commands_results_and_failure_output(self) -> None:
        path = write_rollout(self.root, THREAD, START + [GREP, GREP_OK, TEST, TEST_RUNNING, WAIT, WAIT_FAIL,
                                                         MSG, TOKENS, DONE])
        tl = v.Timeline(path)
        tl.refresh()
        self.assertFalse(tl.running)
        self.assertEqual((tl.model, tl.effort, tl.cwd), ("gpt-6.1-sol", "medium", "/wt/T13-03/host-dag-container"))
        self.assertEqual([e.kind for e in tl.events], ["start", "shell", "shell", "msg", "end"])
        start, grep, test, msg, end = tl.events
        self.assertEqual(start.text, "实现 VM 电源操作 API 并补测试")
        self.assertEqual((grep.text, grep.result), ('rg -n "PowerAction" pkg/api', "ok"))
        self.assertEqual(test.text, "python3 - <<'PY' ↵ run(['go','test'])")
        self.assertEqual((test.result, test.code), ("fail", 1))
        self.assertEqual(test.detail, ["--- FAIL: TestPowerStop (0.02s)", "    power_test.go:44: want 202, got 409"])
        self.assertEqual(msg.text, "409 来自 VMI 未就绪")
        self.assertEqual(end.text, "已提交 4807965b3")
        self.assertEqual((tl.tokens, round(tl.cached_ratio, 2)), (412000, 0.9))
        text = "\n".join(t for t, _ in v.event_lines(tl))
        self.assertIn("✗ exit 1", text)
        self.assertIn("│ --- FAIL: TestPowerStop", text)

    def test_running_command_shows_hourglass_until_turn_ends(self) -> None:
        path = write_rollout(self.root, THREAD, START + [GREP, GREP_OK, TEST, TEST_RUNNING])
        tl = v.Timeline(path)
        tl.refresh()
        self.assertTrue(tl.running)
        self.assertEqual(tl.events[-1].result, "running")
        self.assertIn("⏳", tl.last_activity())
        self.assertIn("⏳ 运行中", v.event_lines(tl)[-1][0])
        self.assertTrue(tl.status_line().startswith("运行中"))
        tools = v.event_lines(tl, tools_only=True)
        self.assertEqual(len(tools), 2)

    def test_incremental_refresh_keeps_partial_lines(self) -> None:
        path = write_rollout(self.root, THREAD, START + [GREP])
        tl = v.Timeline(path)
        tl.refresh()
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(GREP_OK[:20])
        tl.refresh()
        self.assertEqual(tl.events[-1].result, "running")
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(GREP_OK[20:] + "\n")
        tl.refresh()
        self.assertEqual(tl.events[-1].result, "ok")

    def test_follow_prints_new_lines_and_resolution(self) -> None:
        path = write_rollout(self.root, THREAD, START + [TEST, TEST_RUNNING])
        tl = v.Timeline(path)
        tl.refresh()
        with open(path, "a", encoding="utf-8") as fh:
            fh.write("\n".join([WAIT, WAIT_FAIL, MSG]) + "\n")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            hv.follow(tl, False, False, ticks=1, interval=0)
        out = buf.getvalue()
        self.assertIn("↳ python3 - <<'PY'", out)
        self.assertIn("✗ exit 1", out)
        self.assertIn('"409 来自 VMI 未就绪"', out)


def cursor(kind: str, ms: int, **fields: object) -> str:
    return json.dumps({"type": kind, "timestamp_ms": ms, **fields})


def shell_call(sub: str, ms: int, call_id: str, command: str, result: dict | None = None) -> str:
    body: dict = {"args": {"command": command}}
    if result is not None:
        body["result"] = result
    return cursor("tool_call", ms, subtype=sub, call_id=call_id, tool_call={"shellToolCall": body})


CURSOR_LOG = [
    json.dumps({"type": "system", "subtype": "init", "cwd": "/w/KV16-02", "model": "Claude Opus 5.5 1M Medium"}),
    cursor("user", 1791873880000, message={"content": [{"type": "text", "text": "派原票KV16-02 round2\n必读"}]}),
    cursor("assistant", 1791873885000, message={"content": [{"type": "text", "text": "先核对四仓身份。"}]}),
    shell_call("started", 1791873886000, "c1", "git status"),
    shell_call("completed", 1791873888000, "c1", "git status", {"success": {"exitCode": "0", "stdout": "clean"}}),
    cursor("tool_call", 1791873890000, subtype="started", call_id="c2",
           tool_call={"editToolCall": {"args": {"path": "/w/KV16-02/a.go"}}}),
    cursor("tool_call", 1791873891000, subtype="completed", call_id="c2",
           tool_call={"editToolCall": {"args": {"path": "/w/KV16-02/a.go"}, "result": {"success": {}}}}),
    shell_call("started", 1791873892000, "c3", "go test ./pkg/x"),
    shell_call("completed", 1791873899000, "c3", "go test ./pkg/x",
               {"success": {"exitCode": "1", "stdout": "--- FAIL: TestX\nFAIL\tpkg/x", "stderr": ""}}),
    shell_call("started", 1791873900000, "c4", "kubectl get zfsvolume -A"),
]


class CliTimelineTest(unittest.TestCase):
    """Cursor/Claude CLI executors log stream-json, not a Codex rollout; the board reads it the same way."""

    def test_provider_prefixed_executors_are_stripped_on_load(self) -> None:
        run = {"batches": {"B": {"tickets": {
            "A": {"executor": "codex:" + THREAD, "reviews": {"t2": {"thread": "codex:" + REVIEW}}},
            "C": {"executor": "claude:host-dag-cli-claude-C-1101161807"},
            "D": {"executor": THREAD}}}}}
        v.normalize_executors(run)
        tickets = run["batches"]["B"]["tickets"]
        self.assertEqual(tickets["A"]["executor"], THREAD)
        self.assertEqual(tickets["A"]["reviews"]["t2"]["thread"], REVIEW)
        self.assertEqual(tickets["C"]["executor"], "host-dag-cli-claude-C-1101161807")
        self.assertEqual(tickets["D"]["executor"], THREAD)
        self.assertRegex(tickets["A"]["executor"], v.THREAD_RE)

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_cursor_stream_reads_model_messages_commands_and_failures(self) -> None:
        log = self.root / "cli.ndjson"
        log.write_text("\n".join(CURSOR_LOG) + "\n")
        tl = v.CliTimeline(log)
        tl.refresh()
        self.assertTrue(tl.running)
        self.assertEqual((tl.model, tl.cwd), ("Claude Opus 5.5 1M Medium", "/w/KV16-02"))
        self.assertEqual([(e.kind, e.text, e.result) for e in tl.events], [
            ("start", "派原票KV16-02 round2", ""), ("msg", "先核对四仓身份。", ""), ("shell", "git status", "ok"),
            ("patch", "/w/KV16-02/a.go", "ok"), ("shell", "go test ./pkg/x", "fail"),
            ("shell", "kubectl get zfsvolume -A", "running")])
        self.assertEqual(tl.events[4].code, 1)
        self.assertEqual(tl.events[4].detail, ["--- FAIL: TestX", "FAIL\tpkg/x"])
        with log.open("a") as fh:
            fh.write(cursor("result", 1791873990000, subtype="success", result="KV16-02 round2 交父") + "\n")
        tl.refresh()
        self.assertFalse(tl.running)
        self.assertEqual((tl.events[-1].kind, tl.events[-1].text), ("end", "KV16-02 round2 交父"))

    def test_claude_code_stream_takes_times_from_iso_timestamps(self) -> None:
        log = self.root / "cli.ndjson"
        log.write_text("\n".join(json.dumps(r) for r in [
            {"type": "system", "subtype": "init", "cwd": "/w/KV17-05", "model": "opus[1m]"},
            {"type": "user", "timestamp": "2026-10-15T11:11:20.000Z",
             "message": {"content": [{"type": "text", "text": "派原票KV17-05 round7"}]}},
            {"type": "system", "subtype": "thinking_tokens"},
            {"type": "assistant", "timestamp": "2026-10-15T11:11:31.171Z",
             "message": {"content": [{"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}},
            {"type": "user", "timestamp": "2026-10-15T11:11:35.227Z",
             "message": {"content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}]}},
            {"type": "result", "subtype": "success", "result": "交父"},
        ]) + "\n")
        tl = v.CliTimeline(log)
        tl.refresh()
        start = v.parse_ts("2026-10-15T11:11:20.000Z")
        self.assertEqual([(e.kind, round(e.ts - start, 3)) for e in tl.events],
                         [("start", 0.0), ("shell", 11.171), ("end", 15.227)])
        self.assertNotIn("--:--:--", "\n".join(seg[0] for seg in v.event_lines(tl)))

    def test_board_finds_a_cli_executor_through_its_run_file(self) -> None:
        ev = self.root / "evidence/KV16-02/round-2"
        ev.mkdir(parents=True)
        (ev / "cli.ndjson").write_text("\n".join(CURSOR_LOG) + "\n")
        unit = "host-dag-cli-cursor-KV16-02-1013144426"
        (ev / "cli-run.json").write_text(json.dumps({"unit": unit, "log": str(ev / "cli.ndjson")}))
        (self.root / "run.json").write_text(json.dumps({"batches": {"V16": {"tickets": {"KV16-02": {
            "status": "CLAIMED", "executor": unit, "evidence": str(ev)}}}}}))
        r = hv.Run(self.root)
        tl = r.timeline(unit)
        self.assertIsInstance(tl, v.CliTimeline)
        self.assertEqual(tl.events[-1].text, "kubectl get zfsvolume -A")
        self.assertIsNone(r.timeline("host-dag-cli-cursor-KV99-01-1"))

    def test_board_resolves_both_reviewers_with_same_start_timestamp(self) -> None:
        ev = self.root / "evidence/KV18-07/round-10"
        reviews = {}
        for axis in ("spec", "standards"):
            folder = ev / f"review-{axis}-cli"
            folder.mkdir(parents=True)
            log = folder / "cli.ndjson"
            log.write_text("\n".join(CURSOR_LOG) + "\n")
            unit = f"host-dag-cli-claude-KV18-07-review-{axis}-1016131301"
            (folder / "cli-run.json").write_text(json.dumps({"unit": unit, "log": str(log)}))
            reviews[axis] = {"thread": unit, "verdict": "PASS"}
        (self.root / "run.json").write_text(json.dumps({"batches": {"V18": {"tickets": {
            "KV18-07": {"executor": "other", "evidence": str(ev), "reviews": reviews}}}}}))
        r = hv.Run(self.root)
        timelines = [r.timeline(reviews[axis]["thread"]) for axis in ("spec", "standards")]
        self.assertTrue(all(isinstance(t, v.CliTimeline) and t.events for t in timelines))
        self.assertNotEqual(timelines[0].path, timelines[1].path)
        self.assertNotEqual(hv.short_thread(reviews['spec']['thread']),
                            hv.short_thread(reviews['standards']['thread']))


class WrapTest(unittest.TestCase):
    def text(self, rows) -> list[str]:
        return ["".join(t for t, _ in row) for row in rows]

    def test_wraps_by_display_width_under_the_text_column(self) -> None:
        line = [("15:28:49  💬 消息  \"" + "本轮证据已整理，重型 owner 已释放。" * 6 + "\"", "")]
        rows = self.text(v.wrap(line, 60, v.EVENT_INDENT, 3))
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(v.width(r) <= 59 for r in rows))
        self.assertTrue(rows[1].startswith(" " * v.EVENT_INDENT) and not rows[1][v.EVENT_INDENT].isspace())
        self.assertTrue(rows[2].endswith("…"))

    def test_short_lines_and_zero_width_stay_whole(self) -> None:
        self.assertEqual(v.wrap([("short", "ok")], 60, 19, 3), [[("short", "ok")]])
        long = [("x" * 500, "")]
        self.assertEqual(v.wrap(long, 0, 19, 3), [long])

    def test_fit_lines_picks_indent_and_one_row_mode_cuts(self) -> None:
        lines = [[("10:00:00  ⚙ shell  " + "a" * 100, "ok")], [(v.DETAIL_PREFIX + "b" * 100, "dim")]]
        wrapped = self.text(hv.fit_lines(lines, 50, 3))
        self.assertTrue(wrapped[1].startswith(" " * v.EVENT_INDENT + "a"))
        out_row = next(i for i, r in enumerate(wrapped) if r.startswith(v.DETAIL_PREFIX))
        self.assertTrue(wrapped[out_row + 1].startswith(" " * len(v.DETAIL_PREFIX) + "b"))
        cut = self.text(hv.fit_lines(lines, 50, 1))
        self.assertEqual(len(cut), 2)
        self.assertTrue(all(r.endswith("…") for r in cut))

    def test_shorten_prefers_the_longest_prefix(self) -> None:
        home = "/home/u"
        aliases = [("/runs/R/local-dev-dag", "$RUN"), (home + "/Developer/.worktrees/demo", "$WT"), (home, "~")]
        self.assertEqual(v.shorten(f"cat {home}/Developer/.worktrees/demo/T13-03 {home}/x /runs/R/local-dev-dag/ev",
                                   aliases), "cat $WT/T13-03 ~/x $RUN/ev")


class StateTest(unittest.TestCase):
    def test_display_state_follows_dag_states_and_outcomes(self) -> None:
        b = batch_fixture()
        got = {tid: v.display_state(b, tid) for tid in b["tickets"]}
        self.assertEqual(got, {
            "KV13-01": ("ok", ""), "KV13-02": ("ok", ""), "T13-03": ("run", "r2"), "KV13-04": ("run", "审查"),
            "KV13-07": ("cand", ""), "KV13-05": ("ask", ""), "KV13-08": ("ready", ""), "KV13-06": ("wait", ""),
            "KV13-09": ("sup", ""),
        })

    def test_progress_counts_implemented_including_accepted_and_skips_superseded(self) -> None:
        p = v.progress(batch_fixture())
        self.assertEqual((p.total, p.implemented, p.accepted, p.superseded), (8, 4, 2, 1))

    def test_tree_expands_multi_prerequisite_ticket_once(self) -> None:
        lines = [t for t, _ in v.tree_lines(batch_fixture())]
        self.assertEqual(lines[0], "KV13-01")
        self.assertEqual(sum(1 for x in lines if x.endswith("KV13-05")), 1)
        self.assertEqual(sum(1 for x in lines if "KV13-05 ↑ 见上" in x), 1)
        self.assertTrue(any(x.endswith("└─ KV13-06") for x in lines))

    def test_remote_state(self) -> None:
        self.assertEqual(v.remote_state({"accepted_tip": "abcdef12", "remote": {"readback": "abcdef12"}})[0], "ok")
        self.assertEqual(v.remote_state({"accepted_tip": "abcdef12", "remote": {"readback": "0000"}})[0], "warn")
        self.assertEqual(v.remote_state({})[0], "none")


class GraphExportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.run_json = {"batches": {"B1": {"title": "收尾", "tickets": {
            "BASE": {"state": "ACCEPTED", "status": "ACCEPTED", "title": "底座"},
            "MID": {"state": "RUNNING", "title": "中间票", "deps": ["BASE"], "updated_at": "2026-11-05T10:00:00+08:00"},
            "TOP": {"state": "WAITING_DEPENDENCY", "title": "顶层票", "wait_on": "ticket:MID"},
            "ASK": {"state": "WAITING_DEPENDENCY", "title": "等裁定", "wait_on": "decision:k1"},
        }}}}
        (self.dir / "run.json").write_text(json.dumps(self.run_json, ensure_ascii=False))

    def export(self, *extra: str) -> tuple[int, str]:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = hv.main(["--run", str(self.dir), "graph", *extra])
        return code, out.getvalue()

    def test_stdout_has_every_section_and_edge_without_touching_the_run(self) -> None:
        before = (self.dir / "run.json").read_bytes()
        code, text = self.export("-o", "-")
        self.assertEqual(code, 0)
        for wanted in ("批次 1 个，票 4 张；未结束 3 张", "MID", "TOP ", "等 decision:k1", "MID [", "← BASE", "← MID"):
            self.assertIn(wanted, text)
        tree = text.split("三、")[1].split("四、")[0]
        self.assertLess(tree.index("MID"), tree.index("TOP"))
        self.assertNotIn("BASE", tree)
        self.assertEqual((self.dir / "run.json").read_bytes(), before)

    def test_file_output_prints_path_and_leaves_no_temp(self) -> None:
        target = self.dir / "out" / "graph.txt"
        code, printed = self.export("-o", str(target))
        self.assertEqual((code, printed.strip()), (0, str(target)))
        self.assertIn("全局依赖图", target.read_text(encoding="utf-8"))
        self.assertEqual([p.name for p in target.parent.iterdir()], ["graph.txt"])

    def test_default_name_has_readable_time_in_download_dir(self) -> None:
        when = hv._dt.datetime(2026, 11, 5, 18, 8, 49)
        done = subprocess.CompletedProcess([], 0, stdout=f"{self.dir}\n", stderr="")
        with mock.patch.object(hv.subprocess, "run", return_value=done):
            path = hv.graph_default_path(when)
        self.assertEqual(path, self.dir / "HostDAG看板全局依赖图-2026年11月05日-18时08分49秒.txt")


class ActivityTest(unittest.TestCase):
    SID = "01a1aa48-b62a-7122-aca0-946b4a0c0410"
    PARENT = "066f7290-46e0-40cf-9d87-a4a20120f369"

    def text(self, **probe) -> str:
        args = dict(sessions=[], advisors=[], background=[], coordinator_turn=False, locks=[])
        args.update(probe)
        run = mock.Mock()
        tickets = {"DONE": {"state": "ACCEPTED", "status": "ACCEPTED", "executor": self.SID},
                   "WAIT": {"state": "WAITING_RESOURCE", "wait_on": "executor-slot:x"}}
        run.batches.return_value = {"B": {"tickets": tickets}}
        run.all_tickets.return_value = tickets
        run.watch = {"parent_thread": f"claude-{self.PARENT}"}
        return hv.activity_text(run, 1000.0, 600.0, **args)

    def test_session_on_an_accepted_ticket_counts_as_working(self) -> None:
        text = self.text(sessions=[("Codex", self.SID, 940.0)])
        self.assertIn("判断：在干活", text)
        self.assertIn("看板进行中：0 张", text)
        self.assertIn("DONE（票不在进行中，看板上看不到）", text)

    def test_verdicts_for_advisor_coordinator_and_nothing(self) -> None:
        self.assertIn("判断：在等裁定", self.text(advisors=["k1"]))
        self.assertIn("判断：协调者在处理", self.text(coordinator_turn=True))
        self.assertIn("判断：协调者在处理", self.text(sessions=[("Claude", self.PARENT, 990.0)]))
        self.assertIn("判断：可能卡住", self.text())

    def test_unbound_session_and_held_lock_do_not_hide_a_stall(self) -> None:
        text = self.text(sessions=[("Claude", "11111111-1111-1111-1111-111111111111", 990.0)], locks=["remote-platform"])
        self.assertIn("判断：可能卡住", text)
        self.assertIn("其他会话（未绑定票，不计入判断）：1 个", text)
        self.assertIn("持锁环境：remote-platform", text)
        self.assertIn("WAIT 等 executor-slot:x", text)

    def test_units_are_sorted_into_roles_and_infrastructure_is_ignored(self) -> None:
        listed = "\n".join([
            "host-dag-advisor-k1.service loaded active running [systemd-run] /bin/sh",
            "host-dag-claude-turn-a.scope loaded active running [systemd-run] /bin/claude",
            "host-dag-watch-20260926.service loaded active running [systemd-run] /x",
            "prod-package.service loaded active running [systemd-run] /x",
            "ptyxis-spawn-1.scope loaded active running [systemd-run] /x",
            "uu-remote-for-linux-client.service loaded active running [systemd-run] /x",
            "dbus.service loaded active running D-Bus"])
        done = subprocess.CompletedProcess([], 0, stdout=listed, stderr="")
        with mock.patch.object(hv.subprocess, "run", return_value=done):
            self.assertEqual(hv.activity_units(), (["k1"], ["prod-package"], True))


class DependencyDirectionTest(unittest.TestCase):
    def render(self, batches: dict, name: str | None = None) -> list[str]:
        # Only the pure renderers are exercised; no live run or session is loaded.
        run = mock.Mock()
        run.batches.return_value = batches
        run.all_tickets.return_value = {tid: t for b in batches.values() for tid, t in b["tickets"].items()}
        run.freeze_owner.return_value = ""
        lines = hv.graph_view(run) if name is None else hv.tree_view(run, name)
        return ["".join(text for text, _ in line) for line in lines]

    def test_chain_has_same_order_and_legend_in_both_views(self) -> None:
        batches = {"B1": {"tickets": {
            "IMPL-4": {"state": "CREATED", "deps": ["IMPL-3"]},
            "IMPL-3": {"state": "RUNNING"},
        }}}
        for name in ("B1", None):
            with self.subTest(view=name or "global"):
                lines = self.render(batches, name)
                self.assertEqual(lines[1], "上面的先做，下面的等上面")
                rows = [line for line in lines if "IMPL-" in line]
                self.assertTrue(rows[0].startswith("IMPL-3 "), rows)
                self.assertTrue(rows[1].startswith("└─ IMPL-4 "), rows)
                self.assertIn("等 IMPL-3", rows[1])

    def test_diamond_and_multiple_roots_reference_expanded_successor(self) -> None:
        batches = {"B": {"tickets": {
            "A": {"state": "RUNNING"}, "Z": {"state": "RUNNING"},
            "B": {"state": "CREATED", "deps": ["A"]},
            "C": {"state": "CREATED", "deps": ["A"]},
            "D": {"state": "CREATED", "deps": ["B", "C", "Z"]},
            "E": {"state": "CREATED", "deps": ["D"]},
        }}}
        for name in ("B", None):
            with self.subTest(view=name or "global"):
                lines = self.render(batches, name)
                refs = [line for line in lines if "↑ 见上" in line]
                self.assertEqual(len(refs), 2)
                self.assertTrue(all(line.endswith("D ↑ 见上") for line in refs), refs)
                expanded = next(line for line in lines if "D " in line and "↑ 见上" not in line)
                self.assertIn("等 B,C,Z", expanded)
                self.assertNotIn("A", expanded)  # A is transitive, never a direct wait label.
                self.assertTrue(all(lines.index(expanded) < lines.index(line) for line in refs))
                self.assertEqual(sum("E " in line for line in lines), 1)

    def test_global_cross_batch_waits_and_closed_prerequisites(self) -> None:
        batches = {
            "old": {"tickets": {"DONE": {"state": "ACCEPTED"}, "CANCEL": {"state": "CANCELED"}}},
            "first": {"tickets": {"A": {"state": "RUNNING", "deps": ["DONE", "CANCEL"]}}},
            "next": {"tickets": {
                "B": {"state": "WAITING_RESOURCE", "deps": ["A"], "wait_on": "ticket:A"},
                "C": {"state": "WAITING_RESOURCE", "wait_on": "ticket", "wait_holder": "B"},
                "R": {"state": "WAITING_RESOURCE", "wait_on": "capacity:local-memory>12GiB"},
            }},
        }
        lines = self.render(batches)
        rows = [line for line in lines if "[" in line]
        self.assertTrue(rows[0].startswith("A [first]"), rows)
        self.assertTrue(rows[1].startswith("└─ B [next]"), rows)
        self.assertTrue(rows[2].startswith("   └─ C [next]"), rows)
        self.assertIn("等 A", rows[1])
        self.assertIn("等 B", rows[2])
        self.assertIn("等 本机内存>12GiB", rows[3])
        self.assertNotIn("↑ 见上", "\n".join(lines))  # deps + wait_on is one edge.
        self.assertNotIn("DONE", "\n".join(lines))
        self.assertNotIn("CANCEL", "\n".join(lines))

    def test_global_missing_prerequisite_keeps_its_successor_visible(self) -> None:
        lines = self.render({"B": {"tickets": {"CHILD": {"state": "CREATED", "deps": ["MISSING"]}}}})
        missing = next(line for line in lines if "不在本 run" in line)
        child = next(line for line in lines if "CHILD [B]" in line)
        self.assertTrue(missing.startswith("MISSING "), lines)
        self.assertTrue(child.startswith("└─ CHILD "), lines)
        self.assertLess(lines.index(missing), lines.index(child))

    def test_global_cycle_is_finite_and_keeps_disconnected_root(self) -> None:
        lines = self.render({"B": {"tickets": {
            "A": {"state": "CREATED", "deps": ["B"]},
            "B": {"state": "CREATED", "deps": ["A"]},
            "ROOT": {"state": "RUNNING"},
        }}})
        rows = [line for line in lines if "[B]" in line or "↑ 见上" in line]
        self.assertEqual(len(rows), 4)
        self.assertTrue(rows[0].startswith("ROOT "), rows)
        self.assertEqual(sum("↑ 见上" in line for line in rows), 1)


class WakeStatusTest(unittest.TestCase):
    def watch(self, *events, **extra) -> dict:
        return {"settle_seconds": 180, "max_hold_seconds": 600, "pending_events": list(events), **extra}

    def test_nothing_queued(self) -> None:
        self.assertIsNone(v.wake_status(self.watch(), False, 1000))

    def test_executor_result_goes_on_next_poll(self) -> None:
        text = v.wake_status(self.watch({"kind": "completed", "ticket": "KV13-04", "queued_at": 990}), False, 1000)
        self.assertEqual(text, "待唤醒 1（KV13-04，下一轮检查送达（≤30s））")

    def test_review_shows_remaining_quiet_window(self) -> None:
        w = self.watch({"kind": "completed", "ticket": "KV13-04-review-spec", "queued_at": 900})
        self.assertIn("静默期剩 1m20s", v.wake_status(w, False, 1000))

    def test_review_waits_for_running_sibling(self) -> None:
        w = self.watch({"kind": "completed", "ticket": "X-review-spec", "queued_at": 900},
                       tracked={"X-review-standards": {"terminal": False}})
        self.assertIn("等另一审查轴完成", v.wake_status(w, False, 1000))

    def test_busy_coordinator_and_urgent_events(self) -> None:
        w = self.watch({"kind": "failed", "ticket": "A-review-spec", "queued_at": 990},
                       {"kind": "completed", "ticket": "B", "queued_at": 990},
                       {"kind": "completed", "ticket": "C", "queued_at": 990})
        self.assertIn("下一轮检查送达", v.wake_status(w, False, 1000))
        self.assertEqual(v.wake_status(w, True, 1000), "待唤醒 3（A-review-spec、B…，协调者忙，空闲后送达）")


class RunsAndOrderingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.index = self.root / "state/host-dag/runs.json"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_unfinished_batches_come_first(self) -> None:
        done = {"tickets": {"A": {"status": "ACCEPTED"}}}
        run = {"batches": {"OLD": done, "NOW": batch_fixture(), "EMPTY": {"tickets": {}}}}
        self.assertTrue(v.batch_done(done))
        self.assertFalse(v.batch_done({"tickets": {}}))
        self.assertTrue(v.batch_done({"tickets": {"A": {"state": "CANCELED", "status": "SUPERSEDED"}}}))
        self.assertEqual(v.ordered_batch_names(run), ["NOW", "EMPTY", "OLD"])

    def test_turn_end_reports_duration_instead_of_repeating_the_last_message(self) -> None:
        tl = v.Timeline.__new__(v.Timeline)
        tl.running = False
        tl.events = [v.Event(1000, "start", "go"), v.Event(1500, "msg", "done"), v.Event(1500, "end", "done")]
        self.assertTrue(v.event_lines(tl, now=2000)[-1][0].endswith("本轮用时 8m20s"))

    def test_wait_column_names_the_freeze_and_memory(self) -> None:
        queued = {"state": "WAITING_DEPENDENCY", "blocker": "board-freeze (B1 live): review queued"}
        self.assertEqual(hv.wait_short(queued, "wait", {}, "B1-CHAIN"), "冻结排队（B1-CHAIN 窗口）")
        self.assertEqual(hv.wait_short(queued, "wait", {}, ""), "⚠ 冻结已解除，待派")
        mem = {"state": "WAITING_RESOURCE", "wait_on": "capacity:local-memory>12GiB"}
        self.assertEqual(hv.wait_short(mem, "res", {}), "本机内存>12GiB")
        self.assertEqual(hv.wait_short({"state": "WAITING_RESOURCE", "wait_on": "capacity:ZFS"}, "res", {}), "存储容量 ZFS")

    def test_elapsed_time_uses_the_largest_units(self) -> None:
        self.assertEqual([v.dur_long(x) for x in (5, 75, 3600, 90000, 8 * 86400 + 7200)],
                         ["5s", "1m15s", "1h", "1d1h", "1w1d"])

    def test_ticket_span_comes_from_the_executor_id(self) -> None:
        now = datetime(2026, 10, 27, 16, 0).timestamp()
        cli = {"executor": "host-dag-cli-claude-X-1027134120", "state": "RUNNING"}
        self.assertEqual(v.ticket_span(cli, now), (datetime(2026, 10, 27, 13, 41, 20).timestamp(), now))
        codex = {"executor": "01a17bad-c4f2-7ef0-b649-9d297af04c6c", "state": "INTEGRATED",
                 "accepted_at": "2026-10-27T14:14:28+08:00"}
        start, end = v.ticket_span(codex, now)
        self.assertEqual(round(end - start), 5320)
        self.assertIsNone(v.ticket_span({"executor": "", "state": "RUNNING"}, now))
        self.assertIsNone(v.ticket_span({**cli, "state": "CANCELED"}, now))

    def test_ticket_span_ignores_an_earlier_builds_acceptance(self) -> None:
        # 11-22: the cut ticket kept b22's accepted_at (08:19) while the b23 session ran; 用时 read 0.
        now = datetime(2026, 11, 22, 0, 0).timestamp()
        t = {"executor": "host-dag-cli-claude-X-1121120000", "state": "WAITING_RESOURCE",
             "accepted_at": "2026-11-21T08:19:39+08:00"}
        self.assertEqual(v.ticket_span(t, now), (datetime(2026, 11, 21, 12, 0).timestamp(), now))

    def test_register_is_idempotent_and_survives_a_missing_index(self) -> None:
        self.assertEqual(v.known_runs(self.index), [])
        run = self.root / "runs/abc/dag"
        run.mkdir(parents=True)
        self.assertTrue(v.register_run(run, self.index))
        self.assertFalse(v.register_run(run, self.index))
        self.assertEqual(v.known_runs(self.index), [run.resolve()])

    def test_scan_finds_runs_with_batches_and_skips_evidence(self) -> None:
        good = self.root / "runs/r1/dag"
        plain = self.root / "runs/r2/dag"
        buried = self.root / "runs/r3/dag/evidence/old"
        for d, body in ((good, {"batches": {}}), (plain, {"tickets": {}}), (buried, {"batches": {}})):
            d.mkdir(parents=True)
            (d / "run.json").write_text(json.dumps(body))
        self.assertEqual(v.scan_runs(self.root / "runs"), [good])

    def test_run_keys_from_the_runs_list_resolve(self) -> None:
        a = self.root / "runs/01a0b493-aaaa/local-dev-dag"
        b = self.root / "runs/01a061c5-bbbb/x/continuation"
        for d, batches in ((a, {"V13": {}}), (b, {"R3": {}})):
            d.mkdir(parents=True)
            (d / "run.json").write_text(json.dumps({"batches": batches}))
            v.register_run(d, self.index)
        old = v.RUNS_INDEX
        v.RUNS_INDEX = self.index
        try:
            for key, want in (("01a061c5", b), ("R3", b), ("local-dev-dag", a), ("V13", a)):
                self.assertEqual(hv.find_run(key), want.resolve(), key)
            with self.assertRaisesRegex(v.ViewError, "matches several runs"):
                hv.find_run("01a0")
            with self.assertRaisesRegex(v.ViewError, "host-dag runs"):
                hv.find_run("nope")
        finally:
            v.RUNS_INDEX = old

    def test_run_label_keeps_the_part_after_runs(self) -> None:
        self.assertEqual(hv.run_label(Path("/x/runs/01a0b493-176b-7a20/local-dev-dag")), "01a0b493…/local-dev-dag")

    def test_runs_cli_add_lists_batches_and_batch_set_registers(self) -> None:
        env = {**os.environ, "HOST_DAG_RUNS_INDEX": str(self.index), "NO_COLOR": "1"}
        run = self.root / "runs/r9/dag"
        run.mkdir(parents=True)
        (run / "run.json").write_text(json.dumps({"batches": {"V1": {"tickets": {"A": {"status": "ACCEPTED"}}}}}))
        other = self.root / "runs/r8/dag"
        other.mkdir(parents=True)
        (other / "run.json").write_text("{}")
        res = subprocess.run([sys.executable, str(CLI), "--run", str(run), "runs", "--add", str(run)],
                             capture_output=True, text=True, env=env)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("● r9/dag", res.stdout)
        self.assertIn("✔V1 1/1", res.stdout)
        res = subprocess.run([sys.executable, str(CLI), "--run", str(other), "batch", "set", "--batch", "B"],
                             capture_output=True, text=True, env=env)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn(other.resolve(), v.known_runs(self.index))


class WriterTest(unittest.TestCase):
    CANDIDATE = "GC:" + "a" * 40

    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.ref = fable_candidate_ref(self.root, ["KV13-05", "KV13-07", "KV14-01", "KV14-02"], self.CANDIDATE)

    def run_fixture(self) -> dict:
        return {"batches": {"V13": batch_fixture()}}

    def accept(self, run: dict, batch: str, tid: str, **extra: str) -> dict:
        fields = {"status": "ACCEPTED", "candidate": self.CANDIDATE, "candidate_decision_ref": self.ref, **extra}
        return v.apply_ticket_update(run, batch, tid, fields, run_dir=self.root)

    def test_outcome_needs_claimed(self) -> None:
        run = self.run_fixture()
        with self.assertRaisesRegex(v.ViewError, "needs status CLAIMED"):
            v.apply_ticket_update(run, "V13", "KV13-08", {"outcome": "NEEDS_DECISION"})

    def test_accept_requires_accepted_prerequisites(self) -> None:
        run = self.run_fixture()
        with self.assertRaisesRegex(v.ViewError, "T13-03, KV13-04"):
            v.apply_ticket_update(run, "V13", "KV13-05", {"status": "ACCEPTED", "candidate": "GC:" + "a" * 40})

    def test_accept_finds_prerequisites_in_other_batches(self) -> None:
        run = self.run_fixture()
        run["batches"]["V14"] = {"tickets": {"KV14-01": {"status": "CLAIMED", "deps": ["KV13-07"]},
                                             "KV14-02": {"status": "CLAIMED", "deps": ["KV13-05"]}}}
        run["batches"]["V13"]["tickets"]["KV13-07"]["status"] = "ACCEPTED"
        t = self.accept(run, "V14", "KV14-01")
        self.assertEqual(t["status"], "ACCEPTED")
        with self.assertRaisesRegex(v.ViewError, "KV13-05"):
            self.accept(run, "V14", "KV14-02")

    def test_accept_clears_outcome_and_stamps_time(self) -> None:
        run = self.run_fixture()
        t = self.accept(run, "V13", "KV13-07")
        self.assertNotIn("outcome", t)
        self.assertIn("accepted_at", t)

    def test_backfill_keeps_the_given_acceptance_time(self) -> None:
        run = self.run_fixture()
        t = self.accept(run, "V13", "KV13-07", accepted_at="2026-11-05T15:09:00+08:00")
        self.assertEqual(t["accepted_at"], "2026-11-05T15:09:00+08:00")

    @unittest.skipUnless(importlib.util.find_spec('release_coverage') is not None, 'optional release_coverage module not installed')
    def test_backfill_cannot_move_a_new_acceptance_before_the_hook(self) -> None:
        for stamp, code in (("2026-10-08T15:09:00+08:00", "ACCEPTED_AT_BACKFILL_PRE_HOOK"),
                            ("2026-11-05T15:09:00", "ACCEPTED_AT_INVALID")):
            with self.subTest(stamp=stamp), self.assertRaisesRegex(v.ViewError, code):
                self.accept(self.run_fixture(), "V13", "KV13-07", accepted_at=stamp)
        run = self.run_fixture()  # a row already accepted before the hook may keep (re-state) its time
        run["batches"]["V13"]["tickets"]["KV13-07"].update(status="ACCEPTED", candidate=self.CANDIDATE,
                                                        accepted_at="2026-10-08T15:09:00+08:00")
        t = self.accept(run, "V13", "KV13-07", accepted_at="2026-10-08T15:09:00+08:00")
        self.assertEqual(t["accepted_at"], "2026-10-08T15:09:00+08:00")

    @unittest.skipUnless(importlib.util.find_spec('release_coverage') is not None, 'optional release_coverage module not installed')
    def test_new_delivery_acceptance_needs_attribution(self) -> None:
        with self.assertRaisesRegex(v.ViewError, "ACCEPTED_ATTRIBUTION_REQUIRED"):
            v.apply_ticket_update(self.run_fixture(), "V13", "KV13-07",
                                  {"status": "ACCEPTED", "candidate": self.CANDIDATE}, run_dir=self.root)

    def test_rejects_unknown_status_and_verdict(self) -> None:
        run = self.run_fixture()
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(run, "V13", "T13-03", {"status": "DONE"})
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(run, "V13", "T13-03", {"reviews": {"spec": {"verdict": "OK"}}})
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(run, "NOPE", "T13-03", {})


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.runs = base / "run"
        self.runs.mkdir()
        self.sessions = base / "sessions"
        write_rollout(self.sessions, THREAD, START + [GREP, GREP_OK, TEST, TEST_RUNNING])
        (self.runs / "run.json").write_text(json.dumps({"coordinator_session": "x", "tickets": {"LD-01": {}}}))
        (self.runs / "watch.json").write_text(json.dumps({"parent_thread": THREAD, "tracked": {
            "OLD-01": {"thread": THREAD},
            "GROK-01": {"provider": "grok", "session": "s", "log": "/tmp/acp.ndjson"},
        }}))
        self.env = {**os.environ, "HOST_DAG_RUN": str(self.runs), "HOST_DAG_CODEX_SESSIONS": str(self.sessions),
                    "NO_COLOR": "1",
                    # batch set registers the run; never in the user's real ~/.local/state/host-dag/runs.json
                    "HOST_DAG_RUNS_INDEX": str(base / "runs-index.json")}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def cli(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, str(CLI), *args], capture_output=True, text=True, env=self.env)

    def test_new_ticket_requires_title_but_existing_ticket_updates_do_not(self) -> None:
        self.assertEqual(self.cli("batch", "set", "--batch", "V13").returncode, 0)
        before = (self.runs / "run.json").read_bytes()
        rejected = self.cli("ticket", "set", "--batch", "V13", "--ticket", "KV13-NEW")
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("needs --title", rejected.stderr)
        self.assertEqual((self.runs / "run.json").read_bytes(), before)
        created = self.cli("ticket", "set", "--batch", "V13", "--ticket", "KV13-NEW",
                           "--title", "测试票名")
        self.assertEqual(created.returncode, 0, created.stderr)
        updated = self.cli("ticket", "set", "--batch", "V13", "--ticket", "KV13-NEW",
                           "--status", "CLAIMED")
        self.assertEqual(updated.returncode, 0, updated.stderr)
        ticket = json.loads((self.runs / "run.json").read_text())["batches"]["V13"]["tickets"]["KV13-NEW"]
        self.assertEqual(ticket["title"], "测试票名")

    def test_writer_round_trip_keeps_other_receipt_keys(self) -> None:
        self.assertEqual(self.cli("batch", "set", "--batch", "V13", "--dag", "docs/v13.md",
                                  "--accepted-tip", "abc1234", "--remote-readback", "abc1234").returncode, 0)
        for args in (["--ticket", "KV13-01", "--title", "列表", "--status", "CLAIMED", "--executor", THREAD,
                      "--model", "gpt-6.1-sol", "--effort", "medium"],
                     ["--ticket", "KV13-02", "--title", "详情", "--deps", "KV13-01"],
                     ["--ticket", "KV13-01", "--review", f"spec={REVIEW}:PASS", "--review", f"standards={REVIEW}"]):
            res = self.cli("ticket", "set", "--batch", "V13", *args)
            self.assertEqual(res.returncode, 0, res.stderr)
        untitled = self.cli("ticket", "set", "--batch", "V13", "--ticket", "KV13-09", "--status", "CLAIMED")
        self.assertEqual(untitled.returncode, 2)
        self.assertIn("needs --title", untitled.stderr)
        from acceptance_fixture import local_candidate
        candidate = local_candidate(self.runs)
        bad = self.cli("ticket", "set", "--batch", "V13", "--ticket", "KV13-02", "--status", "ACCEPTED",
                       "--candidate", candidate)
        self.assertEqual(bad.returncode, 2)
        self.assertIn("prerequisites: KV13-01", bad.stderr)
        run = json.loads((self.runs / "run.json").read_text())
        self.assertEqual(run["coordinator_session"], "x")
        self.assertIn("LD-01", run["tickets"])
        t = run["batches"]["V13"]["tickets"]["KV13-01"]
        review = dict(t["reviews"]["spec"])
        self.assertIsInstance(review.pop("registered_at"), (int, float))
        self.assertEqual(review, {"thread": REVIEW, "verdict": "PASS"})
        self.assertEqual(run["batches"]["V13"]["tickets"]["KV13-02"]["deps"], ["KV13-01"])

    def test_peek_by_batch_ticket_and_by_legacy_tracked_ticket(self) -> None:
        self.cli("batch", "set", "--batch", "V13")
        self.cli("ticket", "set", "--batch", "V13", "--ticket", "T13-03", "--title", "监控", "--status", "CLAIMED",
                 "--executor", THREAD)
        for target in ("T13-03", "OLD-01", THREAD):
            res = self.cli("peek", target)
            self.assertEqual(res.returncode, 0, res.stderr)
            self.assertIn("⚙ shell  rg -n", res.stdout)
            self.assertIn("⏳ 运行中", res.stdout)
            self.assertIn("工作树 /wt/T13-03/host-dag-container", res.stdout)
        tools = self.cli("peek", "T13-03", "--tools", "-n", "1").stdout
        self.assertNotIn("▶ 开始", tools)
        self.assertEqual(sum(1 for x in tools.splitlines() if "⚙ shell" in x), 1)

    def test_peek_reports_grok_and_unknown_tickets(self) -> None:
        res = self.cli("peek", "GROK-01")
        self.assertEqual(res.returncode, 2)
        self.assertIn("log: /tmp/acp.ndjson", res.stderr)
        self.assertEqual(self.cli("peek", "NOPE-9").returncode, 2)

    def test_board_and_tree_render_from_receipt(self) -> None:
        run = json.loads((self.runs / "run.json").read_text())
        run["batches"] = {"V13": {**batch_fixture(), "accepted_tip": "1a2b3c4d", "remote": {"readback": "1a2b3c4d"}}}
        run["batches"]["V13"]["tickets"]["T13-03"]["model"] = "gpt-6.1-sol"
        run["batches"]["V13"]["tickets"]["KV13-04"]["blocker"] = "等运行窗口"
        run["batches"]["V13"]["tickets"]["KV13-08"]["blocker"] = "排在 KV13-05 之后"
        (self.runs / "run.json").write_text(json.dumps(run))
        old = os.environ.get("HOST_DAG_CODEX_SESSIONS")
        hv.v.SESSIONS_ROOT = self.sessions
        try:
            r = hv.Run(self.runs)
            lines, order = hv.board_lines(r, "V13", 0)
            text = "\n".join("".join(s for s, _ in line) for line in lines)
            self.assertEqual(order[:3], ["KV13-01", "KV13-02", "T13-03"])
            folded_lines, folded = hv.board_lines(r, "V13", 0, hide_ok=True)
            self.assertEqual(len(order) - len(folded), 3)  # two accepted + one failed/canceled fold away
            self.assertNotIn("KV13-01", folded)
            self.assertIn("（已收起 3 张已结束：已验收/作废/失败，h 展开）", "\n".join("".join(s for s, _ in line) for line in folded_lines))
            run["batches"]["V13"]["tickets"]["KV13-08"].update(status="SUPERSEDED", state="CANCELED")
            (self.runs / "run.json").write_text(json.dumps(run))
            _, folded = hv.board_lines(hv.Run(self.runs), "V13", 0, hide_ok=True)
            self.assertNotIn("KV13-08", folded)  # canceled/superseded folds with the accepted ones
            self.assertIn("KV13-04", folded)  # under review stays
            live_lines, live = hv.board_lines(hv.Run(self.runs), "V13", 0, hide_ok=hv.FOLD_LIVE)
            self.assertIn("KV13-04", live)  # under review is live work
            self.assertNotIn("KV13-08", live)
            self.assertTrue(all(hv.v.display_state(run["batches"]["V13"], t)[0] in hv.LIVE_KEYS for t in live))
            self.assertIn("h 显示排队的", "\n".join("".join(s for s, _ in line) for line in live_lines))
            self.assertIn("已实现 4/8", text)  # KV13-04 is under review: it has a candidate
            self.assertIn(" V13 ", text)  # unfinished batch tab has no ✔
            self.assertIn("已验收 2/8", text)
            self.assertIn("远端 ✓ 已同步 1a2b3c4", text)
            self.assertIn("▶ 执行中·r2", text)
            self.assertIn("spec✓ std▶", text)
            self.assertIn("? 需决定", text)
            self.assertIn("镜像源选择", text)
            self.assertIn("gpt-6.1-sol", text)
            row03 = next(line for line in lines if line and line[0][0].startswith("T13-03"))
            self.assertIn("⏳", "".join(s for s, _ in row03))
            row04 = next(line for line in lines if line and line[0][0].startswith("KV13-04"))
            self.assertIn("等运行窗口", "".join(s for s, _ in row04))
            row08 = next(line for line in lines if line and line[0][0].startswith("KV13-08"))
            self.assertIn("○ 可执行", "".join(s for s, _ in row08))
            self.assertIn("排在 KV13-05 之后", "".join(s for s, _ in row08))
            detail = "\n".join("".join(s for s, _ in line) for line in hv.detail_lines(r, "V13", "T13-03", "exec"))
            self.assertIn("后继 KV13-05", detail)
            self.assertEqual(r.coordinator(), THREAD)
            coord = "\n".join("".join(s for s, _ in line)
                              for line in hv.peek_lines("协调者", r.coordinator(), r.timeline(r.coordinator()), {}, False))
            self.assertIn("协调者  thread " + THREAD, coord)
            self.assertIn("⚙ shell  rg -n", coord)
            tree = "\n".join("".join(s for s, _ in line) for line in hv.tree_view(r, "V13"))
            self.assertIn("↑ 见上", tree)
        finally:
            hv.v.SESSIONS_ROOT = Path(old) if old else Path.home() / ".codex" / "sessions"


class BoardScrollTest(unittest.TestCase):
    """Scrolling a long log re-slices cached rows; queued keys cost one redraw."""

    class Scr:
        def __init__(self, keys: list[int]) -> None:
            self.keys, self.draws, self.blocking = list(keys), 0, True

        def getmaxyx(self) -> tuple[int, int]:
            return 20, 120

        def getch(self) -> int:
            if not self.blocking and (not self.keys or self.keys[0] == -1):
                if self.keys:
                    self.keys.pop(0)
                return -1
            return self.keys.pop(0)

        def nodelay(self, flag: bool) -> None:
            self.blocking = not flag

        def timeout(self, ms: int) -> None:
            self.blocking = True

        def erase(self) -> None:
            pass

        def addstr(self, *a: object) -> None:
            pass

        def refresh(self) -> None:
            self.draws += 1

    def board(self, keys: list[int]) -> tuple[object, list[str]]:
        import curses
        b = object.__new__(hv.Board)
        b.c, b.scr, b.r = curses, self.Scr(keys), type("R", (), {"reload": lambda self: None})()
        b.view, b.sel, b.tab, b.tools, b.flt, b.bname, b.rsel = "peek", 0, "exec", False, "", "V", 0
        b.pfilter, b.alerts, b.alert_sel, b.alert_expanded = v.FILTER_ALL, [], -1, False
        b.alert_top, b.alert_follow_end, b.show_timings, b.timings = 0, False, False, {}
        b.hot_warnings = []
        b.read_cache = type("C", (), {"status": ""})()
        b.hot = type("H", (), {"poll": lambda self: []})()
        b.scroll, b.back, b.wrap, b.order, b.runs, b.show_all, b.hide_ok = -1, "board", True, ["T-1"], [], False, False
        b.gen, b.loaded, b.cache = 0, -1, None
        b.attr = {"": 0, "dim": 0}
        calls: list[str] = []

        def lines() -> list:
            calls.append(b.view)
            return [[(f"row {i}", "")] for i in range(200)]
        b.lines = lines
        b.read_lines = lines  # isolate row rendering/key batching from background disk reads
        return b, calls

    def test_scroll_keys_reuse_the_rendered_rows(self) -> None:
        import curses
        b, calls = self.board([])
        for _ in range(5):
            b.handle(curses.KEY_UP)
        b.handle(curses.KEY_PPAGE)
        self.assertEqual(calls, ["peek"])
        self.assertEqual(b.scroll, 200 - 19 - 5 - 18)
        b.handle(ord("w"))  # a non-scroll key renders anew
        b.rendered()
        self.assertEqual(calls, ["peek", "peek"])

    def test_held_arrow_costs_one_redraw(self) -> None:
        import curses
        b, calls = self.board([curses.KEY_UP, curses.KEY_UP, curses.KEY_UP, -1, 27, -1, ord("q"), -1])
        b.back = "board"
        self.assertEqual(b.loop(), 0)
        self.assertEqual(b.scr.draws, 3)  # first frame, after the three arrows, after Esc
        self.assertEqual(calls.count("peek"), 1)


class ObserverTest(unittest.TestCase):
    def test_unconsumed_is_strictly_newer_sorted_oldest_first(self) -> None:
        tickets = {"T": {"updated_at": "1970-01-01T00:01:40Z"}, "unknown": {}}
        artifacts = [{"ticket": "T", "kind": "result", "path": "r", "mtime": 200},
                     {"ticket": "T", "kind": "verdict", "path": "v", "mtime": 101},
                     {"ticket": "T", "kind": "result", "path": "old", "mtime": 100},
                     {"ticket": "unknown", "kind": "result", "path": "u", "mtime": 200}]
        rows = hv.unconsumed_events(tickets, artifacts, now=701)
        self.assertEqual([r["path"] for r in rows], ["v", "r"])
        self.assertEqual([r["wait"] for r in rows], [600, 501])
        self.assertFalse(rows[0]["overdue"])
        self.assertTrue(hv.unconsumed_events(tickets, artifacts, now=702)[0]["overdue"])
        self.assertEqual(hv.unconsumed_events(tickets, [], now=702), [])

    def test_per_module_outputs_collapse_into_one_scan_row(self) -> None:
        full = [{"ticket": "T", "kind": "result", "path": f"run/full/test_{i}/result.json",
                 "wait": 100 - i, "overdue": i == 0} for i in range(5)]
        lone = [{"ticket": "T", "kind": "result", "path": "run/result.json", "wait": 5, "overdue": False},
                {"ticket": "T", "kind": "verdict", "path": "run/full/test_9/verdict.json", "wait": 1, "overdue": False}]
        rows = hv.group_sibling_events(full + lone)
        self.assertEqual([(r["path"], r.get("count")) for r in rows],
                         [("run/full", 5), ("run/result.json", None), ("run/full/test_9/verdict.json", None)])
        self.assertEqual(rows[0]["wait"], 100)
        self.assertTrue(rows[0]["overdue"])

    def test_evidence_cli_review_and_conventional_directories(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            attempt = root / "evidence/demo-local/v36/T/run-1"
            cli = attempt / "cli/cli-run.json"
            cli.parent.mkdir(parents=True)
            cli.write_text("{}")
            result = attempt / "result.json"
            result.write_text("{}")
            verdict = attempt / "review-1/verdict.json"
            verdict.parent.mkdir()
            verdict.write_text("{}")
            external = root / "review/verdict.json"
            external.parent.mkdir()
            external.write_text("{}")
            tickets = {"T": {"evidence": str(cli), "reviews": {"spec": {"evidence": str(external)}}},
                       "missing": {"evidence": str(root / "absent"), "reviews": {"spec": {"verdict": "PASS"}}}}
            rows = hv.observer_artifacts(root, tickets)
            self.assertEqual({r["path"] for r in rows}, {str(result), str(verdict), str(external)})
            self.assertEqual(len(rows), 3)
            # No evidence field: the conventional ticket directory still finds both artifacts.
            self.assertEqual(len(hv.observer_artifacts(root, {"T": {}})), 2)
            verdict.unlink()
            self.assertEqual(len(hv.observer_artifacts(root, {"T": {}})), 1)

    def test_artifact_scan_skips_runtime_material_and_shared_control_history(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("evidence/topic/v/T/run-1/result.json", "evidence/topic/v/T/run-1/windows/job/result.json",
                         "evidence/topic/v/T/run-1/tools/copy/result.json",
                         "evidence/topic/v/T/run-1/window/runtime/result.json",
                         "evidence/topic/v/T/run-1/checks/probe/result.json",
                         "control/other-ticket/result.json", "control/result.json"):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}")
            receipt = root / "control/receipt.md"
            receipt.write_text("receipt")
            rows = hv.observer_artifacts(root, {"T": {"evidence": str(receipt)}})
            self.assertEqual({r["path"] for r in rows}, {str(root / "control/result.json"),
                                                       str(root / "evidence/topic/v/T/run-1/result.json")})

    def test_conventional_scan_is_bounded_but_deep_explicit_reviews_are_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ticket = root / "evidence/topic/v/T"
            result = ticket / "round-1/attempt-1/review-1/verdict.json"
            deep = ticket / "round-1/copied/tree/nested/review/verdict.json"
            for path in (result, deep):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}")
            self.assertEqual({r["path"] for r in hv.observer_artifacts(root, {"T": {}})}, {str(result)})
            tickets = {"T": {"reviews": {"spec": {"evidence": str(deep)}}}}
            self.assertEqual({r["path"] for r in hv.observer_artifacts(root, tickets)}, {str(result), str(deep)})

    def test_queue_and_recent_events_are_read_only_and_tolerate_bad_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name, body in [("queued", "busy"), ("sent", '{"status":"inProgress"}'), ("empty", "")]:
                p = root / f"control/advisor-{name}/send.log"
                p.parent.mkdir(parents=True)
                p.write_text(body)
                os.utime(p, (100, 100))
            queue = hv.advisor_queue(root, now=800)
            self.assertEqual({r["name"] for r in queue}, {"advisor-queued", "advisor-empty"})
            self.assertTrue(all(r["wait"] == 700 for r in queue))
            (root / "alerts.jsonl").write_text('bad\n[]\n' + json.dumps(
                {"at": 100, "key": "parentheal:x", "title": "自动中断"}) + '\n')
            tl = v.Timeline(root / "missing")
            tl.events = [v.Event(101, "start", "HOST-DAG-WATCH-MARKER:test")]
            events = hv.observer_recent(root, {}, tl)
            self.assertIn("watcher 唤醒", events[0]["text"])
            self.assertIn("自动中断", events[1]["text"])
            (root / "logs").mkdir()
            (root / "logs/dag-nudge.jsonl").write_text('\n'.join(json.dumps(
                {"at": i + 101, "action": "luna", "verdict": "nudge", "result": "sent"}) for i in range(12)))
            events = hv.observer_recent(root, {}, None)
            self.assertEqual(len(events), 10)
            self.assertEqual(events[0]["at"], 112)
            self.assertEqual(events[-1]["at"], 103)

    def test_units_use_actual_watcher_and_do_not_mistake_startup_for_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "watch-unit.json").write_text('{"unit":"host-dag-watch"}')
            (root / "watch.json.heartbeat").write_text("100")
            output = ("Id=host-dag-watch.service\nLoadState=loaded\nActiveState=active\nSubState=running\n\n"
                      "Id=host-dag-dag-nudge.timer\nLoadState=loaded\nActiveState=active\nLastTriggerUSec=yesterday\n\n"
                      "Id=host-dag-telegram-bridge.service\nLoadState=loaded\nActiveState=active\nActiveEnterTimestamp=start\n")
            with mock.patch.object(hv.subprocess, "run", return_value=mock.Mock(stdout=output)) as run:
                rows = hv.observer_units(root)
            self.assertIn("show", run.call_args.args[0])
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[0]["unit"], "host-dag-watch.service")
            self.assertIn("心跳", rows[0]["heartbeat"])
            self.assertEqual(rows[1]["state"], "missing")
            self.assertIn("触发", rows[2]["heartbeat"])
            self.assertIn("心跳未知", rows[3]["heartbeat"])
            with mock.patch.object(hv.subprocess, "run", side_effect=OSError):
                self.assertTrue(all(r["state"] == "missing" for r in hv.observer_units(root)))

    def test_render_overdue_missing_data_and_coordinator_wait(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": {
                "T": {"updated_at": "1970-01-01T00:00:01Z", "evidence": str(root / "T")}}}}}))
            (root / "T").mkdir()
            p = root / "T/result.json"
            p.write_text("{}")
            os.utime(p, (100, 100))
            r = hv.Run(root)
            tl = v.Timeline(root / "absent")
            tl.running, tl.turn_started = True, 50
            tl.events = [v.Event(50, "start", "start"), v.Event(60, "tool", "wait_agent", result="running")]
            with mock.patch.object(hv, "observer_units", return_value=[]), mock.patch.object(r, "timeline", return_value=tl):
                lines = hv.observer_lines(r, now=701)
            text = '\n'.join(''.join(s for s, _ in line) for line in lines)
            for label in ("观察器", "守护单元", "未消费事件", "协调者", "顾问通知队列", "最近 10 条", "后台等待：有"):
                self.assertIn(label, text)
            self.assertTrue(any("T" in s and style == "fail" for line in lines for s, style in line))
            p.unlink()
            with mock.patch.object(hv, "observer_units", return_value=[]), mock.patch.object(r, "timeline", return_value=None):
                text = str(hv.observer_lines(r, now=701))
            self.assertIn("未消费事件（0）", text)
            self.assertIn("未知", text)
            tl.running = False
            tl.events.append(v.Event(700, "end", "done"))
            self.assertIn("后台等待：未见", hv.observer_coordinator(tl, THREAD))
            self.assertIn("闲", hv.observer_coordinator(tl, THREAD))

    def test_observer_scans_only_open_tickets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": {
                "OPEN": {"status": "CLAIMED", "state": "RUNNING", "updated_at": "1970-01-01T00:00:01Z",
                         "evidence": str(root / "OPEN")},
                "DONE": {"status": "ACCEPTED", "state": "ACCEPTED", "updated_at": "1970-01-01T00:00:01Z",
                         "evidence": str(root / "DONE")}}}}}))
            for tid in ("OPEN", "DONE"):
                (root / tid).mkdir()
                (root / tid / "result.json").write_text("{}")
            r = hv.Run(root)
            with mock.patch.object(hv, "observer_units", return_value=[]), \
                    mock.patch.object(r, "timeline", return_value=None), \
                    mock.patch.object(hv, "observer_artifacts", wraps=hv.observer_artifacts) as scan:
                text = str(hv.observer_lines(r))
            self.assertEqual(set(scan.call_args.args[1]), {"OPEN"})
            self.assertIn("未消费事件（1）", text)

    def test_enter_survives_order_shrinking_under_selection(self) -> None:
        # 11-18 user: Enter on the board crashed with IndexError after the background render shortened order.
        b, _ = BoardScrollTest().board([])
        b.view, b.order, b.sel = "board", ["ONLY"], 5
        b.default_tab = lambda tid: tid and "exec"
        self.assertFalse(b.handle(10))
        self.assertEqual(b.view, "detail")
        self.assertEqual(b.cur_tid(), "ONLY")

    def test_board_o_enters_and_o_or_escape_returns_without_changing_selection(self) -> None:
        b, _ = BoardScrollTest().board([])
        b.view, b.sel, b.flt = "board", 2, "needle"
        self.assertIn("o 观察器", b.keys_hint())
        for back in (ord("o"), 27):
            self.assertFalse(b.handle(ord("o")))
            self.assertEqual(b.view, "observer")
            self.assertIn("Esc/o", b.keys_hint())
            self.assertFalse(b.handle(back))
            self.assertEqual((b.view, b.sel, b.flt), ("board", 2, "needle"))
        b.view = "observer"
        b.name = lambda: "V"
        with mock.patch.object(b.r, "observer_view", return_value=[[('observer', '')]], create=True) as render:
            self.assertEqual(hv.Board.lines(b), [[('observer', '')]])
            render.assert_called_once_with()

    def test_slow_observer_scan_does_not_block_or_duplicate_and_failure_is_visible(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "run.json").write_text('{"batches": {}}')
            r = hv.Run(root)
            entered, release = threading.Event(), threading.Event()

            def slow_scan(snapshot):
                self.assertIsNot(snapshot, r)
                entered.set()
                release.wait(5)
                return [[("snapshot", "")]]

            with mock.patch.object(hv, "observer_lines", side_effect=slow_scan) as scan:
                try:
                    self.assertIn("扫描中", str(r.observer_view()))
                    self.assertTrue(entered.wait(2))
                    self.assertIn("等待只读扫描", str(r.observer_view()))
                    self.assertEqual(scan.call_count, 1)
                finally:
                    release.set()
                    r._observer["worker"].join(2)
                self.assertIn("snapshot", str(r.observer_view()))
                self.assertEqual(scan.call_count, 1)
            r._observer["finished"] = 0
            with mock.patch.object(hv, "observer_lines", side_effect=OSError):
                r.observer_view()
                r._observer["worker"].join(2)
            self.assertIn("读取失败", str(r.observer_view()))
            self.assertIn("snapshot", str(r.observer_view()))


class CliLogLookupTest(unittest.TestCase):
    def test_evidence_may_be_ticket_dir_round_dir_or_the_run_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cli = Path(tmp) / "T-01" / "round-1" / "cli"
            cli.mkdir(parents=True)
            (cli / "cli.ndjson").write_text("{}\n")
            run_file = cli / "cli-run.json"
            run_file.write_text(json.dumps({"unit": "host-dag-cli-claude-T-01-1", "log": str(cli / "cli.ndjson")}))
            (cli.parent / "result.json").write_text("{}")
            for evidence in (Path(tmp) / "T-01", cli.parent, run_file, cli.parent / "result.json"):
                self.assertEqual(v.find_cli_log("host-dag-cli-claude-T-01-1", str(evidence)), cli / "cli.ndjson")
            self.assertIsNone(v.find_cli_log("host-dag-cli-claude-other", str(run_file)))

    def test_evidence_may_be_a_sibling_dir_of_the_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            rnd = Path(tmp) / "T-01" / "round-3"
            (rnd / "attempt4").mkdir(parents=True)
            cli = rnd / "attempt4-runtime-resume1"
            cli.mkdir()
            (cli / "cli.ndjson").write_text("{}\n")
            (cli / "cli-run.json").write_text(json.dumps({"unit": "host-dag-cli-claude-T-01-2",
                                                          "log": str(cli / "cli.ndjson")}))
            self.assertEqual(v.find_cli_log("host-dag-cli-claude-T-01-2", str(rnd / "attempt4")), cli / "cli.ndjson")

    def test_review_cli_beside_the_run_dir_without_rounds(self) -> None:
        # 11-02: TOOL-LINT-IDLE-SLOTS kept run-r2-20261102/ and review-r2/cli/ side by side, evidence = run dir.
        with tempfile.TemporaryDirectory() as tmp:
            # Deep enough that _ticket_dir's six-level walk never globs the (large) shared TMPDIR.
            ticket = Path(tmp) / "a" / "b" / "c" / "b1" / "T-04"
            run = ticket / "run-r2-20261102"
            run.mkdir(parents=True)
            cli = ticket / "review-r2" / "cli"
            cli.mkdir(parents=True)
            (cli / "cli.ndjson").write_text("{}\n")
            (cli / "cli-run.json").write_text(json.dumps({"unit": "host-dag-cli-claude-T-04-REVIEW-1",
                                                          "log": str(cli / "cli.ndjson")}))
            self.assertEqual(v.find_cli_log("host-dag-cli-claude-T-04-REVIEW-1", str(run)), cli / "cli.ndjson")
            for n in range(v.SIBLING_SCAN_LIMIT):  # a crowded parent (a batch dir) is not scanned
                (ticket / f"X-{n}").mkdir()
            v._CLI_LOG_LOOKUP.clear()
            v._CLI_LOG_CACHE.clear()
            self.assertIsNone(v.find_cli_log("host-dag-cli-claude-T-04-REVIEW-1", str(run)))

    def test_newer_executor_thread_outranks_an_old_cli_dispatch(self) -> None:
        # T-R4 on 10-28: redispatched to a Codex thread at 19:5x while evidence still named the 14:48 CLI run.
        with tempfile.TemporaryDirectory() as tmp:
            run_file = Path(tmp) / "cli" / "cli-run.json"
            run_file.parent.mkdir()
            run_file.write_text(json.dumps({"unit": "host-dag-cli-claude-T-03-1"}))
            os.utime(run_file, (1_800_000_000, 1_800_000_000))
            ms = lambda sec: f"{sec * 1000:012x}"  # noqa: E731
            newer = f"{ms(1_800_000_600)[:8]}-{ms(1_800_000_600)[8:]}-7050-837a-47620e8ab083"
            older = f"{ms(1_799_999_400)[:8]}-{ms(1_799_999_400)[8:]}-7050-837a-47620e8ab083"
            t = {"evidence": str(run_file), "executor": newer}
            self.assertIsNone(v.dispatched_unit(t))
            self.assertEqual(v.effective_executor(t), newer)
            t["executor"] = older
            self.assertEqual(v.effective_executor(t), "host-dag-cli-claude-T-03-1")
            t["executor"] = "host-dag-cli-claude-T-03-0"
            self.assertEqual(v.effective_executor(t), "host-dag-cli-claude-T-03-1")

    def test_evidence_may_be_a_review_dispatch_in_another_round(self) -> None:
        # KV18-03/KV18-07 on 10-16: evidence named round-13/review-incremental-dispatch.json (a reviewer's
        # thread), while the executor's cli-run.json sat in round-13/next1340-cli/ or an earlier round.
        with tempfile.TemporaryDirectory() as tmp:
            ticket = Path(tmp) / "T-02"
            cli = ticket / "round-12" / "next1340-cli"
            cli.mkdir(parents=True)
            (cli / "cli.ndjson").write_text("{}\\n")
            (cli / "cli-run.json").write_text(json.dumps({"unit": "host-dag-cli-claude-T-02-3",
                                                          "log": str(cli / "cli.ndjson")}))
            review = ticket / "round-13" / "review-incremental-dispatch.json"
            review.parent.mkdir()
            review.write_text(json.dumps({"threadId": "01a1-review"}))
            self.assertEqual(v.find_cli_log("host-dag-cli-claude-T-02-3", str(review)), cli / "cli.ndjson")
            self.assertIsNone(v.find_cli_log("host-dag-cli-claude-T-02-other", str(review)))

    def test_grok_session_found_through_newest_dispatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sid = "01a13d08-8cbe-7380-9b09-7b7a39242bcd"
            logs = []
            for n in (5, 6):
                rnd = Path(tmp) / "T-01" / f"round-{n}"
                rnd.mkdir(parents=True)
                log = rnd / "grok.ndjson"
                log.write_text('{"direction": "send", "message": {}}\n')
                os.utime(log, (n, n))
                (rnd / "dispatch.json").write_text(json.dumps({"provider": "grok", "sessionId": sid, "log": str(log)}))
                logs.append(log)
            self.assertEqual(v.find_cli_log(sid, str(Path(tmp) / "T-01")), logs[1])
            self.assertEqual(v.find_cli_log(sid, str(logs[0].with_name("dispatch.json"))), logs[0])
            self.assertIs(v.cli_timeline_class(logs[1]), v.GrokTimeline)


class WaitOnTest(unittest.TestCase):
    def test_waiting_resource_must_name_its_resource(self) -> None:
        run = {"batches": {"V18": {"tickets": {"KV18-03": {"deps": []}}}}}
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(run, "V18", "KV18-03", {"state": "WAITING_RESOURCE"})
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(run, "V18", "KV18-03", {"state": "WAITING_RESOURCE", "wait_on": "window"})
        t = v.apply_ticket_update(run, "V18", "KV18-03", {"state": "WAITING_RESOURCE", "wait_on": "capacity:ZFS",
                                                         "wait_holder": "KV18-05"})
        self.assertEqual(v.wait_label(t["wait_on"], t["wait_holder"]), "存储容量 ZFS · 占用：KV18-05")
        t = v.apply_ticket_update(run, "V18", "KV18-03", {"state": "RUNNING"})
        self.assertNotIn("wait_on", t)


class ClaudeCoordinatorTimelineTest(unittest.TestCase):
    def test_claude_host_thread_reads_its_session_transcript(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            state, projects = Path(tmp) / "state", Path(tmp) / "projects"
            state.mkdir()
            thread, session, cwd = "claude-abc", "11111111-2222-3333-4444-555555555555", "/home/u/repo.x"
            (state / f"{thread}.json").write_text(json.dumps({
                "session": session, "cwd": cwd, "model": "opus[1m]",
                "turns": [{"status": "running", "pid": os.getpid(), "started": 1.0}]}))
            log = projects / "-home-u-repo-x" / f"{session}.jsonl"
            log.parent.mkdir(parents=True)
            log.write_text("\n".join(json.dumps(r) for r in [
                {"type": "user", "timestamp": "2026-10-16T10:17:46Z", "message": {"content": "接任协调者"}},
                {"type": "assistant", "timestamp": "2026-10-16T10:17:50Z",
                 "message": {"content": [{"type": "text", "text": "先核对在途票"}]}}]) + "\n")
            with mock.patch.object(v, "CLAUDE_HOST_STATE", state), mock.patch.object(v, "CLAUDE_PROJECTS", projects):
                self.assertEqual(v.find_claude_session(thread), log)
                tl = v.ClaudeSessionTimeline(log, thread)
                tl.refresh()
                self.assertTrue(tl.running)
                self.assertEqual(tl.model, "opus[1m]")
                self.assertEqual([e.kind for e in tl.events], ["start", "msg"])
                self.assertIsNone(v.find_claude_session("claude-missing"))


class GrokTimelineTest(unittest.TestCase):
    def test_agy_acp_log_from_hostdag_agy_acp_run(self) -> None:
        # 11-23: agy over the official ACP server; lines carry `ts`, model comes from session/set_model.
        def rec(ts: float, direction: str, method: str, **params: object) -> dict:
            return {"direction": direction, "ts": ts, "message": {"method": method, "params": params}}
        recs = [
            rec(10, "out", "session/new", cwd="/wt", mcpServers=[]),
            rec(11, "out", "session/set_model", sessionId="s", modelId="gemini-4-neon-high"),
            rec(12, "out", "session/prompt", sessionId="s", prompt=[{"type": "text", "text": "# T-1 review"}]),
            rec(13, "in", "session/update", update={"sessionUpdate": "tool_call", "toolCallId": "s:2",
                "title": "wc -c a.txt", "kind": "execute", "status": "in_progress",
                "rawInput": {"command_line": "wc -c a.txt"}}),
            rec(14, "in", "session/update", update={"sessionUpdate": "tool_call_update", "toolCallId": "s:2",
                "status": "completed", "rawOutput": {"exit_code": 3, "combinedOutput": "boom"}}),
            rec(15, "in", "session/update", update={"sessionUpdate": "usage_update", "used": 15045}),
            rec(16, "in", "session/update", update={"sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": "PASS"}}),
            {"direction": "observer", "ts": 17, "event": "lifecycle", "status": "completed",
             "stopReason": "end_turn"},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "cli.ndjson"
            log.write_text("".join(json.dumps(r) + "\n" for r in recs))
            self.assertIs(v.cli_timeline_class(log), v.GrokTimeline)
            tl = v.GrokTimeline(log)
            tl.refresh()
        self.assertEqual((tl.model, tl.cwd, tl.tokens, tl.running), ("gemini-4-neon-high", "/wt", 15045, False))
        kinds = [(e.kind, e.result, e.text) for e in tl.events]
        self.assertEqual(kinds[0][0], "start")
        self.assertEqual(kinds[1][:2], ("shell", "fail"))
        self.assertEqual(tl.events[1].code, 3)
        self.assertEqual(tl.events[0].ts, 12)
        self.assertEqual((kinds[-1][0], kinds[-1][2]), ("end", "PASS"))

    def test_turn_messages_and_tool_calls(self) -> None:
        def upd(ms: int, **u: object) -> dict:
            return {"direction": "receive", "message": {"method": "session/update", "params": {
                "update": u, "_meta": {"agentTimestampMs": ms, "totalTokens": 5000}}}}
        recs = [
            {"direction": "send", "message": {"method": "session/load", "params": {"cwd": "/wt"}}},
            {"direction": "send", "message": {"method": "session/prompt", "params": {
                "prompt": [{"type": "text", "text": "# KV17-05 round6\nbody"}]}}},
            upd(1000, sessionUpdate="agent_message_chunk", content={"text": "先做"}),
            upd(1100, sessionUpdate="agent_message_chunk", content={"text": "只读准备。"}),
            upd(1200, sessionUpdate="tool_call", toolCallId="a", title="run_terminal_command",
                rawInput={"command": "ls /x"}, _meta={"x.ai/tool": {"kind": "execute"}}),
            upd(1300, sessionUpdate="tool_call_update", toolCallId="a", status="completed",
                rawOutput={"exit_code": "2", "output_for_prompt": "exit: 2\nno such file"}),
            upd(1400, sessionUpdate="tool_call", toolCallId="b", title="Read `/a/b.md`", kind="read",
                status="completed"),
            upd(1500, sessionUpdate="tool_call", toolCallId="c", title="search_replace",
                rawInput={"file_path": "/a/c.py", "new_string": "x"}),
            upd(1600, sessionUpdate="tool_call_update", toolCallId="c", status="completed"),
            {"direction": "receive", "message": {"method": "_x.ai/session/prompt_complete",
                                                 "params": {"stopReason": "end_turn"}}},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "grok.ndjson"
            log.write_text("".join(json.dumps(r) + "\n" for r in recs))
            tl = v.GrokTimeline(log)
            tl.refresh()
        got = [(e.kind, e.text, e.result, e.code) for e in tl.events]
        self.assertEqual(got, [("start", "# KV17-05 round6", "", None), ("msg", "先做只读准备。", "", None),
                               ("shell", "ls /x", "fail", 2), ("tool", "Read /a/b.md", "ok", None),
                               ("patch", "/a/c.py", "ok", None), ("end", "先做只读准备。", "", None)])
        self.assertEqual(tl.events[2].detail, ["exit: 2", "no such file"])
        self.assertFalse(tl.running)
        self.assertEqual((tl.cwd, tl.tokens), ("/wt", 5000))


class SessionTabsTest(unittest.TestCase):
    """11-20 user: the always-empty `standards —` tab is gone unless a standards review is registered."""

    def test_standards_tab_only_with_a_registered_session(self) -> None:
        r = hv.Run(Path("/nonexistent"), load=False)
        self.assertEqual(hv.session_tabs(r, {"executor": "x"}), ["exec", "spec"])
        self.assertEqual(hv.session_tabs(r, {"reviews": {"spec": {"thread": "a"}}}), ["exec", "spec"])
        both = {"reviews": {"other": {"thread": "c"}, "standards": {"thread": "b"}, "spec": {"thread": "a"}}}
        self.assertEqual(hv.session_tabs(r, both), ["exec", "spec", "standards", "other"])


class AgyTimelineTest(unittest.TestCase):
    """11-19 user: agy executors showed no duration, last reply or log on the board."""

    def test_stamped_agy_stream_json_turn(self) -> None:
        def step(ts: float, idx: int, state: str, stype: str, **extra: object) -> dict:
            return {"event": "step_update", "step_update": {"step_index": idx, "state": state, "step_type": stype,
                                                            **extra}, "ts": ts}
        recs = [
            {"event": "init", "init": {"model": "gemini-4-neon-high", "cwd": "/wt"}, "ts": 100.0},
            step(101.0, 0, "DONE", "user_input"),
            step(102.0, 1, "ACTIVE", "agent_response", text_delta="先读"),
            step(103.0, 1, "DONE", "agent_response", text_delta="合同。\n"),
            step(104.0, 2, "ACTIVE", "tool", tool_name="run_command",
                 tool_info={"name": "run_command", "parameters": {"CommandLine": "ls /x"}}),
            step(105.0, 2, "DONE", "tool", tool_name="run_command",
                 tool_info={"name": "run_command", "parameters": {"CommandLine": "ls /x"},
                            "output": "Command exited with code 2.\nno such file"}),
            step(106.0, 3, "ACTIVE", "tool", tool_name="replace_file_content",
                 tool_info={"name": "replace_file_content", "parameters": {"TargetFile": "/wt/a.py"}}),
            step(107.0, 3, "DONE", "tool", tool_name="replace_file_content",
                 tool_info={"name": "replace_file_content", "parameters": {"TargetFile": "/wt/a.py"}}),
            {"event": "result", "result": {"status": "SUCCESS", "response": "完成\n",
                                           "usage": {"total_tokens": 22175}}, "ts": 110.0},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "cli.ndjson"
            log.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in recs))
            self.assertIs(v.cli_timeline_class(log), v.AgyTimeline)
            tl = v.AgyTimeline(log)
            tl.refresh()
        got = [(e.kind, e.text, e.result, e.code) for e in tl.events]
        self.assertEqual(got, [("start", "agy 开始执行", "", None), ("msg", "先读合同。", "", None),
                               ("shell", "ls /x", "fail", 2), ("patch", "/wt/a.py", "ok", None),
                               ("end", "完成", "", None)])
        self.assertFalse(tl.running)
        self.assertEqual((tl.model, tl.cwd, tl.tokens, tl.turn_started, tl.last_ts),
                         ("gemini-4-neon-high", "/wt", 22175, 100.0, 110.0))

    def test_running_agy_turn_and_unit_start_time(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "cli.ndjson"
            log.write_text(json.dumps({"event": "init", "init": {"model": "m"}, "ts": 50.0}) + "\n")
            tl = v.AgyTimeline(log)
            tl.refresh()
        self.assertTrue(tl.running)
        self.assertIsNotNone(v.executor_started("host-dag-cli-agy-TOOL-X-1119124750", 2026))

    def test_board_swaps_parser_when_empty_agy_log_gets_its_first_event(self) -> None:
        # 11-20 user: PROD-GC-CLOSURE-GATE-PER-ARCH's agy log only showed after restarting the board.
        unit = "host-dag-cli-agy-T-AGY-1120153507"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "run"
            out = root / "evidence" / "a" / "b" / "T-AGY" / "review-agy-r1"
            out.mkdir(parents=True)
            log = out / "cli.ndjson"
            log.write_text("")
            (out / "cli-run.json").write_text(json.dumps({"unit": unit, "log": str(log)}))
            (root / "run.json").write_text(json.dumps({"batches": {"B": {"tickets": {
                "T-AGY": {"status": "CLAIMED", "executor": unit, "evidence": str(out)}}}}}))
            v._CLI_LOG_LOOKUP.clear()
            v._CLI_LOG_CACHE.clear()
            r = hv.Run(root)
            self.assertIsInstance(r.timeline(unit), v.CliTimeline)
            log.write_text(json.dumps({"event": "init", "init": {"model": "gemini-4-neon-high"}, "ts": 50.0}) + "\n")
            tl = r.timeline(unit)
            self.assertIsInstance(tl, v.AgyTimeline)
            self.assertEqual(tl.model, "gemini-4-neon-high")
            self.assertTrue(tl.running)

    def test_existing_cli_and_grok_logs_keep_their_parsers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            claude = Path(tmp) / "a.ndjson"
            claude.write_text(json.dumps({"type": "system", "subtype": "init"}) + "\n")
            grok = Path(tmp) / "b.ndjson"
            grok.write_text(json.dumps({"direction": "send", "message": {}}) + "\n")
            self.assertIs(v.cli_timeline_class(claude), v.CliTimeline)
            self.assertIs(v.cli_timeline_class(grok), v.GrokTimeline)


class BatchListTest(unittest.TestCase):
    @staticmethod
    def batch(status: str, at: str) -> dict:
        return {"updated_at": at, "tickets": {"T-1": {"status": status, "accepted_at": at if status == "ACCEPTED" else None}}}

    def test_unfinished_plus_three_latest_finished_and_b_shows_all(self) -> None:
        run = {"batches": {
            "OLD1": self.batch("ACCEPTED", "2026-10-01T10:00:00+08:00"),
            "NOW": self.batch("CLAIMED", "2026-10-02T10:00:00+08:00"),
            "OLD2": self.batch("ACCEPTED", "2026-10-02T10:00:00+08:00"),
            "NEW1": self.batch("ACCEPTED", "2026-10-05T10:00:00+08:00"),
            "NEW2": self.batch("ACCEPTED", "2026-10-04T10:00:00+08:00"),
            "NEW3": self.batch("ACCEPTED", "2026-10-03T10:00:00+08:00"),
        }}
        self.assertEqual(v.visible_batch_names(run), ["NOW", "NEW1", "NEW2", "NEW3"])
        self.assertEqual(v.visible_batch_names(run, keep="OLD1"), ["NOW", "OLD1", "NEW1", "NEW2", "NEW3"])
        self.assertEqual(len(v.visible_batch_names(run, show_all=True)), 6)

    def test_title_is_recorded_or_taken_from_the_dag_heading(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "dag.md").write_text("intro\n# 云主机 存储对接 PoC DAG（B1）\n")
            self.assertEqual(v.batch_title({"dag": "dag.md"}, Path(tmp)), "云主机 存储对接 PoC")
            self.assertEqual(v.batch_title({"dag": "dag.md", "title": "自定"}, Path(tmp)), "自定")
            self.assertEqual(v.batch_title({"dag": "missing.md"}, Path(tmp)), "")


class BoardOddities20261023(unittest.TestCase):
    def test_reviewing_ticket_counts_as_implemented(self) -> None:
        batch = {"tickets": {"A": {"state": "ACCEPTED"}, "B": {"state": "REVIEWING", "executor": "x"},
                             "C": {"state": "RUNNING", "executor": "y"}}}
        p = v.progress(batch)
        self.assertEqual((p.total, p.implemented, p.accepted), (3, 2, 1))

    def test_timed_resource_wait_shows_as_scheduled(self) -> None:
        later = (datetime.now() + timedelta(hours=3)).astimezone().isoformat(timespec="seconds")
        batch = {"tickets": {"S": {"state": "WAITING_RESOURCE", "wait_on": "host:build-1-soak-report-time",
                                   "blocker": f"WAIT-UNTIL:{later} 后台观察"},
                             "R": {"state": "WAITING_RESOURCE", "wait_on": "host:build-1", "blocker": "等主机"}}}
        self.assertEqual(v.display_state(batch, "S")[0], "sched")
        self.assertEqual(v.display_state(batch, "R")[0], "res")


if __name__ == "__main__":
    unittest.main()


class WaitColumnsTest(unittest.TestCase):
    def test_wait_column_response_time_and_stale_waits(self) -> None:
        import tempfile, time as _t
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "control").mkdir()
            (root / "control/api-page-validation-pause.json").write_text(json.dumps({"owner": None, "status": "RESUMED"}))
            tickets = {"A": {"status": "ACCEPTED", "accepted_at": "2026-10-18T14:16:00+08:00"},
                       "B": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "wait_on": "ticket:A",
                             "wait_holder": "A", "updated_at": "2026-10-19T10:00:00+08:00"},
                       "C": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "wait_on": "page-window"}}
            run = __import__("types").SimpleNamespace(path=root)
            self.assertEqual(hv.wait_short(tickets["B"], "res"), "票 A")
            self.assertEqual(hv.wait_short({"wait_on": "ticket", "wait_holder": "A"}, "res"), "票 A")
            many = {**tickets, "D": {"status": "CLAIMED"}, "E": {"status": "CLAIMED"},
                    "W": {"deps": ["A", "D", "E"]}}
            self.assertEqual(hv.wait_short(many["W"], "wait", many), "票 D 等2张")
            self.assertEqual(hv.wait_short({"deps": ["A"]}, "wait", tickets), "前置都已验收")
            self.assertEqual(hv.ticket_cols({"tickets": {"KV18-03-FIX-SNAP-ORPHAN": {}}})[0][1], 24)
            self.assertIn("已验收", hv.wait_stale(run, tickets["B"], tickets))
            self.assertIn("页面空闲", hv.wait_stale(run, tickets["C"], tickets))
            detail = "".join(x for x, _ in hv.wait_detail(run, {**tickets["C"], "_id": "C"}, tickets))
            self.assertIn("页面现在空闲", detail)
            now = _t.time()
            self.assertTrue(hv.response_text(now - 30, now).endswith("前"))
            self.assertRegex(hv.response_text(now - 3 * 86400, now), r"^\d\d-\d\d \d\d:\d\d$")


class StageAWaitGuardTest(unittest.TestCase):
    """docs/local-dev/dag-state-machine-design-20261019.md stage A: invalid waits are refused at write."""

    def run_data(self):
        return {"batches": {"V18": {"tickets": {"KV18-02": {"status": "CLAIMED", "deps": [], "title": "x"},
                                                "T18-06": {"status": "CLAIMED", "title": "y"}}},
                            "B1": {"tickets": {"T-05": {"status": "ACCEPTED"}}}}}

    def test_waits_that_name_nothing_or_something_already_there_are_refused(self) -> None:
        import tempfile
        v = hv.v
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "advisor").mkdir()
            (root / "advisor/log.jsonl").write_text(json.dumps(
                {"key": "advisor:KV18-02:maint-orphan", "verdict": "decided", "decision": "D", "at": 1}) + "\n")
            cases = [
                ("KV18-02", {"state": "WAITING_DEPENDENCY"}, "没有未完成的前置票"),
                ("T18-06", {"state": "WAITING_RESOURCE", "wait_on": "ticket:T-05"}, "已验收"),
                ("T18-06", {"state": "WAITING_RESOURCE", "wait_on": "ticket:NOPE-1"}, "找不到"),
                ("KV18-02", {"state": "WAITING_RESOURCE", "wait_on": "decision:advisor:KV18-02:maint-orphan"}, "已裁定"),
            ]
            for tid, fields, needle in cases:
                with self.assertRaises(v.ViewError) as err:
                    v.apply_ticket_update(self.run_data(), "V18", tid, fields, run_dir=root)
                self.assertIn(needle, str(err.exception))
            ok = v.apply_ticket_update(self.run_data(), "V18", "KV18-02",
                                       {"state": "WAITING_RESOURCE", "wait_on": "page-window"}, run_dir=root)
            self.assertEqual(ok["wait_on"], "page-window")
            self.assertIsNone(v.answer_for(v.advisor_answers(root), ""))
            created = v.apply_ticket_update(self.run_data(), "V18", "KV18-02",
                                            {"state": "CREATED", "blocker": "排期：下一批"}, run_dir=root)
            self.assertEqual(created["state"], "CREATED")

    def test_pending_action_shows_on_the_board_and_clears_on_the_next_state_write(self) -> None:
        v = hv.v
        data = self.run_data()
        data["batches"]["V18"]["tickets"]["KV18-02"]["pending_action"] = {"kind": "apply_decision"}
        self.assertEqual(v.display_state(data["batches"]["V18"], "KV18-02"), ("ready", "待执行裁定"))
        t = v.apply_ticket_update(data, "V18", "KV18-02", {"state": "RUNNING"})
        self.assertNotIn("pending_action", t)


class ApproveWindowTest(unittest.TestCase):
    def test_approval_needs_a_granted_spec_and_queues_the_ticket(self) -> None:
        import tempfile
        v = hv.v
        with tempfile.TemporaryDirectory() as tmp:
            spec = Path(tmp) / "spec.json"
            spec.write_text(json.dumps({"owner": "o", "grant": {}}))
            data = {"batches": {"V18": {"tickets": {"T": {"status": "CLAIMED"}}}}}
            with self.assertRaises(v.ViewError):
                v.apply_ticket_update(data, "V18", "T", {"window_spec": str(spec)})
            spec.write_text(json.dumps({"owner": "o", "grant": {"allowRealWindow": True}}))
            t = v.apply_ticket_update(data, "V18", "T", {"window_spec": str(spec), "window_priority": 3})
            self.assertEqual((t["page_request"]["status"], t["page_request"]["priority"], t["wait_on"]),
                             ("pending", 3, "page-window"))
            everything = {"T": t, "U": {"page_request": {"id": "x", "status": "pending", "priority": 9}}}
            self.assertEqual(hv.wait_short(t, "res", everything), "页面队列 #2")
            data["batches"]["V18"]["tickets"]["T"]["page_request"]["status"] = "claimed"
            with self.assertRaises(v.ViewError):
                v.apply_ticket_update(data, "V18", "T", {"window_spec": ""})


class ScheduledFollowUpTest(unittest.TestCase):
    def test_created_with_future_wait_until_shows_scheduled(self) -> None:
        v = hv.v
        batch = {"tickets": {"F": {"state": "CREATED", "blocker": "WAIT-UNTIL:2099-10-22T09:00:00+08:00 排期：下一批"},
                             "G": {"state": "CREATED", "blocker": "WAIT-UNTIL:2000-01-01T00:00:00+08:00 过期"}}}
        self.assertEqual(v.display_state(batch, "F")[0], "sched")
        self.assertNotEqual(v.display_state(batch, "G")[0], "sched")


class FlowV2Test(unittest.TestCase):
    """Board after flow v2 (control/flow-rules-20261022.md): leftovers, closure, cumulative candidate, rounds."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.tmp.name)
        (self.run_dir / "control").mkdir()
        tickets = {
            "V17-FIX-A": {"status": "SUPERSEDED", "state": "CANCELED", "round": 2, "blocker": "DEFERRED"},
            "V17-FIX-B": {"status": "SUPERSEDED", "state": "CANCELED", "blocker": "CANCELLED（flow-rules §8）"},
            "V17-FIX-C": {"status": "CLAIMED", "state": "RUNNING", "round": 5},
            "V17-FIX-D": {"status": "ACCEPTED", "state": "ACCEPTED", "round": 3},
        }
        run = {"batches": {
            "V17": {"title": "V17", "tickets": tickets},
            "V18": {"title": "V18", "closure": "CLOSED-UNFINISHED",
                    "tickets": {"KV18-01": {"status": "CLAIMED", "state": "RUNNING"}}},
            "ARM-D1": {"title": "ARM-D1 ARM 交付批次", "tickets": {
                "KV18-08-SHARED-HANDOVER": {"state": "CREATED", "deps": [], "owner": "x", "title": "交接"}}},
        }}
        (self.run_dir / "run.json").write_text(json.dumps(run), encoding="utf-8")
        (self.run_dir / "watch.json").write_text(json.dumps({
            "parent_thread": "claude-2210270d-3c13-4d96-8b26-b23b32e3f4ab",
            "previous_parent_thread": THREAD}), encoding="utf-8")
        ledger = [
            {"ticket": "V17-FIX-A", "source": "V17-FIX-A", "category": "pre-existing",
             "finding": {"severity": "P2", "title": "旧格式：finding 内嵌"}},
            {"source": "V17-FIX-A", "category": "deferred", "severity": "P3", "description": "推下一批"},
            {"source": "V17-FIX-C", "category": "nit", "severity": "P2", "description": "运行中票的发现"},
            {"source": "OTHER-01", "category": "env", "severity": "P2", "description": "别的批次"},
        ]
        (self.run_dir / "control/leftover-ledger.jsonl").write_text(
            "\n".join(json.dumps(x, ensure_ascii=False) for x in ledger) + "\nnot json\n\n", encoding="utf-8")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def board(self, name: str) -> tuple[str, list]:
        lines, _ = hv.board_lines(hv.Run(self.run_dir), name, -1)
        return "\n".join("".join(s for s, _ in line) for line in lines), lines

    def row(self, lines: list, tid: str) -> list:
        return next(line for line in lines if line and line[0][0].startswith(tid + " "))

    def test_leftover_ledger_turns_superseded_into_leftover_but_not_others(self) -> None:
        r = hv.Run(self.run_dir)
        batch = r.batches()["V17"]
        self.assertEqual(v.display_state(batch, "V17-FIX-A"), ("left", ""))
        self.assertEqual(v.display_state(batch, "V17-FIX-B"), ("sup", ""))
        self.assertEqual(v.display_state(batch, "V17-FIX-C")[0], "run")  # a ledger row does not cancel a live ticket
        p = v.progress(batch)
        self.assertEqual((p.total, p.superseded, p.counts["left"], p.counts["sup"]), (2, 2, 1, 1))
        text, lines = self.board("V17")
        row_a, row_b = self.row(lines, "V17-FIX-A"), self.row(lines, "V17-FIX-B")
        self.assertIn("↪ 转遗留", "".join(s for s, _ in row_a))
        self.assertIn("推下一批", "".join(s for s, _ in row_a))  # the latest ledger row's description
        self.assertIn("⊘ 作废", "".join(s for s, _ in row_b))
        self.assertEqual(row_a[2][1], "left")
        self.assertEqual(row_b[2][1], "sup")
        self.assertIn("作废 1 · 转遗留 1（不计入总数）", text)
        self.assertNotIn("已取代", text)

    def test_ledger_reader_tolerates_formats_and_bad_lines(self) -> None:
        rows = v.load_leftovers(self.run_dir)
        self.assertEqual(len(rows), 4)
        self.assertEqual(v.leftover_text(rows[0]), "旧格式：finding 内嵌")
        self.assertEqual(v.leftover_ids(rows[0]), {"V17-FIX-A"})
        self.assertEqual(v.load_leftovers(self.run_dir / "missing"), [])
        self.assertEqual(v.load_leftovers(None), [])

    def test_ledger_with_invalid_utf8_does_not_crash_board(self) -> None:
        path = self.run_dir / "control/leftover-ledger.jsonl"
        path.write_bytes(path.read_bytes() + b'\xff\n{"source":"V17-FIX-B","description":"bad \xff text"}\n')
        text, lines = self.board("V17")
        self.assertIn("新流程 遗留 4 条", text)
        self.assertEqual(self.row(lines, "V17-FIX-A")[2][1], "left")
        self.assertEqual(self.row(lines, "V17-FIX-B")[2][1], "left")
        self.assertEqual(v.load_leftovers(self.run_dir)[-1]["description"], "bad \ufffd text")

    def test_candidate_set_with_invalid_utf8_reports_parse_error(self) -> None:
        (self.run_dir / "control/candidate-set.json").write_bytes(b'{"branch":"bad \xff"}')
        self.assertIn("candidate-set.json 无法解析", self.board("V17")[0])

    def test_flow_line_counts_batch_leftovers_and_reports_missing_candidate(self) -> None:
        text, _ = self.board("V17")
        self.assertIn("新流程 遗留 3 条 · 累计候选未建", text)
        self.assertNotIn("收口", text)
        text, _ = self.board("V18")
        self.assertIn("新流程 遗留 0 条 · 收口 CLOSED-UNFINISHED · 累计候选未建", text)

    def test_candidate_set_shows_branch_and_short_shas(self) -> None:
        (self.run_dir / "control/candidate-set.json").write_text(json.dumps({
            "branch": "cumulative/release-20261022", "updated_at": "2026-10-22T10:00:00+08:00",
            "repos": {"host-dag-container": "b68dca124aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                      "/home/x/web-app": {"sha": "0123456789abcdef0123"}}}))
        text, _ = self.board("V17")
        self.assertIn("累计候选 cumulative/release-20261022  host-dag-container b68dca12 web-app 01234567", text)
        (self.run_dir / "control/candidate-set.json").write_text(json.dumps([
            {"repo": "auth-svc", "sha": "abcdef1234567", "branch": "cumulative/x"}]))
        self.assertEqual(v.candidate_set(self.run_dir), {"branch": "", "repos": [], "error": "candidate-set.json 格式未知"})
        (self.run_dir / "control/candidate-set.json").write_text(json.dumps({
            "repos": [{"repo": "auth-svc", "sha": "abcdef1234567", "branch": "cumulative/x"}]}))
        self.assertEqual(v.candidate_set(self.run_dir), {"branch": "cumulative/x", "repos": [("auth-svc", "abcdef12")]})
        (self.run_dir / "control/candidate-set.json").write_text(json.dumps({  # T17-INTEGRATE's schemaVersion 1
            "schemaVersion": 1, "branch": "cumulative/release-20261022", "status": "ALIGNED_STEPS_1_THROUGH_3B",
            "shaTuple": {"host-dag-container": "7c3504b7193463db945c8f97b82b153bf137b455",
                         "auth-svc": "f3abbb7ad881061894ff26f56b9178b6c267d7cd"},
            "repositories": {"host-dag-container": {"sha": "7c3504b7193463db945c8f97b82b153bf137b455"}}}))
        self.assertEqual(v.candidate_set(self.run_dir), {"branch": "cumulative/release-20261022",
                                                         "repos": [("host-dag-container", "7c3504b7"), ("auth-svc", "f3abbb7a")]})
        (self.run_dir / "control/candidate-set.json").write_text("{broken")
        self.assertIn("无法解析", self.board("V17")[0])

    def test_batch_closure_labels(self) -> None:
        self.assertIsNone(v.batch_closure({}))
        self.assertEqual(v.batch_closure({"closure": "closed-scoped"}), "CLOSED-SCOPED")
        self.assertEqual(v.batch_closure({"outcome": "UNVERIFIED-BATCH"}), "UNVERIFIED-BATCH")
        self.assertEqual(v.batch_closure({"closure": {"status": "ACCEPTED"}}), "验收通过")
        self.assertEqual(v.batch_closure({"closed": False}), None)

    def test_round_budget_column_warns_past_three(self) -> None:
        self.assertEqual(v.round_budget({"round": 2}), ("第 2/3 轮", False))
        self.assertEqual(v.round_budget({"round": 3}), ("第 3/3 轮", False))
        self.assertEqual(v.round_budget({"round": 4}), ("第 4/3 轮", True))
        self.assertIsNone(v.round_budget({}))
        self.assertIsNone(v.round_budget({"round": "x"}))
        _, lines = self.board("V17")
        row_c, row_d = self.row(lines, "V17-FIX-C"), self.row(lines, "V17-FIX-D")
        self.assertEqual((row_c[4][0].strip(), row_c[4][1]), ("第 5/3 轮", "fail"))
        self.assertEqual((row_d[4][0].strip(), row_d[4][1]), ("第 3/3 轮", ""))
        self.assertEqual(self.row(lines, "V17-FIX-B")[4][0].strip(), "—")
        detail = "".join(s for s, _ in hv.detail_lines(hv.Run(self.run_dir), "V17", "V17-FIX-C", "exec")[0])
        self.assertIn("第 5/3 轮", detail)

    def test_flow_line_counts_latest_status_once(self) -> None:
        path = self.run_dir / "control/leftover-ledger.jsonl"
        rows = [{"id": "L1", "source": "V17-FIX-A", "status": "OPEN"},
                {"id": "L1", "source": "V17-FIX-A", "status": "CLOSED"},
                {"id": "L2", "source": "V17-FIX-A", "status": "SCHEDULED"},
                {"id": "L3", "source": "V17-FIX-B", "status": "OPEN"}]
        path.write_text("\n".join(json.dumps(x) for x in rows) + "\n")
        text, _ = self.board("V17")
        self.assertIn("新流程 遗留 2 条（其中 1 条已排进修复票） · 已关 1 条", text)

    def test_created_ticket_without_status_renders_and_batch_is_listed(self) -> None:
        r = hv.Run(self.run_dir)
        self.assertIn("ARM-D1", v.visible_batch_names(r.run))
        text, lines = self.board("ARM-D1")
        self.assertIn(" ARM-D1 ", text)
        self.assertIn("○ 可执行", "".join(s for s, _ in self.row(lines, "KV18-08-SHARED-HANDOVER")))
        self.assertIn("新流程 遗留 0 条", text)

    def test_round_budget_warns_only_for_unfinished_tickets(self) -> None:
        path = self.run_dir / "run.json"
        run = json.loads(path.read_text())
        tickets = run["batches"]["V17"]["tickets"]
        cases = {
            "ACCEPTED": {"state": "ACCEPTED"},
            "INTEGRATED": {"state": "INTEGRATED"},
            "CANCELED": {"state": "CANCELED"},
            "LEGACY-ACCEPTED": {"status": "ACCEPTED"},
            "SUPERSEDED": {"status": "SUPERSEDED"},
            "RUNNING": {"state": "RUNNING"},
            "REVIEWING": {"state": "REVIEWING"},
            "FAILED": {"state": "FAILED"},
        }
        for tid, record in cases.items():
            tickets[tid] = {**record, "round": 5}
        path.write_text(json.dumps(run), encoding="utf-8")
        r = hv.Run(self.run_dir)
        _, lines = self.board("V17")
        for tid in cases:
            with self.subTest(ticket=tid):
                warn = tid in {"RUNNING", "REVIEWING", "FAILED"}
                self.assertEqual(v.round_budget(tickets[tid]), ("第 5/3 轮", warn))
                budget, style = self.row(lines, tid)[4]
                self.assertEqual(budget.strip(), "第 5/3 轮")
                self.assertEqual(style == "fail", warn)
                detail = hv.detail_lines(r, "V17", tid, "exec")[0]
                budget, style = next((s, st) for s, st in detail if "第 5/3 轮" in s)
                self.assertEqual(style == "fail", warn)

    def test_canceled_ticket_moved_to_other_batch_takes_precedence_over_leftovers(self) -> None:
        path = self.run_dir / "run.json"
        run = json.loads(path.read_text())
        source = run["batches"]["V17"]["tickets"]
        target = run["batches"]["ARM-D1"]["tickets"]
        for tid in ("V17-FIX-A", "V17-FIX-B", "V17-FIX-C", "V17-FIX-D"):
            target[tid] = {"state": "CREATED"}
        # Even an old pending action must not override a canceled ticket's destination.
        source["V17-FIX-A"]["pending_action"] = {"kind": "apply_decision"}
        handover = "KV18-08-SHARED-HANDOVER"
        run["batches"]["V18"]["tickets"][handover] = {"state": "CANCELED"}
        path.write_text(json.dumps(run), encoding="utf-8")
        before = path.read_bytes()
        r = hv.Run(self.run_dir)
        batch = r.batches()["V17"]
        text, lines = self.board("V17")
        for tid in ("V17-FIX-A", "V17-FIX-B"):
            with self.subTest(ticket=tid):
                self.assertEqual(v.display_state(batch, tid), ("move", "ARM-D1"))
                row = self.row(lines, tid)
                self.assertEqual(row[2][0].strip(), "→ 移至 ARM-D1")
                self.assertEqual(row[2][1], "move")
                self.assertNotIn("⊘ 作废", "".join(s for s, _ in row))
                self.assertNotIn("↪ 转遗留", "".join(s for s, _ in row))
                detail = hv.detail_lines(r, "V17", tid, "exec")[0]
                self.assertIn(("→ 移至 ARM-D1", "move"), detail)
        self.assertEqual(v.display_state(batch, "V17-FIX-C")[0], "run")
        self.assertEqual(v.display_state(batch, "V17-FIX-D")[0], "ok")
        p = v.progress(batch)
        self.assertEqual((p.total, p.superseded, p.counts["move"], p.counts["left"], p.counts["sup"]),
                         (2, 2, 2, 0, 0))
        self.assertIn("移至其他批次 2（不计入总数）", text)
        _, rows = self.board("V18")
        self.assertEqual(self.row(rows, handover)[2][0].strip(), "→ 移至 ARM-D1")
        self.assertEqual(v.display_state(r.batches()["ARM-D1"], handover)[0], "ready")
        r.reload()
        self.assertEqual(path.read_bytes(), before)  # derived markers stay read-only

    def test_canceled_copies_in_two_batches_both_show_leftovers(self) -> None:
        path = self.run_dir / "run.json"
        run = json.loads(path.read_text())
        target = run["batches"]["ARM-D1"]["tickets"]
        target["V17-FIX-A"] = {"status": "SUPERSEDED"}
        target["V17-FIX-B"] = {"state": "CANCELED"}  # no ledger row: fall back to 作废
        path.write_text(json.dumps(run), encoding="utf-8")
        for name in ("V17", "ARM-D1"):
            with self.subTest(batch=name):
                _, lines = self.board(name)
                self.assertEqual(self.row(lines, "V17-FIX-A")[2][0].strip(), "↪ 转遗留")
                self.assertEqual(self.row(lines, "V17-FIX-B")[2][0].strip(), "⊘ 作废")

    def test_migration_chain_only_uses_effective_destination(self) -> None:
        path = self.run_dir / "run.json"
        run = json.loads(path.read_text())
        tid = "V17-FIX-A"
        run["batches"]["V18"]["tickets"][tid] = {
            "status": "SUPERSEDED", "updated_at": "2026-10-23T12:00:00+08:00"}
        run["batches"]["ARM-D1"]["tickets"][tid] = {
            "state": "CREATED", "updated_at": "2026-10-22T12:00:00+08:00"}
        path.write_text(json.dumps(run), encoding="utf-8")
        for name in ("V17", "V18"):
            with self.subTest(batch=name):
                _, lines = self.board(name)
                self.assertEqual(self.row(lines, tid)[2][0].strip(), "→ 移至 ARM-D1")
        r = hv.Run(self.run_dir)
        self.assertEqual(r.batches()["ARM-D1"]["tickets"][tid][v.MOVED_MARK], "")
        self.assertEqual(v.display_state(r.batches()["ARM-D1"], tid)[0], "ready")

    def test_multiple_effective_destinations_use_latest_update(self) -> None:
        path = self.run_dir / "run.json"
        run = json.loads(path.read_text())
        tid = "V17-FIX-A"
        # Different UTC offsets ensure this compares instants rather than timestamp strings.
        run["batches"]["V18"]["tickets"][tid] = {
            "state": "CREATED", "updated_at": "2026-10-22T10:00:00+00:00"}
        run["batches"]["ARM-D1"]["tickets"][tid] = {
            "state": "CREATED", "updated_at": "2026-10-22T12:00:00+08:00"}
        path.write_text(json.dumps(run), encoding="utf-8")
        self.assertEqual(self.row(self.board("V17")[1], tid)[2][0].strip(), "→ 移至 V18")
        # Legacy batch-level timestamps work when the ticket has none.
        del run["batches"]["ARM-D1"]["tickets"][tid]["updated_at"]
        run["batches"]["ARM-D1"]["updated_at"] = "2026-10-23T12:00:00+08:00"
        path.write_text(json.dumps(run), encoding="utf-8")
        self.assertEqual(self.row(self.board("V17")[1], tid)[2][0].strip(), "→ 移至 ARM-D1")

    def test_coordinator_is_the_claude_parent_thread(self) -> None:
        r = hv.Run(self.run_dir)
        self.assertEqual(r.coordinator(), "claude-2210270d-3c13-4d96-8b26-b23b32e3f4ab")
        self.assertEqual(r.coordinator_label(), "Claude·2210270d")
        self.assertEqual(hv.coordinator_label(THREAD), "Codex·01a11e85")
        self.assertEqual(hv.coordinator_label(""), "—")
        text, _ = self.board("V17")
        self.assertIn("协调者 Claude·2210270d", text)
        self.assertNotIn("Codex", text.splitlines()[0])

    def test_board_help_says_fix_tickets_open_no_page_window(self) -> None:
        res = subprocess.run([sys.executable, str(CLI), "board", "--help"], capture_output=True, text=True)
        self.assertEqual(res.returncode, 0, res.stderr)
        self.assertIn("修复票不再开页面窗口", res.stdout)


class CliLogLookupCache(unittest.TestCase):
    def test_miss_is_cached_until_ttl(self):
        calls = []
        orig = v.find_cli_log_uncached
        v.find_cli_log_uncached = lambda e, ev: calls.append(e) or None
        try:
            v._CLI_LOG_LOOKUP.clear()
            self.assertIsNone(v.find_cli_log("host-dag-grok-X", "/nowhere"))
            self.assertIsNone(v.find_cli_log("host-dag-grok-X", "/nowhere"))
            self.assertEqual(len(calls), 1)
            key = ("host-dag-grok-X", "/nowhere")
            v._CLI_LOG_LOOKUP[key] = (v._CLI_LOG_LOOKUP[key][0] - v.CLI_LOG_LOOKUP_TTL - 1, None)
            v.find_cli_log("host-dag-grok-X", "/nowhere")
            self.assertEqual(len(calls), 2)
        finally:
            v.find_cli_log_uncached = orig
            v._CLI_LOG_LOOKUP.clear()


class AlertViewTest(unittest.TestCase):
    """`a` lists every alert (no desktop filter), marks desktop ones and expands one on Enter."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.run_dir = Path(self.tmp.name) / "run"
        self.run_dir.mkdir()
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": {}}}}))
        rows = [{"at": 1_760_000_000 + i, "key": f"k{i}", "title": f"告警 {i}",
                 "body": f"第 {i} 条的正文\n第二段 {'长' * 5}", "desktop": i % 3 == 0} for i in range(45)]
        (self.run_dir / "alerts.jsonl").write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n")
        self.r = hv.Run(self.run_dir)

    def text(self, lines: list[list[tuple[str, str]]]) -> str:
        return "\n".join("".join(s for s, _ in line) for line in lines)

    def test_every_alert_is_listed_without_the_desktop_filter(self) -> None:
        rows = hv.alert_rows(self.r)
        self.assertEqual(len(rows), 45)  # more than the old 40-line tail
        lines = hv.alert_lines(rows)
        self.assertEqual(len(lines) - 1, 45)
        text = self.text(lines)
        self.assertIn("告警 0", text)
        self.assertIn("告警 44", text)
        self.assertIn("桌面", text)
        self.assertEqual(text.count("桌面"), sum(1 for r in rows if r.get("desktop")))
        self.assertNotIn("第 0 条的正文", text)  # bodies stay folded in the list
        self.assertIn("45 条", text)

    def test_a_selected_alert_expands_its_full_text(self) -> None:
        rows = hv.alert_rows(self.r)
        lines = hv.alert_lines(rows, sel=1, expanded=True)
        text = self.text(lines)
        self.assertIn("告警 1", text)
        self.assertIn("第 1 条的正文", text)
        self.assertIn("第二段", text)  # the whole body, not one clipped row
        self.assertIn("k1", text)
        self.assertNotIn("告警 0", text)  # only the selected entry is shown while expanded
        wrapped = hv.alert_lines(rows, sel=1, expanded=True, cols=40)
        self.assertGreater(len(wrapped), len(lines) - 1)

    def test_an_empty_or_missing_log_says_so(self) -> None:
        self.assertIn("没有告警", self.text(hv.alert_lines([])))
        (self.run_dir / "alerts.jsonl").unlink()
        self.assertEqual(hv.alert_rows(self.r), [])
        self.assertIn("没有 alerts.jsonl", self.text(hv.alert_lines(hv.alert_rows(self.r), missing=True)))


class BoardAlertKeysTest(unittest.TestCase):
    """Up/down pick, Enter expands, Esc collapses and then returns; the board read is untouched."""

    class Scr:
        def getmaxyx(self) -> tuple[int, int]:
            return 30, 120

    def board(self, run: hv.Run) -> object:
        import curses
        b = object.__new__(hv.Board)
        b.c, b.scr, b.r = curses, self.Scr(), run
        b.view, b.sel, b.tab, b.flt, b.bname = "alerts", 0, "exec", "", "V"
        b.pfilter, b.scroll, b.back, b.wrap, b.order, b.runs = v.FILTER_ALL, 0, "board", True, [], []
        b.show_all, b.hide_ok, b.gen, b.loaded, b.cache, b.rsel = False, hv.FOLD_LIVE, 0, 0, None, 0
        b.alerts, b.alert_sel, b.alert_expanded = [], -1, False
        b.alert_top, b.alert_follow_end, b.show_timings, b.timings, b.hot_warnings = 0, False, False, {}, []
        b.attr = {"": 0, "dim": 0, "sel": 0}
        b.lines()
        return b

    def text(self, lines: list[list[tuple[str, str]]]) -> str:
        return "\n".join("".join(s for s, _ in line) for line in lines)

    def test_selection_enter_and_escape(self) -> None:
        import curses
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            run_dir.mkdir()
            (run_dir / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": {}}}}))
            rows = [{"at": 1_760_000_000 + i, "key": f"k{i}", "title": f"告警 {i}", "body": f"正文 {i}",
                     "desktop": False} for i in range(5)]
            (run_dir / "alerts.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
            b = self.board(hv.Run(run_dir))
            self.assertEqual(len(b.alerts), 5)
            self.assertEqual(b.alert_sel, 4)  # opening `a` starts on the newest, like the old tail view
            self.assertFalse(b.handle(curses.KEY_UP))
            self.assertEqual(b.alert_sel, 3)
            self.assertIn("告警 3", self.text(b.lines()))
            b.handle(10)
            self.assertTrue(b.alert_expanded)
            expanded = self.text(b.lines())
            self.assertIn("正文 3", expanded)
            self.assertNotIn("告警 4", expanded)
            b.handle(27)
            self.assertFalse(b.alert_expanded)
            self.assertEqual(b.view, "alerts")
            b.handle(27)
            self.assertEqual(b.view, "board")
            # A picked entry survives the 2 s refresh: following the end is a separate flag.
            b.view, b.alert_sel, b.alert_follow_end = "alerts", 1, False
            b.lines()
            self.assertEqual(b.alert_sel, 1)
            b.alert_follow_end = True
            b.lines()
            self.assertEqual(b.alert_sel, 4)


class PeekFilterBoardTest(unittest.TestCase):
    """peek/coord Tab cycles all -> messages -> tools -> all; the title names the state."""

    def setUp(self) -> None:
        self.tl = v.Timeline(Path("/nonexistent/rollout.jsonl"))
        self.tl.running = False
        self.tl.events = [v.Event(1, "start", "任务"), v.Event(2, "shell", "ls -l", result="ok"),
                          v.Event(3, "msg", "说明")]

    def text(self, lines: list[list[tuple[str, str]]]) -> str:
        return "\n".join("".join(s for s, _ in line) for line in lines)

    def test_the_title_bar_names_the_state(self) -> None:
        for mode, label in (("all", "全部"), ("msg", "只看消息"), ("tool", "只看工具")):
            head = self.text(hv.peek_lines("协调者", "t", self.tl, {}, mode=mode))[:80]
            self.assertIn(label, head)

    def test_empty_filters_have_a_hint(self) -> None:
        only_tools = v.Timeline(Path("/nonexistent/x.jsonl"))
        only_tools.events = [v.Event(1, "shell", "ls", result="ok")]
        self.assertIn("无消息", self.text(hv.peek_lines("协调者", "t", only_tools, {}, mode="msg")))
        only_messages = v.Timeline(Path("/nonexistent/x.jsonl"))
        only_messages.events = [v.Event(1, "msg", "说明")]
        self.assertIn("无工具调用", self.text(hv.peek_lines("协调者", "t", only_messages, {}, mode="tool")))
        empty = v.Timeline(Path("/nonexistent/x.jsonl"))
        empty.events = []
        self.assertIn("无事件", self.text(hv.peek_lines("协调者", "t", empty, {}, mode="all")))
        self.assertIn("无事件", self.text(hv.peek_lines("协调者", "t", empty, {})))

    def test_tab_cycles_the_board_state_and_entering_resets_to_all(self) -> None:
        import curses
        b = object.__new__(hv.Board)
        b.c, b.scr = curses, type("S", (), {"getmaxyx": lambda self: (30, 120)})()
        b.view, b.pfilter, b.back, b.scroll, b.wrap = "peek", v.FILTER_MSG, "board", -1, True
        b.order, b.sel, b.tab = ["T-1"], 0, "exec"
        b.gen, b.attr = 0, {"": 0}
        b.bname, b.flt, b.rsel, b.show_all, b.hide_ok = "V", "", 0, False, False
        b.alerts, b.alert_sel, b.alert_expanded, b.hot_warnings = [], -1, False, []
        b.rendered = lambda: [[("row", "")]]
        b.handle(9)
        self.assertEqual(b.pfilter, v.FILTER_TOOL)
        b.handle(9)
        self.assertEqual(b.pfilter, v.FILTER_ALL)
        b.view = "board"
        b.handle(ord("p"))
        self.assertEqual((b.view, b.pfilter), ("peek", v.FILTER_ALL))
        b.handle(9)
        self.assertEqual(b.pfilter, v.FILTER_MSG)
        b.view = "coord"
        b.handle(27)
        b.handle(ord("c"))
        self.assertEqual((b.view, b.pfilter), ("coord", v.FILTER_ALL))
        # The footer repeats the state: the title scrolls away while following the end.
        for state, label in ((v.FILTER_ALL, "全部"), (v.FILTER_MSG, "只看消息"), (v.FILTER_TOOL, "只看工具")):
            b.pfilter = state
            self.assertIn("当前 " + label, hv.Board.keys_hint(b))

    def test_f_from_detail_enters_the_full_screen_peek_on_all(self) -> None:
        import curses
        b = object.__new__(hv.Board)
        b.c, b.scr = curses, type("S", (), {"getmaxyx": lambda self: (30, 120)})()
        b.view, b.pfilter, b.back, b.scroll, b.wrap = "detail", v.FILTER_TOOL, "board", 5, True
        b.order, b.sel, b.tab = ["T-1"], 0, "exec"
        b.gen, b.attr = 0, {"": 0}
        b.bname, b.flt, b.rsel, b.show_all, b.hide_ok = "V", "", 0, False, False
        b.alerts, b.alert_sel, b.alert_expanded, b.hot_warnings = [], -1, False, []
        b.rendered = lambda: [[("row", "")]]
        b.handle(ord("f"))
        self.assertEqual((b.view, b.pfilter, b.back, b.scroll), ("peek", v.FILTER_ALL, "detail", -1))


class PeekBoardWiringTest(unittest.TestCase):
    """Board peek/coord pass the alias table by keyword: the run path reads as $RUN, state stays 全部."""

    class Scr:
        def getmaxyx(self) -> tuple[int, int]:
            return 30, 120

    def board(self, run_dir: Path, view: str, thread: str) -> object:
        import curses
        r = hv.Run(run_dir)
        tl = v.Timeline(Path("/nonexistent/rollout.jsonl"))
        tl.running = False
        tl.events = [v.Event(1, "msg", f"证据 {run_dir}/evidence/a.md")]
        r.timeline = lambda _thread: tl  # type: ignore[method-assign]
        b = object.__new__(hv.Board)
        b.c, b.scr, b.r = curses, self.Scr(), r
        b.view, b.sel, b.tab, b.flt, b.bname = view, 0, "exec", "", "V"
        b.pfilter, b.scroll, b.back, b.wrap, b.order, b.runs = v.FILTER_ALL, 0, "board", True, [], []
        if view == "peek":
            b.order = ["T-1"]
        b.show_all, b.hide_ok, b.gen, b.loaded, b.cache, b.rsel = False, hv.FOLD_LIVE, 0, 0, None, 0
        b.alerts, b.alert_sel, b.alert_expanded = [], -1, False
        b.alert_top, b.alert_follow_end, b.show_timings, b.timings, b.hot_warnings = 0, False, False, {}, []
        b.attr = {"": 0, "dim": 0, "sel": 0}
        return b

    def text(self, lines: list[list[tuple[str, str]]]) -> str:
        return "\n".join("".join(s for s, _ in line) for line in lines)

    def render(self, view: str) -> str:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            run_dir.mkdir()
            (run_dir / "run.json").write_text(json.dumps({
                "coordinator_session": THREAD,
                "batches": {"V": {"tickets": {"T-1": {"executor": THREAD}}}},
            }))
            b = self.board(run_dir, view, THREAD)
            return self.text(b.lines()).replace(str(run_dir), "<RUN>")

    def test_coord_shortens_the_run_path_and_stays_on_all(self) -> None:
        body = self.render("coord")
        self.assertIn("$RUN/evidence/a.md", body)
        self.assertNotIn("<RUN>/evidence", body)
        self.assertIn("全部", body)
        self.assertNotIn("只看工具", body)

    def test_peek_shortens_the_run_path_and_stays_on_all(self) -> None:
        body = self.render("peek")
        self.assertIn("$RUN/evidence/a.md", body)
        self.assertNotIn("<RUN>/evidence", body)
        self.assertIn("全部", body)
        self.assertNotIn("只看工具", body)


class BoardFreezeReadTest(unittest.TestCase):
    """The live freeze is read once per board refresh, not once per row (614 opens on B1 全部)."""

    class Scr:
        def getmaxyx(self) -> tuple[int, int]:
            return 30, 160

    def test_one_board_refresh_opens_the_freeze_file_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            (run_dir / "control/locks").mkdir(parents=True)
            (run_dir / "control/locks/board-freeze.json").write_text(json.dumps({"owner": "B1-GATE"}))
            tickets = {f"T-{i}": {"state": "WAITING_DEPENDENCY",
                                 "blocker": "board-freeze (V): review queued", "title": f"票 {i}"}
                       for i in range(6)}
            (run_dir / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": tickets}}}))
            r = hv.Run(run_dir)
            real = hv.read_json
            with mock.patch.object(hv, "read_json", side_effect=real) as reader:
                lines, order = hv.board_lines(r, "V", 0)
            opened = [call for call in reader.call_args_list
                      if str(call.args[0]).endswith("board-freeze.json")]
            self.assertEqual(len(order), 6)
            self.assertEqual(len(opened), 1, f"one refresh opened the freeze file {len(opened)} times")
            body = "\n".join("".join(s for s, _ in line) for line in lines)
            self.assertIn("冻结排队", body)  # the read-once owner still reaches every row


class HotLinkWatchTest(unittest.TestCase):
    """The header only warns: an authority file the manifest calls linked is not a symlink,
    sits on another device, or the entry is detached. Nothing is ever re-linked or written."""

    RUN_ID = "01a0b493-176b-7a20-918b-6ffb0bd45c66"

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / "home"
        self.run_dir = self.root / "run"
        self.run_dir.mkdir()
        self.ssd = self.home / ".local/state/host-dag-hot-runs" / self.RUN_ID / "segment1"
        self.ssd.mkdir(parents=True)
        (self.run_dir / "run.json").write_text('{"batches":{}}')
        (self.run_dir / "watch.json").write_text("{}")
        self.write_manifest()

    def write_manifest(self, **entries: dict) -> None:
        rows = entries or {"run.json": {"phase": "linked"}, "watch.json": {"phase": "linked"}}
        state = {"run": str(self.run_dir), "ssd": str(self.ssd), "phase": "active",
                 "entries": [{"path": name, **fields} for name, fields in rows.items()]}
        path = self.ssd / "segment1-migration.json"
        path.write_text(json.dumps(state))

    def test_manifest_is_found_through_the_run_id(self) -> None:
        self.assertEqual(hv.hot_run_id(self.run_dir, {"task_thread": self.RUN_ID}), self.RUN_ID)
        self.assertEqual(hv.hot_run_id(self.run_dir), "")  # a plain directory name is not an id
        state = hv.hot_manifest(self.run_dir, self.RUN_ID, home=self.home)
        self.assertEqual({e["path"] for e in state["entries"]}, {"run.json", "watch.json"})
        self.assertEqual(hv.hot_link_check(self.run_dir, self.RUN_ID, home=self.home),
                         ["run.json 非软链", "watch.json 非软链"])
        self.assertFalse((self.run_dir / "run.json").is_symlink())  # warned, never repaired

    def test_a_link_on_the_expected_device_is_quiet(self) -> None:
        for name in ("run.json", "watch.json"):
            target = self.ssd / name
            target.write_text("{}")
            (self.run_dir / name).unlink()
            (self.run_dir / name).symlink_to(target)
        self.assertEqual(hv.hot_link_check(self.run_dir, self.RUN_ID, home=self.home), [])
        self.assertEqual(hv.hot_link_check(self.run_dir, self.RUN_ID, home=self.home,
                                           expected_device=os.stat(self.ssd).st_dev + 1),
                         ["run.json 不在预期设备", "watch.json 不在预期设备"])

    def test_detached_entries_are_skipped(self) -> None:
        self.write_manifest(**{"run.json": {"phase": "linked", "detached": True},
                               "watch.json": {"phase": "linked", "detached_at": 1_760_000_000}})
        self.assertEqual(hv.hot_link_check(self.run_dir, self.RUN_ID, home=self.home), [])

    def test_only_the_authority_files_are_checked(self) -> None:
        self.write_manifest(**{"run.json": {"phase": "linked"}, "advisor/log.jsonl": {"phase": "linked"},
                               "control/x.json": {"phase": "copied"}})
        (self.run_dir / "run.json").unlink()
        (self.run_dir / "run.json").symlink_to(self.ssd / "run.json")
        (self.ssd / "run.json").write_text("{}")
        self.assertEqual(hv.hot_link_check(self.run_dir, self.RUN_ID, home=self.home), [])

    def test_a_missing_manifest_is_not_an_error(self) -> None:
        self.assertEqual(hv.hot_link_check(self.run_dir, "", home=self.home), [])
        self.assertEqual(hv.hot_link_check(self.run_dir, "no-such-run", home=self.home), [])

    def test_the_background_check_reports_without_writing(self) -> None:
        before = {p.name: p.read_bytes() for p in sorted(self.run_dir.iterdir())}
        manifest = (self.ssd / "segment1-migration.json").read_bytes()
        # Hold the real disk check until the first frame returns. Without a
        # barrier a fast worker may legitimately publish its result immediately.
        import threading
        release = threading.Event()
        check = hv.hot_link_check
        def blocked(*args, **kwargs):
            if not release.wait(3):
                raise AssertionError("first poll waited for disk")
            return check(*args, **kwargs)
        with mock.patch.object(hv, "hot_link_check", side_effect=blocked):
            watch = hv.HotLinkWatch(self.run_dir, lambda: self.RUN_ID, home=self.home, interval=0)
            try:
                self.assertEqual(watch.poll(), [])
            finally:
                release.set()
            watch.worker.join(3)
        self.assertEqual(watch.poll(), ["run.json 非软链", "watch.json 非软链"])
        self.assertEqual({p.name: p.read_bytes() for p in sorted(self.run_dir.iterdir())}, before)
        self.assertEqual((self.ssd / "segment1-migration.json").read_bytes(), manifest)


class BoardHeaderAndTimingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.run_dir = Path(self.tmp.name) / "run"
        self.run_dir.mkdir()
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {"V": {"tickets": {}}}}))
        (self.run_dir / "watch.json").write_text(json.dumps({"parent_thread": THREAD}))
        self.r = hv.Run(self.run_dir)

    def text(self, lines: list[list[tuple[str, str]]]) -> str:
        return "\n".join("".join(s for s, _ in line) for line in lines)

    def test_a_broken_hot_link_is_red_in_the_title_bar(self) -> None:
        batch = self.r.batches()["V"]
        self.assertNotIn("软链断开", self.text([hv.header(self.r, batch)]))
        head = hv.header(self.r, batch, hot=["run.json 非软链", "watch.json 非软链"])
        self.assertIn("软链断开 2", self.text([head]))
        self.assertIn("fail", [style for _, style in head])
        lines, _ = hv.board_lines(self.r, "V", 0, hot=["run.json 非软链"])
        self.assertIn("软链断开 1", self.text(lines))

    def test_stage_timings_are_readable_in_the_board(self) -> None:
        line = hv.timing_line({"stat": .0001, "read": .002, "parse": .003, "render": .004}, fresh_at=1_760_000_000)
        text = self.text([line])
        self.assertIn("stat", text)
        self.assertIn("读", text)
        self.assertIn("解析", text)
        self.assertIn("渲染", text)
        self.assertIn("上次成功刷新", text)

    def test_t_toggles_the_timing_line_and_the_help_names_it(self) -> None:
        import curses
        b = object.__new__(hv.Board)
        b.c = curses
        b.view, b.show_timings, b.gen, b.scroll = "board", False, 0, 0
        b.order, b.sel, b.tab, b.pfilter, b.flt = ["T-1"], 0, "exec", v.FILTER_ALL, ""
        b.handle(ord("t"))
        self.assertTrue(b.show_timings)
        self.assertIn("t 计时", hv.Board.keys_hint(b))
        b.handle(ord("t"))
        self.assertFalse(b.show_timings)

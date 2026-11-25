from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import hostdag_self_heal as heal  # noqa: E402

WATCH = SCRIPTS / "host-dag-watch.py"
NOW = 1_800_000_000.0

# A host double that also understands --message-file and cancel, which the
# shared watcher fake does not need.
FAKE = r'''
import json, os, sys
from pathlib import Path
root = Path(os.environ["FAKE_DIR"])
argv = sys.argv[1:]
with (root / "calls.jsonl").open("a") as fh:
    fh.write(json.dumps(argv) + "\n")
def thread(tid):
    return json.loads((root / "threads" / f"{tid}.json").read_text())
def save(tid, value):
    (root / "threads" / f"{tid}.json").write_text(json.dumps(value))
cfg = json.loads((root / "config.json").read_text()) if (root / "config.json").exists() else {}
if argv[:1] == ["goal"]:
    print(json.dumps({"changed": False, "goal": None})); sys.exit(0)
action, tid = argv[1], argv[2]
if action == "read":
    if "--view" in argv:
        print(json.dumps({"threadId": tid, "messages": [], "nextCursor": None})); sys.exit(0)
    print(json.dumps(thread(tid))); sys.exit(0)
if action == "cancel":
    if cfg.get("cancel") == "fail":
        print(json.dumps({"error": {"code": "STOP_UNCONFIRMED", "message": "no terminal"}})); sys.exit(1)
    t = thread(tid); t["status"] = "cancelled"; save(tid, t)
    print(json.dumps({"threadId": tid, "status": "cancelled"})); sys.exit(0)
if action == "send":
    text = argv[argv.index("--message") + 1] if "--message" in argv else \
        Path(argv[argv.index("--message-file") + 1]).read_text()
    with (root / "sent.jsonl").open("a") as fh:
        fh.write(json.dumps({"thread": tid, "message": text}, ensure_ascii=False) + "\n")
    t = thread(tid); t["status"] = "running"; t["result"] = {"turnId": "turn-after-send", "text": ""}; save(tid, t)
    print(json.dumps({"threadId": tid, "status": "running"})); sys.exit(0)
print(json.dumps({"error": {"code": "INVALID_ARGUMENT", "message": "fake"}})); sys.exit(1)
'''


def rollout(path: Path, records: list[dict], mtime: float) -> str:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    os.utime(path, (mtime, mtime))
    return str(path)


def call(call_id: str) -> dict:
    return {"type": "response_item", "payload": {"type": "custom_tool_call", "call_id": call_id}}


def output(call_id: str) -> dict:
    return {"type": "response_item", "payload": {"type": "custom_tool_call_output", "call_id": call_id}}


class ActivityTest(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory(prefix="selfheal-")
        self.addCleanup(self._td.cleanup)
        self.dir = Path(self._td.name)

    def test_open_tool_call_waits_on_tool(self) -> None:
        path = rollout(self.dir / "r.jsonl", [call("a"), output("a"), call("b")], NOW)
        self.assertEqual(heal.turn_activity(path)["waiting"], "tool")

    def test_tool_output_waits_on_model(self) -> None:
        path = rollout(self.dir / "r.jsonl", [call("a"), output("a"),
                                              {"type": "event_msg", "payload": {"type": "token_count"}}], NOW)
        self.assertEqual(heal.turn_activity(path)["waiting"], "model")

    def test_completed_turn_is_idle(self) -> None:
        path = rollout(self.dir / "r.jsonl", [output("a"), {"type": "event_msg", "payload": {"type": "task_complete"}}], NOW)
        self.assertEqual(heal.turn_activity(path)["waiting"], "idle")

    def test_stall_thresholds(self) -> None:
        model = {"mtime": NOW - heal.MODEL_STALL_SECONDS, "waiting": "model"}
        self.assertEqual(heal.stall(model, NOW), "model")
        self.assertIsNone(heal.stall({**model, "mtime": NOW - 60}, NOW))
        tool = {"mtime": NOW - heal.MODEL_STALL_SECONDS * 2, "waiting": "tool"}
        self.assertIsNone(heal.stall(tool, NOW), "a long build is not a stall")
        self.assertEqual(heal.stall({**tool, "mtime": NOW - heal.TOOL_STALL_ALERT_SECONDS}, NOW), "tool")
        self.assertIsNone(heal.stall(None, NOW))

    def test_transient_classifier(self) -> None:
        self.assertTrue(heal.is_transient("Error running remote compact task: Connection failed: error sending request"))
        self.assertTrue(heal.is_transient("stream disconnected before completion"))
        self.assertFalse(heal.is_transient("jest: 3 tests failed"))


class WatcherSelfHealTest(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory(prefix="selfheal-watch-")
        self.addCleanup(self._td.cleanup)
        self.base = Path(self._td.name)
        self.fake_dir = self.base / "fake"
        (self.fake_dir / "threads").mkdir(parents=True)
        self.fake = self.base / "fake_host.py"
        self.fake.write_text(FAKE)
        self.state = self.base / "watch.json"
        self.env = {**os.environ, "FAKE_DIR": str(self.fake_dir), "HOST_DAG_WATCH_NOTIFY": "0",
                    "HOST_DAG_CODEX_SERVER_PID_FILE": str(self.base / "app-server.pid")}
        self.watch("init", "--parent-thread", "parent", "--host-cli", str(self.fake), "--host-cwd", str(self.base),
                 "--poll-interval", "30", "--direction-interval", "360000", "--host-timeout", "10")

    def watch(self, *args: str, now: float | None = None) -> dict:
        argv = [sys.executable, str(WATCH), "--state", str(self.state)]
        if now is not None:
            argv += ["--now", str(now)]
        done = subprocess.run(argv + list(args), capture_output=True, text=True, env=self.env, timeout=60)
        self.assertIn(done.returncode, (0, 3), done.stdout + done.stderr)
        return json.loads(done.stdout) if done.stdout.strip() else {}

    def thread(self, tid: str, status: str, text: str = "", turn: str = "t1", records=None, idle: float = 0) -> None:
        path = None
        if records is not None:
            path = rollout(self.base / f"{tid}.jsonl", records, NOW - idle)
        (self.fake_dir / "threads" / f"{tid}.json").write_text(json.dumps({
            "threadId": tid, "status": status, "result": {"turnId": turn, "text": text, "availability": "available"},
            "path": path}))

    def calls(self) -> list[list[str]]:
        path = self.fake_dir / "calls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def sent(self) -> list[dict]:
        path = self.fake_dir / "sent.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def alerts(self) -> list[dict]:
        path = self.base / "alerts.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def cancels(self, tid: str) -> list[list[str]]:
        return [c for c in self.calls() if c[:3] == ["thread", "cancel", tid]]

    def test_parent_stuck_on_model_is_interrupted_and_backlog_delivered(self) -> None:
        self.thread("worker", "completed", "REVIEW_READY")
        self.thread("parent", "running", records=[call("a"), output("a")], idle=heal.MODEL_STALL_SECONDS + 5)
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        result = self.watch("poll", now=NOW)
        self.assertEqual(result.get("send"), "delivered", result)
        self.assertEqual(self.cancels("parent")[0][-1], "t1")
        message = self.sent()[0]["message"]
        self.assertIn("watcher 已中断该轮", message)
        self.assertIn("KVF-09", message)
        self.assertTrue(any(a["title"] == "父协调者卡住已自动中断" for a in self.alerts()))

    def test_parent_in_long_tool_is_never_interrupted(self) -> None:
        self.thread("worker", "completed", "done")
        self.thread("parent", "running", records=[call("build")], idle=heal.TOOL_STALL_ALERT_SECONDS + 5)
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        result = self.watch("poll", now=NOW)
        self.assertEqual(result.get("send"), "held_busy")
        self.assertEqual(self.cancels("parent"), [])
        self.assertTrue(any("工具调用" in a["title"] for a in self.alerts()))

    def test_recently_active_parent_keeps_holding(self) -> None:
        self.thread("worker", "completed", "done")
        self.thread("parent", "running", records=[call("a"), output("a")], idle=60)
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        self.assertEqual(self.watch("poll", now=NOW).get("send"), "held_busy")
        self.assertEqual(self.cancels("parent"), [])

    def test_parent_whose_last_turn_failed_still_receives_events(self) -> None:
        self.thread("worker", "completed", "done")
        self.thread("parent", "failed", "stream disconnected")
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        self.assertEqual(self.watch("poll", now=NOW).get("send"), "delivered")

    def test_transient_worker_failure_retries_with_backoff_then_escalates(self) -> None:
        self.thread("parent", "completed")
        self.thread("worker", "failed", "Error running remote compact task: Connection failed", turn="w1")
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.assertEqual(self.sent(), [], "first sighting waits for the backoff")
        self.watch("poll", now=NOW + heal.RETRY_BACKOFF_SECONDS[0] + 1)
        self.assertEqual(len(self.sent()), 1)
        self.assertEqual(self.sent()[0]["thread"], "worker")
        self.assertIn("自动续跑", self.sent()[0]["message"])
        # Second blip on the resumed turn, then a third that exhausts the budget.
        self.thread("worker", "failed", "stream disconnected before completion", turn="w2")
        self.watch("poll", now=NOW + 10_000)
        self.watch("poll", now=NOW + 10_000 + heal.RETRY_BACKOFF_SECONDS[1] + 1)
        self.assertEqual([s["thread"] for s in self.sent()], ["worker", "worker"])
        self.thread("worker", "failed", "Connection failed again", turn="w3")
        self.watch("poll", now=NOW + 50_000)
        parent_msgs = [s for s in self.sent() if s["thread"] == "parent"]
        self.assertEqual(len(parent_msgs), 1)
        self.assertIn("failed", parent_msgs[0]["message"])
        self.assertTrue(any(a["title"].endswith("自动续跑已用完") for a in self.alerts()))

    def test_product_failure_goes_to_parent_without_retry(self) -> None:
        self.thread("parent", "completed")
        self.thread("worker", "failed", "jest: 3 tests failed")
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.assertEqual([s["thread"] for s in self.sent()], ["parent"])

    def test_worker_stuck_on_model_is_interrupted_then_resumed(self) -> None:
        self.thread("parent", "completed")
        self.thread("worker", "running", records=[call("a"), output("a")], idle=heal.MODEL_STALL_SECONDS + 5)
        self.watch("track", "--ticket", "KVF-09", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.assertEqual(self.cancels("worker")[0][-1], "t1")
        self.watch("poll", now=NOW + 30)
        self.watch("poll", now=NOW + 30 + heal.RETRY_BACKOFF_SECONDS[0] + 1)
        self.assertEqual([s["thread"] for s in self.sent()], ["worker"])
        self.assertIn("watcher 中断了卡住的一轮", self.sent()[0]["message"])

    def server(self, pid: int, ticks: int) -> None:
        (self.base / "app-server.pid").write_text(json.dumps({
            "pid": pid, "processIdentity": {"bootId": "boot-1", "startTicks": ticks}}))

    def test_worker_cut_off_by_server_restart_is_resumed_and_parent_told_why(self) -> None:
        # 2026-10-08: systemd-oomd killed the app-server's cgroup; both running turns came back "cancelled".
        self.server(100, 1)
        self.thread("parent", "completed")
        self.thread("worker", "running", turn="w1")
        self.watch("track", "--ticket", "R3-D", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.server(200, 2)
        self.thread("worker", "cancelled", turn="w1")
        self.watch("poll", now=NOW + 30)
        self.watch("poll", now=NOW + 30 + heal.RETRY_BACKOFF_SECONDS[0] + 1)
        worker_msgs = [s for s in self.sent() if s["thread"] == "worker"]
        self.assertEqual(len(worker_msgs), 1)
        self.assertIn("Codex 后台服务重启", worker_msgs[0]["message"])
        parent_msgs = [s["message"] for s in self.sent() if s["thread"] == "parent"]
        self.assertTrue(any("不是用户取消" in m and "R3-D" in m for m in parent_msgs), parent_msgs)
        self.assertFalse(any("session cancelled" in m for m in parent_msgs), parent_msgs)

    def test_cancel_on_the_same_server_still_goes_to_parent(self) -> None:
        self.server(100, 1)
        self.thread("parent", "completed")
        self.thread("worker", "running", turn="w1")
        self.watch("track", "--ticket", "R3-D", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.thread("worker", "cancelled", turn="w1")
        self.watch("poll", now=NOW + 30)
        self.watch("poll", now=NOW + 30 + heal.RETRY_BACKOFF_SECONDS[0] + 1)
        self.assertEqual([s["thread"] for s in self.sent()], ["parent"])

    def test_new_turn_cancelled_after_a_restart_is_not_blamed_on_it(self) -> None:
        self.server(100, 1)
        self.thread("parent", "completed")
        self.thread("worker", "running", turn="w1")
        self.watch("track", "--ticket", "R3-D", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)
        self.server(200, 2)
        self.thread("worker", "cancelled", turn="w2")
        self.watch("poll", now=NOW + 30)
        self.watch("poll", now=NOW + 30 + heal.RETRY_BACKOFF_SECONDS[0] + 1)
        self.assertEqual([s["thread"] for s in self.sent()], ["parent"])

    def uncertain(self, marker: str) -> None:
        state = json.loads(self.state.read_text())
        state["pending_send"] = {"marker": marker, "status": "uncertain", "message": marker + "\nx",
                                 "event_n": 1, "saved_at": time.time() - 600}
        self.state.write_text(json.dumps(state))

    def test_uncertain_send_found_in_parent_rollout_is_delivered(self) -> None:
        marker = "HOST-DAG-WATCH-MARKER:aaaa"
        self.thread("worker", "completed", "done")
        self.watch("track", "--ticket", "KV9-08", "--thread", "worker", now=NOW - 1)
        self.watch("poll", now=NOW)  # queue the completion
        self.thread("parent", "completed", records=[{"type": "response_item", "payload": {
            "type": "message", "role": "user", "content": [{"text": marker}]}}])
        self.uncertain(marker)
        self.watch("poll", now=NOW + 30)
        state = json.loads(self.state.read_text())
        self.assertNotEqual((state.get("pending_send") or {}).get("status"), "uncertain")

    def test_uncertain_send_absent_from_idle_parent_rollout_is_redelivered(self) -> None:
        self.thread("worker", "completed", "REVIEW_READY")
        self.thread("parent", "completed", records=[output("a")])
        self.watch("track", "--ticket", "KV9-08", "--thread", "worker", now=NOW - 1)
        self.uncertain("HOST-DAG-WATCH-MARKER:never-arrived")
        self.watch("poll", now=NOW)
        sent = [s for s in self.sent() if s["thread"] == "parent"]
        self.assertEqual(len(sent), 1)
        self.assertIn("KV9-08", sent[0]["message"])

    def test_watch_writes_heartbeat(self) -> None:
        self.watch("watch", "--max-cycles", "1")
        self.assertTrue(Path(str(self.state) + ".heartbeat").exists())


class WatchdogTest(unittest.TestCase):
    def setUp(self) -> None:
        import importlib.util
        from unittest import mock
        spec = importlib.util.spec_from_file_location("watchdog", SCRIPTS / "host-dag-watchdog.py")
        assert spec and spec.loader
        self.dog = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.dog)
        self._td = tempfile.TemporaryDirectory(prefix="watchdog-")
        self.addCleanup(self._td.cleanup)
        base = Path(self._td.name)
        self.state = base / "watch.json"
        self.config = base / "watch-unit.json"
        self.config.write_text(json.dumps({"unit": "w", "workdir": str(base), "state": str(self.state),
                                           "argv": ["/usr/bin/python3", "watch.py", "--state", str(self.state), "watch"]}))
        self.started: list[dict] = []
        patches = [
            mock.patch.object(self.dog, "ensure_codex_daemon", return_value="ok"),
            mock.patch.object(self.dog, "start_watcher", side_effect=lambda conf: self.started.append(conf) or
                              subprocess.CompletedProcess([], 0, "", "")),
            mock.patch.object(self.dog, "systemctl", return_value=subprocess.CompletedProcess([], 0, "", "")),
            mock.patch.dict(os.environ, {"HOST_DAG_WATCH_NOTIFY": "0"}),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.mock = mock

    def check(self, unit, sleep=lambda _s: None) -> None:
        import argparse
        states = iter(unit) if isinstance(unit, list) else None
        state = (lambda _u: next(states)) if states else (lambda _u: unit)
        with self.mock.patch.object(self.dog, "unit_state", side_effect=state):
            self.dog.cmd_check(argparse.Namespace(config=str(self.config), sleep=sleep))

    def beat(self, age: float) -> None:
        Path(str(self.state) + ".heartbeat").write_text(str(time.time() - age))

    def test_fresh_heartbeat_leaves_watcher_alone(self) -> None:
        self.beat(30)
        self.check("active")
        self.assertEqual(self.started, [])

    def test_silent_watcher_is_restarted_with_recorded_argv(self) -> None:
        self.beat(self.dog.STALE_SECONDS + 5)
        self.check("active")
        self.assertEqual(self.started[0]["argv"][-1], "watch")
        alerts = (self.state.parent / "alerts.jsonl").read_text()
        self.assertIn("已自动重启", alerts)

    def test_failed_unit_is_recreated(self) -> None:
        self.beat(5)
        self.check("failed")
        self.assertEqual(len(self.started), 1)

    def test_coordinator_track_window_is_not_a_failure(self) -> None:
        self.beat(5)
        slept: list[float] = []
        self.check(["inactive", "active"], sleep=slept.append)  # stopped for tracking, back 30 s later
        self.assertEqual((self.started, slept), ([], [self.dog.RECHECK_SECONDS]))

    def test_restart_race_with_coordinator_counts_as_restarted(self) -> None:
        self.beat(5)
        with self.mock.patch.object(self.dog, "start_watcher",
                                    return_value=subprocess.CompletedProcess([], 1, "", "unit already exists")):
            self.check(["inactive", "inactive", "active"])
        alerts = (self.state.parent / "alerts.jsonl").read_text()
        self.assertIn("已自动重启", alerts)
        self.assertNotIn("重启失败", alerts)

    def test_pending_systemd_restart_is_awaited_not_raced(self) -> None:
        self.beat(200)  # watcher exited 3 on a host-CLI TIMEOUT; RestartSec=60 pending
        self.check(["activating", "activating", "activating", "active"])
        self.assertEqual(self.started, [])
        rows = [json.loads(line) for line in (self.state.parent / "alerts.jsonl").read_text().splitlines()]
        self.assertTrue(any("systemd 自动重启" in r.get("title", "") for r in rows))
        self.assertNotIn("重启失败", json.dumps(rows, ensure_ascii=False))

    def test_restart_that_never_comes_is_still_recreated(self) -> None:
        self.beat(200)
        self.check(["activating"] * 5 + ["failed"])
        self.assertEqual(len(self.started), 1)


class MilestoneTest(unittest.TestCase):
    def setUp(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("watchdog_m", SCRIPTS / "host-dag-watchdog.py")
        assert spec and spec.loader
        self.dog = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.dog)
        self._td = tempfile.TemporaryDirectory(prefix="milestone-")
        self.addCleanup(self._td.cleanup)
        self.root = Path(self._td.name)
        self.conf = {"evidence": str(self.root), "state": str(self.root / "watch.json")}
        (self.root / "watch.json").write_text(json.dumps({"tracked": {}}))
        (self.root / "run.json").write_text(json.dumps({"batches": {}}))

    def write(self, rel: str, status: str, mtime: float) -> None:
        path = self.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"status": status}))
        os.utime(path, (mtime, mtime))
        state_path = self.root / "watch.json"
        state = json.loads(state_path.read_text())
        ticket = self.dog.ticket_of(path, self.root)
        state["tracked"][ticket] = {"evidence": str(path.parent)}
        state_path.write_text(json.dumps(state))

    def test_ticket_is_found_at_any_depth(self) -> None:
        for rel, ticket in [("demo-local-20260927/v8/KV8-01/parent-acceptance.json", "KV8-01"),
                            ("v7/KV7-11/a/b/c/result.json", "KV7-11"), ("LD-22/result.json", "LD-22")]:
            self.assertEqual(self.dog.ticket_of(self.root / rel, self.root), ticket)

    def test_nested_acceptance_is_a_stage_not_the_ticket(self) -> None:
        seen = {"last_scan": NOW - 60}
        self.write("v8/KV8-09/source-review/parent-acceptance.json", "ACCEPTED_SOURCE_SCOPE", NOW - 10)
        titles = [t for _k, t, _b in self.dog.scan_milestones(self.conf, seen, NOW)]
        self.assertEqual(titles, ["KV8-09 阶段验收（source-review）"])

    def test_first_scan_only_sets_a_starting_point(self) -> None:
        self.write("v7/KV7-01/parent-acceptance.json", "ACCEPTED_SOURCE_SCOPE", NOW - 100)
        seen: dict = {}
        self.assertEqual(self.dog.scan_milestones(self.conf, seen, NOW), [])
        self.assertEqual(seen["last_scan"], NOW)

    def test_new_acceptance_and_blocked_are_announced_once(self) -> None:
        seen = {"last_scan": NOW - 60}
        self.write("v7/KV7-10/parent-acceptance.json", "ACCEPTED_SOURCE_SCOPE", NOW - 30)
        self.write("v7/KV7-11/result.json", "BLOCKED", NOW - 20)
        self.write("v7/KV7-09/parent-acceptance.json", "ACCEPTED_SOURCE_SCOPE", NOW - 600)
        found = self.dog.scan_milestones(self.conf, seen, NOW)
        self.assertEqual(sorted(t for _k, t, _b in found), ["KV7-10 已验收", "KV7-11 被阻塞，需要处理"])
        self.assertEqual(self.dog.scan_milestones(self.conf, seen, NOW + 600), [])


if __name__ == "__main__":
    unittest.main()


class WatchdogCodexStartTest(unittest.TestCase):
    """2026-10-09: a daemon started from the oneshot watchdog unit lives in that unit's cgroup."""

    def setUp(self) -> None:
        import importlib.util
        from unittest import mock
        spec = importlib.util.spec_from_file_location("watchdog_codex", SCRIPTS / "host-dag-watchdog.py")
        assert spec and spec.loader
        self.dog = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.dog)
        self._td = tempfile.TemporaryDirectory(prefix="watchdog-codex-")
        self.addCleanup(self._td.cleanup)
        self.calls: list[list[str]] = []
        self.unit_loaded = True
        def run(argv, **_kw):
            self.calls.append(list(argv))
            if argv[1:3] == ["--user", "cat"]:
                return subprocess.CompletedProcess(argv, 0 if self.unit_loaded else 1, "", "")
            return subprocess.CompletedProcess(argv, 0, "", "")
        for patch in (
            mock.patch.object(self.dog, "CODEX_SOCKET", Path(self._td.name) / "missing.sock"),
            mock.patch.object(self.dog.subprocess, "run", side_effect=run),
            mock.patch.object(self.dog.shutil, "which", return_value="/usr/bin/codex"),
            mock.patch.dict(os.environ, {"HOST_DAG_WATCH_NOTIFY": "0"}),
        ):
            patch.start()
            self.addCleanup(patch.stop)

    def test_missing_socket_starts_the_dedicated_unit(self) -> None:
        self.assertEqual(self.dog.ensure_codex_daemon({"state_path": str(Path(self._td.name) / "w.json")}), "started")
        self.assertIn(["systemctl", "--user", "start", "codex-app-server.service"], self.calls)
        self.assertFalse(any(c[:3] == ["/usr/bin/codex", "app-server", "daemon"] for c in self.calls))

    def test_without_the_unit_falls_back_to_daemon_start(self) -> None:
        self.unit_loaded = False
        self.dog.ensure_codex_daemon({"state_path": str(Path(self._td.name) / "w.json")})
        self.assertIn(["/usr/bin/codex", "app-server", "daemon", "start"], self.calls)


class MemoryAlertTest(unittest.TestCase):
    """2026-10-09: alert before systemd-oomd kills a user cgroup; never pause dispatch."""

    MEMINFO = "MemTotal:       64000000 kB\nMemAvailable:    {avail} kB\nSwapFree:         1000 kB\n"
    PRESSURE = "some avg10={p10} avg60={p60} avg300=0.50 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"

    def setUp(self) -> None:
        import importlib.util
        spec = importlib.util.spec_from_file_location("watchdog_mem", SCRIPTS / "host-dag-watchdog.py")
        assert spec and spec.loader
        self.dog = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.dog)
        self._td = tempfile.TemporaryDirectory(prefix="memory-")
        self.addCleanup(self._td.cleanup)
        self.state = Path(self._td.name) / "watch.json"
        self.alerts = {"state_path": str(self.state), "alerts_sent": {}}
        self.memo: dict = {}
        from unittest import mock
        patcher = mock.patch.dict(os.environ, {"HOST_DAG_WATCH_NOTIFY": "0"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def status(self, avail_gib: float, p10: float, p60: float = 1.0) -> dict:
        return self.dog.memory_status(self.MEMINFO.format(avail=int(avail_gib * 2**20)),
                                      self.PRESSURE.format(p10=f"{p10:.2f}", p60=f"{p60:.2f}"))

    def swap(self, total_gib: float, free_gib: float) -> dict:
        return {**self.status(20, 0), "swap_total_gib": total_gib, "swap_free_gib": free_gib}

    def logged(self) -> list[dict]:
        log = self.state.parent / "alerts.jsonl"
        return [json.loads(x) for x in log.read_text().splitlines()] if log.exists() else []

    def test_parses_available_swap_and_pressure(self) -> None:
        self.assertEqual(self.status(3.5, 12.5), {"available_gib": 3.5, "swap_free_gib": 0.0, "swap_total_gib": 0.0,
                                                  "pressure_avg10": 12.5, "pressure_avg60": 1.0})

    def test_healthy_memory_is_silent(self) -> None:
        self.assertIsNone(self.dog.check_memory(self.alerts, self.memo, self.status(20, 0), NOW, []))
        self.assertEqual(self.logged(), [])

    def test_alerts_once_on_entry_then_hourly_then_records_recovery(self) -> None:
        low = self.status(3.0, 0)
        self.assertIn("可用内存 3.0G", self.dog.check_memory(self.alerts, self.memo, low, NOW, ["13.4G vm"]))
        self.dog.check_memory(self.alerts, self.memo, low, NOW + 60, [])
        self.assertEqual([a["key"] for a in self.logged()], ["memory"])
        self.assertIn("13.4G vm", self.logged()[0]["body"])
        self.assertIn("只告警", self.logged()[0]["body"])
        self.dog.check_memory(self.alerts, self.memo, low, NOW + 3600, [])
        self.assertEqual([a["key"] for a in self.logged()], ["memory", "memory"])
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 0), NOW + 3700, [])
        self.assertEqual(self.logged()[-1]["key"], "memory-recovered")
        self.assertFalse(self.logged()[-1]["desktop"])
        self.assertEqual(self.memo["memory"], {})

    def test_single_avg10_spike_is_silent_but_two_in_a_row_alert(self) -> None:
        # 2026-11-02: a lone avg10 59% while swap refilled raised a desktop alert with 35G available.
        self.assertIsNone(self.dog.check_memory(self.alerts, self.memo, self.status(20, 45), NOW, []))
        self.assertIsNone(self.dog.check_memory(self.alerts, self.memo, self.status(20, 5), NOW + 60, []))
        self.assertIsNone(self.dog.check_memory(self.alerts, self.memo, self.status(20, 45), NOW + 120, []))
        reason = self.dog.check_memory(self.alerts, self.memo, self.status(20, 45), NOW + 180, [])
        self.assertIn("连续两次", reason)
        self.assertEqual([a["key"] for a in self.logged()], ["memory"])

    def test_sustained_avg60_alerts_at_once(self) -> None:
        self.assertIn("avg60 30.0%", self.dog.check_memory(self.alerts, self.memo, self.status(20, 10, 30), NOW, []))

    def test_oomd_pressure_threshold_alerts_critical_on_first_check(self) -> None:
        self.assertIn("avg10 65.0%", self.dog.check_memory(self.alerts, self.memo, self.status(20, 65), NOW, []))
        self.assertEqual([a["key"] for a in self.logged()], ["memory-critical"])

    def test_full_swap_is_logged_not_shown(self) -> None:
        # 2026-11-02 14:58: 21G of 23G swap held cold pages (baobab), 43G available, pressure 0;
        # oomd monitors no cgroup for swap here, so "OOM" was a false alarm.
        from unittest import mock
        with mock.patch.object(self.dog, "alert") as sent:
            self.assertIn("swap 已用 91%", self.dog.check_memory(self.alerts, self.memo, self.swap(23.3, 2.1), NOW, []))
        self.assertEqual(sent.call_args.args[1], "memory-swap-low")
        self.assertIs(sent.call_args.kwargs["desktop"], False)

    def test_low_swap_is_logged_only(self) -> None:
        from unittest import mock
        with mock.patch.object(self.dog, "alert") as sent:
            self.assertIn("剩余 0.3G", self.dog.check_memory(self.alerts, self.memo, self.swap(2.5, 0.3), NOW, []))
        self.assertEqual(sent.call_args.args[1], "memory-swap-low")
        self.assertIs(sent.call_args.kwargs["desktop"], False)

    def test_escalation_alerts_immediately_and_same_state_waits_30_minutes(self) -> None:
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 10, 30), NOW, [])
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 10, 30), NOW + 600, [])
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 70, 30), NOW + 660, [])
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 70, 30), NOW + 1200, [])
        self.assertEqual([a["key"] for a in self.logged()], ["memory", "memory-critical"])
        self.dog.check_memory(self.alerts, self.memo, self.status(20, 70, 30), NOW + 660 + 1800, [])
        self.assertEqual([a["key"] for a in self.logged()], ["memory", "memory-critical", "memory-critical"])


class TelegramAlertTest(unittest.TestCase):
    """Desktop alerts are mirrored to Telegram when a config is present."""

    def setUp(self) -> None:
        import http.server
        import threading
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.received: list[tuple[str, dict]] = []
        received = self.received

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received.append((self.path, json.loads(body)))
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"ok":true}')

            def log_message(self, *args) -> None:
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.config = self.base / "telegram.env"
        self.config.write_text(
            "# test\nTELEGRAM_BOT_TOKEN=123:abc\nTELEGRAM_CHAT_ID='42'\n"
            f"TELEGRAM_API=http://127.0.0.1:{self.server.server_port}\n", encoding="utf-8")
        self.saved = {k: os.environ.get(k) for k in ("HOST_DAG_TELEGRAM_CONFIG", "HOST_DAG_WATCH_NOTIFY")}
        os.environ["HOST_DAG_TELEGRAM_CONFIG"] = str(self.config)
        os.environ["HOST_DAG_WATCH_NOTIFY"] = "1"
        self.run_patch = heal.subprocess.run
        heal.subprocess.run = lambda *a, **k: None  # no real notify-send

    def tearDown(self) -> None:
        heal.subprocess.run = self.run_patch
        for key, value in self.saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.server.shutdown()
        self.server.server_close()
        self.tmp.cleanup()

    def state(self) -> dict:
        names = ["KV17-03", "T17-01", "KV18-01", "KV17-06", "KV9-09"]
        (self.base / "run.json").write_text(json.dumps({"batches": {"TEST": {"tickets": {
            name: {"state": "ACCEPTED"} for name in names}}}}))
        return {"state_path": str(self.base / "watch.json"), "alerts_sent": {}}

    def test_desktop_alert_is_pushed_once_per_hour(self) -> None:
        state = self.state()
        self.assertTrue(heal.alert(state, "accepted:KV17-03:ACCEPTED", "KV17-03 已验收", "说明", NOW))
        self.assertFalse(heal.alert(state, "accepted:KV17-03:ACCEPTED", "KV17-03 已验收", "说明", NOW + 60))
        self.assertEqual(len(self.received), 1)
        path, payload = self.received[0]
        self.assertEqual(path, "/bot123:abc/sendMessage")
        self.assertEqual(payload["chat_id"], "42")
        self.assertEqual(payload["text"], "KV17-03 已验收\n\n说明")

    def test_log_only_and_disabled_alerts_are_not_pushed(self) -> None:
        heal.alert(self.state(), "accepted:A-1:ACCEPTED", "A-1 已验收", "b", NOW, desktop=False)
        os.environ["HOST_DAG_WATCH_NOTIFY"] = "0"
        heal.alert(self.state(), "accepted:B-1:ACCEPTED", "B-1 已验收", "b", NOW)
        self.assertEqual(self.received, [])

    def test_missing_config_or_unreachable_api_never_raises(self) -> None:
        os.environ["HOST_DAG_TELEGRAM_CONFIG"] = str(self.base / "absent.env")
        self.assertFalse(heal.send_telegram("t", "b"))
        self.config.write_text("TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_CHAT_ID=1\nTELEGRAM_API=http://127.0.0.1:9\n",
                               encoding="utf-8")
        os.environ["HOST_DAG_TELEGRAM_CONFIG"] = str(self.config)
        self.assertFalse(heal.send_telegram("t", "b"))


class ForPeopleTest(unittest.TestCase):
    def test_notification_memory_uses_decimal_gb(self):
        self.assertEqual(heal.for_people("MemAvailable 20880524 KiB。"), "MemAvailable 21.38 GB。")
        self.assertEqual(heal.for_people("峰值 4.6 GiB，剩余 512 MiB"), "峰值 4.94 GB，剩余 536.87 MB")

    def test_paths_and_their_lead_ins_are_dropped(self) -> None:
        cases = {
            "安装尚未开始；host-a 无新告警。详情 /run/media/x/KV18-01/round-1/report.md":
                "安装尚未开始；host-a 无新告警。",
            "未验收/解锁。/run/media/a/b/report.md": "未验收/解锁。",
            "证据在 $RUN/evidence/T-05/round-6/report.md。": "证据。",
            "请求：control/kv1705-r7-window-request.json 已写": "请求：control/kv1705-r7-window-request.json 已写",
            "ACCEPTED_SCOPED；整票以票目录顶层为准\n/run/media/x/v18/KV18-01/round-3":
                "ACCEPTED_SCOPED；整票以票目录顶层为准",
            "原round1/report.md及receipt.json已写入": "原round1/report.md及receipt.json已写入",
        }
        for raw, want in cases.items():
            self.assertEqual(heal.for_people(raw), want, raw)


class QuietHoursTest(TelegramAlertTest):
    """22:00–08:00 alerts are held and sent as one digest after 08:00."""

    def at(self, hour: int) -> float:
        return time.mktime((2026, 10, 14, hour, 30, 0, 0, 0, -1))

    def test_night_alert_is_held_then_flushed_in_the_morning(self) -> None:
        heal.alert(self.state(), "coordinator:KV17-03-accepted", "KV17-03 已验收", "夜里的事", self.at(23))
        self.assertEqual(self.received, [])
        self.assertEqual(heal.flush_deferred(self.base, self.at(3)), 0)
        self.assertEqual(heal.flush_deferred(self.base, self.at(8)), 1)
        self.assertIn("补发的通知", self.received[0][1]["text"])
        self.assertIn("KV17-03 已验收", self.received[0][1]["text"])
        self.assertFalse((self.base / "advisor" / "deferred.jsonl").exists())

    def test_quiet_hour_bounds(self) -> None:
        self.assertTrue(heal.is_quiet_hours(self.at(22)))
        self.assertTrue(heal.is_quiet_hours(self.at(7)))
        self.assertFalse(heal.is_quiet_hours(self.at(8)))
        self.assertFalse(heal.is_quiet_hours(self.at(21)))


class TelegramFilterTest(TelegramAlertTest):
    """Only final acceptances and user questions reach Telegram, once per ticket."""

    def test_round_noise_stays_off_telegram(self) -> None:
        for key, title in [("coordinator:KV17-02-round4-stop", "KV17-02 round4 已停笔"),
                           ("stage:T17-01:round-5:ACCEPTED", "T17-01 阶段验收（round-5）"),
                           ("advisor-fyi:CP-01-x", "已代决：CP-01 需决定"),
                           ("watcher", "Host DAG watcher 已自动重启")]:
            heal.alert(self.state(), key, title, "b", NOW)
        self.assertEqual(self.received, [])

    def test_ticket_acceptance_is_pushed_once_across_sources(self) -> None:
        heal.alert(self.state(), "accepted:T17-01:ACCEPTED", "T17-01 已验收", "b", NOW)
        heal.alert(self.state(), "coordinator:T17-01-accepted", "T17-01 已验收，无需操作", "b", NOW)
        heal.alert(self.state(), "advisor:T-01-sudo", "待你决定：T-01 需决定", "b", NOW)
        heal.alert(self.state(), "coordinator:V17-parent-accepted", "V17 已验收完成", "b", NOW)
        titles = [p["text"].split("\n")[0] for _, p in self.received]
        self.assertEqual(titles, ["T17-01 已验收", "待你决定：T-01 需决定"])

    def test_loosely_worded_coordinator_acceptances_and_questions_are_pushed(self) -> None:
        for key, title in [("coordinator:KV18-01-accepted-scoped", "KV18-01 限定验收完成"),
                           ("coordinator:KV17-06-accepted-reduced", "KV17-06 已限定验收，剩余项转07"),
                           ("coordinator:KV18-01-iso-not-shared", "KV18-01 需你决定（A级）：ISO 不能挂"),
                           ("coordinator:T-05-round8-stop", "T-05 round8 验收门槛未满足"),
                           ("stage:KV18-01:round-3:ACCEPTED_SCOPED", "KV18-01 阶段验收（round-3）"),
                           ("advisor-fyi:KV18-01-x", "已代决：KV18-01 需决定")]:
            heal.alert(self.state(), key, title, "b", NOW)
        self.assertEqual([p["text"].split("\n")[0] for _, p in self.received],
                         ["KV18-01 限定验收完成", "KV17-06 已限定验收，剩余项转07"])

    def test_coordinator_urgent_reaches_telegram_only_when_it_asks_the_user(self) -> None:
        self.assertFalse(heal.telegram_worthy("coordinator:urgent:T17-INTEGRATE:s5", "S50 recovery required"))
        self.assertTrue(heal.telegram_worthy("coordinator:urgent:host-a", "host-a 宿主异常，需要你去机房"))
        self.assertTrue(heal.telegram_worthy("urgent:host-a-down", "host-a 宿主已 10 分钟无响应"))

    def test_urgent_bypasses_quiet_hours_and_result_is_pushed(self) -> None:
        night = time.mktime((2026, 10, 14, 23, 30, 0, 0, 0, -1))
        heal.alert(self.state(), "urgent:host-a-down", "host-a 无响应，需要去机房", "b", night)
        heal.alert(self.state(), "result:T2", "T2 结果", "b", NOW)
        self.assertEqual([p["text"].split("\n")[0] for _, p in self.received], ["host-a 无响应，需要去机房"])

    def test_coordinator_prefixed_result_and_failed_send_is_queued(self) -> None:
        heal.alert(self.state(), "coordinator:result:KVT2", "KVT2 整批结束", "b", NOW)
        self.assertEqual(self.received, [])
        self.config.write_text("TELEGRAM_BOT_TOKEN=1:x\nTELEGRAM_CHAT_ID=1\nTELEGRAM_API=http://127.0.0.1:9\n",
                               encoding="utf-8")
        heal.send_telegram = (lambda original: (lambda t, b: False))(heal.send_telegram)
        try:
            heal.alert(self.state(), "accepted:KV9-09:ACCEPTED", "KV9-09 已验收", "b", NOW)
        finally:
            import importlib
            importlib.reload(heal)
        self.assertTrue((self.base / "advisor" / "deferred.jsonl").exists())



class TelegramResultRegressionTest(unittest.TestCase):
    def test_real_executor_progress_cannot_bypass_filter(self):
        for key, title in [
            ("coordinator:result:KV18-02:fe-standard-built-1628", "KV18-02 FE标准压缩构建exit0"),
            ("coordinator:result:KV17-09:round2:slot", "KV17-09 round2 重槽忙，红测未开始"),
            ("coordinator:result:KV18-10:resume10-rootplan", "KV18-10 attempt4 root-plan 已绿，未验收")]:
            self.assertFalse(heal.telegram_worthy(key, title))

    def test_acceptance_requires_receipt_not_prefix_or_title(self):
        for key in ["accepted:KV18-02", "coordinator:KV18-02-accepted"]:
            for states in [None, {"V18/KV18-02": {"state": "RUNNING"}}]:
                self.assertFalse(heal.telegram_worthy(key, "KV18-02 已验收", states=states))
            self.assertTrue(heal.telegram_worthy(key, "KV18-02 已验收",
                            states={"V18/KV18-02": {"state": "ACCEPTED"}}))

    def test_old_unclassified_deferred_noise_is_archived(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            heal.defer_telegram(base, "KV18-10 已绿，未验收", "b", NOW)
            heal.defer_telegram(base, "重槽忙", "b", NOW, key="coordinator:result:slot")
            with patch.object(heal, "is_quiet_hours", return_value=False), patch.object(heal, "send_telegram") as send:
                self.assertEqual(heal.flush_deferred(base, NOW), 0)
                send.assert_not_called()
            self.assertEqual(len((base / "advisor/suppressed.jsonl").read_text().splitlines()), 2)

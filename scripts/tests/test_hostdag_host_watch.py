from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "host-dag-watch.py"
PY = sys.executable

FAKE_HOST = r"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(os.environ["HOST_DAG_FAKE_HOST_DIR"])
CALLS = ROOT / "calls.jsonl"
THREADS = ROOT / "threads"
SENT = ROOT / "sent.jsonl"
CONFIG = ROOT / "config.json"
MESSAGES = ROOT / "messages.json"


def load_json(path: Path, default):
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def emit(payload, code=0):
    raw = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    return code


def fail(code_name, message, details=None, exit_code=1):
    body = {"error": {"code": code_name, "message": message, "details": details or {}}}
    raw = (json.dumps(body, ensure_ascii=False) + "\n").encode("utf-8")
    sys.stdout.buffer.write(raw)
    sys.stdout.buffer.flush()
    return exit_code


def main(argv):
    CALLS.parent.mkdir(parents=True, exist_ok=True)
    with CALLS.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"argv": argv, "cwd": os.getcwd()}, ensure_ascii=False) + "\n")
    cfg = load_json(CONFIG, {})
    if cfg.get("unreachable"):
        return fail("RUNTIME_UNREACHABLE", "runtime down")
    sleep_s = float(cfg.get("read_sleep") or 0)
    if sleep_s > 0 and argv[:2] == ["thread", "read"]:
        time.sleep(sleep_s)
    if argv[:2] == ["goal", "pause"] and len(argv) == 3:
        goal = cfg.get("goal")
        if goal is None:
            return fail("INVALID_ARGUMENT", "goal unsupported")
        changed = goal.get("status") == "active"
        if changed:
            goal = {**goal, "status": "paused"}
            cfg["goal"] = goal
            write_json(CONFIG, cfg)
        return emit({"threadId": argv[2], "changed": changed, "goal": goal})
    if len(argv) < 3 or argv[0] != "thread":
        return fail("INVALID_ARGUMENT", "unsupported fake argv")
    action = argv[1]
    thread_id = argv[2]
    if action == "read":
        view = "result"
        cursor = None
        limit = 25
        i = 3
        while i < len(argv):
            if argv[i] == "--view" and i + 1 < len(argv):
                view = argv[i + 1]
                i += 2
                continue
            if argv[i] == "--cursor" and i + 1 < len(argv):
                cursor = argv[i + 1]
                i += 2
                continue
            if argv[i] == "--limit" and i + 1 < len(argv):
                limit = int(argv[i + 1])
                i += 2
                continue
            return fail("INVALID_ARGUMENT", "unknown read option")
        if view == "messages":
            pages = load_json(MESSAGES, {}).get(thread_id, {"messages": []})
            messages = list(pages.get("messages") or [])
            offset = int(cursor or 0)
            chunk = messages[offset:offset + limit]
            nxt = offset + limit if offset + limit < len(messages) else None
            return emit({
                "threadId": thread_id,
                "messages": chunk,
                "nextCursor": None if nxt is None else str(nxt),
            })
        path = THREADS / (thread_id + ".json")
        if not path.is_file():
            return fail("THREAD_NOT_FOUND", "missing thread")
        return emit(load_json(path, {}))
    if action == "send":
        message = ""
        i = 3
        while i < len(argv):
            if argv[i] == "--message" and i + 1 < len(argv):
                message = argv[i + 1]
                i += 2
                continue
            return fail("INVALID_ARGUMENT", "unknown send option")
        mode = cfg.get("send") or "ok"
        if mode == "busy":
            return fail("THREAD_BUSY", "turn active")
        if mode == "writer":
            return fail("NATIVE_RPC_ERROR", json.dumps({
                "code": -32600, "message": "thread " + thread_id + " already has an active writer"}))
        if mode == "timeout":
            time.sleep(float(cfg.get("send_sleep") or 60))
            return fail("INTERNAL_ERROR", "should have timed out")
        if mode == "crash":
            return 1
        if mode == "unreachable":
            return fail("RUNTIME_UNREACHABLE", "runtime down")
        box = load_json(MESSAGES, {})
        rec = box.setdefault(thread_id, {"messages": []})
        rec.setdefault("messages", []).append({"role": "user", "text": message})
        write_json(MESSAGES, box)
        with SENT.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"thread": thread_id, "message": message}, ensure_ascii=False) + "\n")
        return emit({
            "threadId": thread_id,
            "status": "running",
            "turn": {"turnId": "sent-turn", "status": "running"},
        })
    return fail("INVALID_ARGUMENT", "unsupported action")


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]) or 0)
"""


def require_tmpdir() -> Path:
    base = os.environ.get("TMPDIR")
    if not base:
        raise RuntimeError("TMPDIR must be set to an explicit test scratch directory")
    resolved = Path(base).resolve()
    text = str(resolved).replace("\\", "/").lower()
    # Native Ubuntu uses ~/.cache scratch, not the retired hd01-test work root.
    if text == "/tmp" or text.startswith("/tmp/"):
        raise RuntimeError(f"TMPDIR must not be /tmp: {resolved}")
    if "appdata/local/temp" in text:
        raise RuntimeError(f"TMPDIR must not be the Windows user temp directory: {resolved}")
    if len(text) >= 2 and text[1] == ":" and text[0] == "c":
        raise RuntimeError(f"TMPDIR must not be a C: temporary directory: {resolved}")
    return resolved


def load_watch_module():
    """A fresh in-process copy of the watcher for pure-function tests (no subprocess, no state file)."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(f"host_watch_{id(object())}", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HostWatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory(prefix="hostwatch-", dir=str(require_tmpdir()))
        self.base = Path(self._td.name)
        self.state = self.base / "watch-state.json"
        self.fake_dir = self.base / "fake-host"
        self.fake_dir.mkdir()
        (self.fake_dir / "threads").mkdir()
        self.fake = self.base / "fake_host.py"
        self.fake.write_text(FAKE_HOST.lstrip(), encoding="utf-8")
        self.host_cwd = self.base / "host-cwd"
        self.host_cwd.mkdir()
        self.env = os.environ.copy()
        self.env["HOST_DAG_FAKE_HOST_DIR"] = str(self.fake_dir)
        self.env["CODEXHOST_RUNTIME_TOKEN"] = "secret-token-value-test-only"
        # Fake alerts must not reach the user's desktop as real DAG notifications.
        self.env["HOST_DAG_WATCH_NOTIFY"] = "0"
        self.env["TMPDIR"] = str(self.base)
        self.env["TMP"] = str(self.base)
        self.env["TEMP"] = str(self.base)
        self.env["PYTHONUTF8"] = "0"
        self.env.pop("PYTHONIOENCODING", None)
        self.addCleanup(self._td.cleanup)

    def test_advisor_verdicts_accept_iso_and_numeric_timestamps(self) -> None:
        watch = load_watch_module()
        log = self.base / "advisor" / "log.jsonl"
        log.parent.mkdir()
        rows = [
            {"key": "epoch", "verdict": "answered", "at": 100},
            {"key": "numeric", "verdict": "answered", "at": "200.5"},
            {"key": "user", "verdict": "user_decided", "at": "2026-10-23T11:22:24+08:00"},
        ]
        log.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
        self.assertEqual(watch._advisor_verdicts(self.base), {
            "epoch": 100.0, "numeric": 200.5, "user": 1792725744.0,
        })

    def test_bad_advisor_timestamp_preserves_other_verdicts(self) -> None:
        watch = load_watch_module()
        log = self.base / "advisor" / "log.jsonl"
        log.parent.mkdir()
        rows = [
            {"key": "valid", "verdict": "answered", "at": 100},
            {"key": "valid", "verdict": "answered", "at": "invalid"},
            {"key": "bad", "verdict": "answered", "at": {}},
            {"key": "later", "verdict": "answered", "at": 300},
        ]
        log.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
        self.assertEqual(watch._advisor_verdicts(self.base), {"valid": 100.0, "later": 300.0})

    def run_watch(self, *args: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [PY, str(SCRIPT), "--state", str(self.state), *args],
            capture_output=True, text=True, encoding="utf-8", check=False, timeout=timeout,
            env=self.env, cwd=str(self.base),
        )

    def payload(self, result: subprocess.CompletedProcess[str]) -> dict:
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError:
            self.fail(f"exit={result.returncode} stdout={result.stdout!r} stderr={result.stderr!r}")
        if not isinstance(data, dict):
            self.fail(f"expected object: {data!r}")
        return data

    def write_thread(self, thread_id: str, status: str, text: str, turn_id: str = "turn-1") -> None:
        path = self.fake_dir / "threads" / (thread_id + ".json")
        path.write_text(json.dumps({
            "threadId": thread_id,
            "harnessId": "grok",
            "status": status,
            "turn": {"turnId": turn_id, "status": status},
            "result": {"availability": "available", "text": text},
            "nextCursor": "cursor-0",
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def write_config(self, **fields) -> None:
        path = self.fake_dir / "config.json"
        current = {}
        if path.is_file():
            current = json.loads(path.read_text(encoding="utf-8"))
        current.update(fields)
        path.write_text(json.dumps(current) + "\n", encoding="utf-8")

    def calls(self) -> list[dict]:
        path = self.fake_dir / "calls.jsonl"
        if not path.is_file():
            return []
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def sent(self) -> list[dict]:
        path = self.fake_dir / "sent.jsonl"
        if not path.is_file():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def load_state(self) -> dict:
        return json.loads(self.state.read_text(encoding="utf-8"))

    def init_watch(self, **kwargs) -> dict:
        parent = kwargs.get("parent", "parent-1")
        args = [
            "init",
            "--parent-thread", parent,
            "--host-cli", str(self.fake.resolve()),
            "--host-cwd", str(self.host_cwd.resolve()),
            "--poll-interval", str(kwargs.get("poll_interval", 30)),
            "--direction-interval", str(kwargs.get("direction_interval", 3600)),
            "--host-timeout", str(kwargs.get("host_timeout", 3)),
            "--event-limit", str(kwargs.get("event_limit", 8)),
        ]
        if "now" in kwargs:
            args = ["--now", str(kwargs["now"]), *args]
        if kwargs.get("run_receipt"):
            args.extend(["--run-receipt", str(kwargs["run_receipt"])])
        if kwargs.get("pause_goal"):
            args.append("--pause-parent-goal")
        if "settle_seconds" in kwargs:
            args.extend(["--settle-seconds", str(kwargs["settle_seconds"])])
        if "max_hold_seconds" in kwargs:
            args.extend(["--max-hold-seconds", str(kwargs["max_hold_seconds"])])
        result = self.run_watch(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return self.payload(result)

    def track(self, ticket: str, thread: str, evidence: str = "evidence/hd01.txt", now: float | None = None) -> dict:
        args = ["track", "--ticket", ticket, "--thread", thread, "--evidence", evidence]
        if now is not None:
            args = ["--now", str(now), *args]
        result = self.run_watch(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return self.payload(result)

    def poll(self, now: float | None = None, expect: int = 0) -> dict:
        args = ["poll"]
        if now is not None:
            args = ["--now", str(now), *args]
        result = self.run_watch(*args)
        self.assertEqual(result.returncode, expect, result.stderr + result.stdout)
        return self.payload(result)

    def test_help_lists_six_commands(self) -> None:
        result = subprocess.run(
            [PY, str(SCRIPT), "--help"], capture_output=True, text=True, encoding="utf-8",
            check=False, timeout=10, env=self.env,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        for name in ("init", "track", "untrack", "poll", "watch", "status"):
            self.assertIn(name, result.stdout)

    def test_idle_poll_does_not_call_host(self) -> None:
        self.init_watch()
        data = self.poll(now=10)
        self.assertEqual(data.get("called_host"), False)
        self.assertEqual(data.get("events"), [])
        self.assertEqual(self.calls(), [])
        status = self.payload(self.run_watch("status"))
        self.assertEqual(status.get("tracked"), [])
        self.assertEqual(self.calls(), [])

    def test_native_same_text_new_turn_is_a_new_completion(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "same result")
        path = self.fake_dir / "threads" / "child-1.json"
        payload = json.loads(path.read_text())
        payload["result"]["turnId"] = "native-turn-1"
        path.write_text(json.dumps(payload))
        self.track("NATIVE-01", "child-1", now=1)
        self.poll(now=2)
        self.poll(now=3)
        self.assertEqual(len(self.sent()), 1)
        payload["result"]["turnId"] = "native-turn-2"
        path.write_text(json.dumps(payload))
        self.poll(now=4)
        self.assertEqual(len(self.sent()), 2)

    def test_status_is_local_only_and_skips_lock_writer(self) -> None:
        self.init_watch()
        self.track("HD-01", "child-1", now=1)
        result = self.run_watch("status")
        self.assertEqual(result.returncode, 0, result.stderr)
        data = self.payload(result)
        self.assertEqual(data["parent_thread"], "parent-1")
        self.assertEqual(data["tracked"][0]["ticket"], "HD-01")
        self.assertIsNone(data.get("pending_send"))
        self.assertEqual(self.calls(), [])
        self.assertNotIn("secret-token-value-test-only", result.stdout)
        self.assertNotIn("CODEXHOST_RUNTIME_TOKEN", self.state.read_text(encoding="utf-8"))

    def test_completed_results_merge_into_capped_batch(self) -> None:
        self.init_watch(event_limit=2, now=5)
        self.write_thread("parent-1", "running", "parent busy")
        self.write_thread("c1", "completed", "one done")
        self.write_thread("c2", "completed", "two done")
        self.write_thread("c3", "completed", "three done")
        self.track("T1", "c1", now=5)
        self.track("T2", "c2", now=5)
        self.track("T3", "c3", now=5)
        data = self.poll(now=6)
        self.assertTrue(data.get("called_host"))
        self.assertEqual(len(data.get("events") or []), 2)
        self.assertTrue(data.get("truncated"))
        self.assertEqual(data.get("send"), "held_busy")
        self.assertEqual(self.sent(), [])
        self.assertEqual(len(self.load_state().get("pending_events") or []), 3)
        kinds = [item["kind"] for item in data["events"]]
        self.assertEqual(kinds, ["completed", "completed"])

    def test_parent_busy_holds_events_without_send(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "still working")
        self.write_thread("child-1", "completed", "Needs Decision: which fixture?")
        self.track("HD-01", "child-1", now=20)
        data = self.poll(now=21)
        self.assertEqual(data.get("send"), "held_busy")
        self.assertEqual(len(data.get("events") or []), 1)
        self.assertEqual(data["events"][0]["kind"], "completed")
        self.assertEqual(self.sent(), [])
        again = self.poll(now=22)
        self.assertEqual(again.get("send"), "held_busy")
        self.assertEqual(again.get("events"), [])
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)
        self.assertEqual(self.sent(), [])

    def test_duplicate_completion_and_turn_id_backfill_do_not_reawake(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "same result body", turn_id="turn-a")
        self.track("HD-01", "child-1", now=30)
        first = self.poll(now=31)
        self.assertEqual(len(first.get("events") or []), 1)
        self.write_thread("child-1", "completed", "same result body", turn_id="turn-b")
        second = self.poll(now=32)
        self.assertEqual(second.get("events"), [])
        self.write_thread("child-1", "completed", "same result body", turn_id="turn-c")
        third = self.poll(now=33)
        self.assertEqual(third.get("events"), [])
        self.write_thread("child-1", "completed", "new actual result", turn_id="turn-c")
        fourth = self.poll(now=34)
        self.assertEqual(len(fourth.get("events") or []), 1)
        self.assertIn("new actual result", fourth["events"][0]["summary"])

    def test_direction_check_only_for_active_after_interval(self) -> None:
        self.init_watch(direction_interval=3600)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "still running", turn_id="t-run")
        self.track("HD-01", "child-1", now=100)
        early = self.poll(now=100)
        self.assertEqual(early.get("events"), [])
        due = self.poll(now=3700)
        self.assertEqual(len(due.get("events") or []), 1)
        self.assertEqual(due["events"][0]["kind"], "direction")
        again = self.poll(now=3701)
        self.assertEqual(again.get("events"), [])
        self.write_thread("child-1", "completed", "finished", turn_id="t-done")
        done = self.poll(now=8000)
        kinds = [item["kind"] for item in done.get("events") or []]
        self.assertEqual(kinds, ["completed"])
        later = self.poll(now=20000)
        self.assertEqual(later.get("events"), [])

    def test_parent_idle_send_uses_unique_marker(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle parent")
        self.write_thread("child-1", "completed", "worker finished this turn")
        self.track("HD-01", "child-1", now=50)
        data = self.poll(now=51)
        self.assertEqual(data.get("send"), "delivered")
        sent = self.sent()
        self.assertEqual(len(sent), 1)
        message = sent[0]["message"]
        self.assertIn("HOST-DAG-WATCH-MARKER:", message)
        self.assertIn("Do not create a new Goal", message)
        self.assertIn("HD-01", message)
        self.assertIn("evidence/hd01.txt", message)
        self.assertNotIn("secret-token-value-test-only", message)
        self.assertLessEqual(len(message), 4000)
        self.assertEqual(self.load_state().get("pending_events"), [])

    def test_thread_busy_is_zero_commit_and_retryable(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=60)
        self.write_config(send="busy")
        first = self.poll(now=61)
        self.assertEqual(first.get("send"), "thread_busy")
        self.assertEqual(self.sent(), [])
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)
        self.write_config(send="ok")
        second = self.poll(now=62)
        self.assertEqual(second.get("send"), "delivered")
        self.assertEqual(len(self.sent()), 1)

    def alerts(self) -> list[dict]:
        path = self.state.parent / "alerts.jsonl"
        if not path.is_file():
            return []
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]

    def test_other_writer_keeps_marker_and_alerts_by_cause(self) -> None:
        self.env["HOST_DAG_WATCH_NOTIFY"] = "0"
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=60)
        self.write_config(send="writer")
        first = self.poll(now=61)
        self.assertEqual(first.get("send"), "writer_conflict")
        pending = self.load_state()["pending_send"]
        self.assertEqual(pending["status"], "busy")
        marker = pending["marker"]
        second = self.poll(now=91)
        self.assertEqual(second.get("send"), "writer_conflict")
        self.assertEqual(self.load_state()["pending_send"]["marker"], marker)
        self.assertEqual(self.sent(), [])
        keys = {a["key"] for a in self.alerts()}
        self.assertEqual(keys, {"writer-conflict:parent-1"})
        self.assertIn("其他客户端", self.alerts()[0]["body"])
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)
        self.write_config(send="ok")
        third = self.poll(now=121)
        self.assertEqual(third.get("send"), "delivered")
        self.assertEqual(len(self.sent()), 1)
        self.assertIn(marker, self.sent()[0]["message"])

    def test_uncertain_alert_key_is_per_parent(self) -> None:
        self.env["HOST_DAG_WATCH_NOTIFY"] = "0"
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=160)
        self.write_config(send="crash")
        self.assertEqual(self.poll(now=161).get("send"), "uncertain")
        self.assertEqual({a["key"] for a in self.alerts()}, {"uncertain:parent-1"})

    def test_uncertain_send_stops_resend_when_marker_absent(self) -> None:
        self.init_watch(host_timeout=1)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=70)
        self.write_config(send="timeout", send_sleep=5)
        first = self.poll(now=71, expect=0)
        self.assertEqual(first.get("send"), "uncertain")
        self.assertEqual(self.sent(), [])
        self.assertEqual(self.load_state()["pending_send"]["status"], "uncertain")
        self.write_config(send="ok")
        second = self.poll(now=72)
        self.assertEqual(second.get("send"), "uncertain")
        self.assertEqual(self.sent(), [])

    def test_timeout_confirms_delivery_when_marker_present(self) -> None:
        self.init_watch(host_timeout=1)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=80)
        held = self.poll(now=81)
        self.assertEqual(held.get("send"), "held_busy")
        marker = "HOST-DAG-WATCH-MARKER:preloaded"
        (self.fake_dir / "messages.json").write_text(json.dumps({
            "parent-1": {"messages": [{"role": "user", "text": "noise"}, {"role": "user", "text": marker + " body"}]},
        }), encoding="utf-8")
        state = self.load_state()
        state["pending_send"] = {
            "marker": marker, "status": "intent", "message": marker + " body",
            "event_n": 1, "saved_at": 80,
        }
        self.state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        self.write_thread("parent-1", "completed", "idle")
        data = self.poll(now=82)
        self.assertEqual(data.get("send"), "delivered")
        self.assertTrue(data.get("recovered"))
        self.assertEqual(self.sent(), [])
        self.assertEqual(self.load_state().get("pending_events"), [])

    def test_writer_mutex_rejects_second_poll(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work")
        self.track("HD-01", "child-1", now=90)
        self.write_config(read_sleep=3)
        watch = subprocess.Popen(
            [PY, str(SCRIPT), "--state", str(self.state), "watch", "--max-cycles", "1"],
            cwd=str(self.base), env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
        )
        try:
            deadline = time.time() + 8
            busy = None
            while time.time() < deadline:
                if (self.fake_dir / "calls.jsonl").is_file():
                    break
                time.sleep(0.05)
            result = self.run_watch("--now", "91", "poll")
            busy = self.payload(result)
            self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
            self.assertEqual(busy.get("error"), "busy")
        finally:
            try:
                watch.communicate(timeout=12)
            except subprocess.TimeoutExpired:
                watch.kill()
                watch.communicate(timeout=5)

    def test_host_disconnect_records_reason_and_does_not_start_another_host(self) -> None:
        self.init_watch()
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=95)
        self.write_config(unreachable=True)
        data = self.poll(now=96, expect=3)
        self.assertEqual(data.get("error"), "RUNTIME_UNREACHABLE")
        self.assertIn("runtime", (data.get("message") or "").lower())
        self.assertNotIn("secret-token-value-test-only", data.get("message") or "")
        invoked = [item["argv"] for item in self.calls()]
        self.assertTrue(invoked)
        self.assertTrue(all(row[0] == "thread" for row in invoked))
        self.assertTrue(all("delegate" not in row for row in invoked))
        err = self.load_state().get("last_host_error") or {}
        self.assertEqual(err.get("code"), "RUNTIME_UNREACHABLE")

    def test_unknown_status_is_not_success(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "mystery", "not a known status")
        self.track("HD-01", "child-1", now=110)
        data = self.poll(now=111)
        self.assertEqual(len(data.get("events") or []), 1)
        self.assertEqual(data["events"][0]["kind"], "unknown_status")
        self.assertIn("coordinator", data["events"][0]["summary"])
        self.assertNotEqual(data["events"][0].get("kind"), "completed")
        self.assertFalse(self.load_state()["tracked"]["HD-01"].get("terminal"))

    def test_host_uses_argv_fixed_cwd_and_timeout_without_shell(self) -> None:
        self.init_watch(host_timeout=3)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=120)
        self.poll(now=121)
        rows = self.calls()
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(Path(row["cwd"]).resolve(), self.host_cwd.resolve())
            self.assertIsInstance(row["argv"], list)
            self.assertEqual(row["argv"][0], "thread")
            self.assertNotIn("&&", " ".join(row["argv"]))
        self.assertTrue(any(row["argv"][:3] == ["thread", "read", "child-1"] for row in rows))
        self.assertTrue(any(row["argv"][:3] == ["thread", "send", "parent-1"] for row in rows))

    def test_watch_stops_on_stop_file(self) -> None:
        self.init_watch(poll_interval=0)
        stop = self.base / "stop"
        stop.write_text("1", encoding="utf-8")
        result = self.run_watch("watch", "--poll-interval", "0", "--stop-file", str(stop))
        self.assertEqual(result.returncode, 0, result.stderr)
        data = self.payload(result)
        self.assertEqual(data.get("cycles"), 1)
        self.assertEqual(data.get("stopped"), "stop-file")
        self.assertEqual(self.calls(), [])

    def test_failed_status_keeps_result_text(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "failed", "boom details from worker")
        self.track("HD-01", "child-1", now=130)
        data = self.poll(now=131)
        self.assertEqual(data["events"][0]["kind"], "failed")
        self.assertIn("boom details from worker", data["events"][0]["summary"])
        self.assertTrue(self.load_state()["tracked"]["HD-01"].get("terminal"))

    def test_recovered_send_keeps_later_events(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "first result")
        self.track("HD-01", "child-1", now=140)
        held = self.poll(now=141)
        self.assertEqual(held.get("send"), "held_busy")
        marker = "HOST-DAG-WATCH-MARKER:keep-later"
        (self.fake_dir / "messages.json").write_text(json.dumps({
            "parent-1": {"messages": [{"role": "user", "text": marker}]},
        }), encoding="utf-8")
        state = self.load_state()
        state["pending_send"] = {
            "marker": marker, "status": "intent", "message": marker,
            "event_n": 1, "saved_at": 141,
        }
        self.state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        self.write_thread("child-1", "completed", "second result")
        self.write_thread("parent-1", "completed", "idle")
        data = self.poll(now=142)
        self.assertEqual(data.get("send"), "delivered")
        self.assertTrue(data.get("recovered"))
        self.assertEqual(len(self.sent()), 1)
        self.assertIn("second result", self.sent()[0]["message"])
        self.assertNotIn("first result", self.sent()[0]["message"])

    def test_track_mutex_with_watcher(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work")
        self.track("HD-01", "child-1", now=150)
        self.write_config(read_sleep=3)
        watch = subprocess.Popen(
            [PY, str(SCRIPT), "--state", str(self.state), "watch", "--max-cycles", "1"],
            cwd=str(self.base), env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
        )
        try:
            deadline = time.time() + 8
            while time.time() < deadline:
                if (self.fake_dir / "calls.jsonl").is_file():
                    break
                time.sleep(0.05)
            result = self.run_watch("track", "--ticket", "HD-02", "--thread", "child-2")
            busy = self.payload(result)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(busy["registration"]["status"], "queued")
            self.assertTrue(Path(busy["request"]).exists())
        finally:
            try:
                watch.communicate(timeout=12)
            except subprocess.TimeoutExpired:
                watch.kill()
                watch.communicate(timeout=5)

        self.write_config(read_sleep=0)
        self.write_thread("child-2", "completed", "recovered result")
        self.poll(now=200)
        self.assertIn("HD-02", self.load_state()["tracked"])
        self.assertEqual(json.loads(Path(busy["request"]).with_suffix(".ack").read_text())["status"], "applied")

    def test_send_crash_recovers_via_marker_search(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=160)
        self.write_config(send="crash")
        data = self.poll(now=161)
        self.assertEqual(data.get("send"), "uncertain")
        self.assertNotEqual(data.get("send"), "unknown_parent")
        self.assertEqual(self.sent(), [])
        self.assertEqual(self.load_state()["pending_send"]["status"], "uncertain")

    def test_disconnect_during_recover_keeps_intent(self) -> None:
        self.init_watch()
        self.track("HD-01", "child-1", now=170)
        result = self.run_watch("untrack", "--ticket", "HD-01")
        self.assertEqual(result.returncode, 0, result.stderr)
        state = self.load_state()
        state["pending_events"] = [{
            "kind": "completed", "ticket": "HD-01", "thread": "child-1",
            "status": "completed", "summary": "done", "evidence": "evidence/hd01.txt",
        }]
        state["pending_send"] = {
            "marker": "HOST-DAG-WATCH-MARKER:rebind",
            "status": "intent",
            "message": "HOST-DAG-WATCH-MARKER:rebind",
            "event_n": 1,
            "saved_at": 170,
        }
        self.state.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        self.write_config(unreachable=True)
        data = self.poll(now=171, expect=3)
        self.assertEqual(data.get("error"), "RUNTIME_UNREACHABLE")
        self.assertEqual(self.load_state()["pending_send"]["status"], "intent")

    def test_packet_char_cap_is_hard(self) -> None:
        self.init_watch(event_limit=1)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "x" * 8000)
        self.track("HD-01", "child-1", evidence="e" * 8000, now=180)
        data = self.poll(now=181)
        self.assertEqual(data.get("send"), "delivered")
        self.assertLessEqual(len(self.sent()[0]["message"]), 4000)

    def test_utf8_host_json_decodes_when_pythonutf8_disabled(self) -> None:
        chinese = "开工先核对 HD-02 隔离 worktree、Ticket Base 和操作说明规格，再按实际命令边界写文档并核验。老"
        self.assertIn(b"\x81", chinese.encode("utf-8"))
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", chinese)
        self.track("HD-02", "child-1", now=200)
        data = self.poll(now=201)
        self.assertNotEqual(data.get("error"), "HOST_ERROR", data)
        self.assertEqual(len(data.get("events") or []), 1)
        self.assertEqual(data["events"][0]["kind"], "completed")
        self.assertIn("开工先核对", data["events"][0]["summary"])
        self.assertIn("老", data["events"][0]["summary"])
        self.assertEqual(data.get("send"), "held_busy")

    def test_utf8_event_send_roundtrip_when_pythonutf8_disabled(self) -> None:
        chinese = "开工先核对 HD-02 隔离 worktree、Ticket Base 和操作说明规格，再按实际命令边界写文档并核验。老"
        self.init_watch()
        self.write_thread("parent-1", "completed", "idle parent")
        self.write_thread("child-1", "completed", chinese)
        self.track("HD-02", "child-1", now=210)
        data = self.poll(now=211)
        self.assertEqual(data.get("send"), "delivered")
        sent = self.sent()
        self.assertEqual(len(sent), 1)
        self.assertIn("开工先核对", sent[0]["message"])
        self.assertIn("老", sent[0]["message"])
        self.assertIn("HD-02", sent[0]["message"])

    def test_untrack_is_observation_only(self) -> None:
        self.init_watch()
        self.track("HD-01", "child-1", now=1)
        result = self.run_watch("untrack", "--ticket", "HD-01")
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.payload(self.run_watch("status"))
        self.assertEqual(status.get("tracked"), [])
        self.assertEqual(self.calls(), [])

    def test_untrack_keeps_queued_events(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=190)
        self.poll(now=191)
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)
        result = self.run_watch("untrack", "--ticket", "HD-01")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)

    def goal_calls(self) -> list[list[str]]:
        return [c["argv"] for c in self.calls() if c["argv"][:1] == ["goal"]]

    def test_pause_parent_goal_once_then_before_each_wake(self) -> None:
        self.init_watch(pause_goal=True)
        self.write_config(goal={"threadId": "parent-1", "status": "active"})
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work", turn_id="t-run")
        self.track("HD-01", "child-1", now=1)
        self.poll(now=2)
        self.poll(now=3)
        self.assertEqual(self.goal_calls(), [["goal", "pause", "parent-1"]])
        self.assertEqual(self.load_state()["parent_goal"]["status"], "paused")
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done", turn_id="t-done")
        self.assertEqual(self.poll(now=4).get("send"), "delivered")
        argv = [c["argv"][:2] for c in self.calls()]
        self.assertLess(len(argv) - 1 - argv[::-1].index(["goal", "pause"]), argv.index(["thread", "send"]))

    def test_goal_pause_failure_never_blocks_delivery(self) -> None:
        self.init_watch(pause_goal=True)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=1)
        self.assertEqual(self.poll(now=2).get("send"), "delivered")
        self.assertEqual(self.load_state()["parent_goal"]["error"], "INVALID_ARGUMENT")

    def test_idle_poll_retries_failed_goal_pause_with_backoff(self) -> None:
        self.init_watch(pause_goal=True)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work", turn_id="t-run")
        self.track("HD-01", "child-1", now=1)
        self.poll(now=2)
        self.assertEqual(self.load_state()["parent_goal"]["error"], "INVALID_ARGUMENT")
        self.write_config(goal={"threadId": "parent-1", "status": "active"})
        self.poll(now=30)
        self.assertEqual(len(self.goal_calls()), 1)
        self.poll(now=63)
        self.assertEqual(len(self.goal_calls()), 2)
        self.assertEqual(self.load_state()["parent_goal"]["status"], "paused")
        self.poll(now=10_000)
        self.assertEqual(len(self.goal_calls()), 2)

    def test_goal_is_untouched_without_opt_in(self) -> None:
        self.init_watch()
        self.write_config(goal={"threadId": "parent-1", "status": "active"})
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=1)
        self.poll(now=2)
        self.assertEqual(self.goal_calls(), [])

    def test_busy_backlog_keeps_one_wake_worthy_event_per_fact(self) -> None:
        self.init_watch(direction_interval=10)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work", turn_id="t-run")
        self.track("HD-01", "child-1", now=100)
        for now in (100, 200, 300):
            self.poll(now=now)
        kinds = [e["kind"] for e in self.load_state()["pending_events"]]
        self.assertEqual(kinds, ["direction"])
        self.write_thread("child-1", "completed", "done", turn_id="t-done")
        self.poll(now=400)
        kinds = [e["kind"] for e in self.load_state()["pending_events"]]
        self.assertEqual(kinds, ["completed"])

    def test_settle_window_merges_paired_reviews_into_one_wake(self) -> None:
        self.init_watch(settle_seconds=120, now=0)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("std", "running", "reviewing", turn_id="s0")
        self.write_thread("spec", "running", "reviewing", turn_id="p0")
        self.track("R-review-standards", "std", now=0)
        self.track("R-review-spec", "spec", now=0)
        self.write_thread("std", "completed", "Standards PASS", turn_id="s1")
        self.assertEqual(self.poll(now=100).get("send"), "settling")
        self.write_thread("spec", "completed", "Spec PASS", turn_id="p1")
        self.assertEqual(self.poll(now=160).get("send"), "settling")
        self.assertEqual(self.sent(), [])
        self.assertEqual(self.poll(now=280).get("send"), "delivered")
        sent = self.sent()
        self.assertEqual(len(sent), 1)
        self.assertIn("Standards PASS", sent[0]["message"])
        self.assertIn("Spec PASS", sent[0]["message"])

    def test_review_axis_waits_for_its_sibling_past_the_quiet_window(self) -> None:
        self.init_watch(settle_seconds=120, now=0)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("std", "running", "reviewing", turn_id="s0")
        self.write_thread("spec", "running", "reviewing", turn_id="p0")
        self.write_thread("other", "running", "working", turn_id="o0")
        self.track("V-01-review-standards", "std", now=0)
        self.track("V-01-review-spec", "spec", now=0)
        self.track("V-02-review-spec", "other", now=0)
        self.write_thread("std", "completed", "Standards PASS", turn_id="s1")
        self.poll(now=10)
        # 7 minutes later Spec is still running: an unrelated review group doesn't matter.
        self.assertEqual(self.poll(now=430).get("send"), "settling")
        self.write_thread("spec", "completed", "Spec PASS", turn_id="p1")
        self.assertEqual(self.poll(now=440).get("send"), "settling")
        self.assertEqual(self.poll(now=560).get("send"), "delivered")
        sent = self.sent()
        self.assertEqual(len(sent), 1)
        self.assertIn("Standards PASS", sent[0]["message"])
        self.assertIn("Spec PASS", sent[0]["message"])

    def test_review_axis_hold_is_capped_and_skipped_for_failures(self) -> None:
        self.init_watch(settle_seconds=120, now=0)
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("spec", "running", "reviewing", turn_id="p0")
        self.track("V-01-review-spec", "spec", now=0)
        self.write_thread("std", "completed", "Standards PASS", turn_id="s1")
        self.track("V-01-review-standards", "std", now=0)
        self.poll(now=10)
        self.assertEqual(self.poll(now=1700).get("send"), "settling")
        self.assertEqual(self.poll(now=1811).get("send"), "delivered")
        self.write_thread("std2", "failed", "boom")
        self.track("V-02-review-standards", "std2", now=1811)
        self.write_thread("spec2", "running", "reviewing", turn_id="q0")
        self.track("V-02-review-spec", "spec2", now=1811)
        self.assertEqual(self.poll(now=1812).get("send"), "delivered")
        self.assertIn("boom", self.sent()[-1]["message"])

    def test_review_settle_is_capped_and_executor_results_and_failures_go_at_once(self) -> None:
        """2026-10-09: only review outcomes settle; an executor's result wakes the parent on the next poll."""
        self.init_watch(settle_seconds=120, max_hold_seconds=200, now=0)
        self.write_thread("parent-1", "completed", "idle")
        for n, turn in ((10, "a"), (110, "b"), (190, "c")):
            self.write_thread("rv", "completed", "review " + turn, turn_id=turn)
            if n == 10:
                self.track("HD-01-review-spec", "rv", now=0)
            self.assertEqual(self.poll(now=n).get("send"), "settling")
        self.write_thread("rv", "completed", "review d", turn_id="d")
        self.assertEqual(self.poll(now=215).get("send"), "delivered")
        self.write_thread("child-1", "completed", "impl done")
        self.track("HD-03", "child-1", now=215)
        self.assertEqual(self.poll(now=216).get("send"), "delivered")
        self.assertIn("impl done", self.sent()[-1]["message"])
        self.write_thread("child-2", "failed", "boom")
        self.track("HD-02", "child-2", now=216)
        self.assertEqual(self.poll(now=217).get("send"), "delivered")
        self.assertIn("boom", self.sent()[-1]["message"])

    def test_min_wake_gap_holds_routine_events_and_lets_urgent_ones_through(self) -> None:
        """2026-10-20: at most one routine wake per gap; failures and decisions still go at once."""
        self.init_watch(now=0)
        state = self.load_state()
        state["min_wake_gap_seconds"] = 300
        self.state.write_text(json.dumps(state), encoding="utf-8")
        self.write_thread("parent-1", "completed", "idle")
        self.write_thread("child-1", "completed", "impl one")
        self.track("HD-01", "child-1", now=0)
        self.assertEqual(self.poll(now=1).get("send"), "delivered")
        self.write_thread("child-2", "completed", "impl two")
        self.track("HD-02", "child-2", now=1)
        self.assertEqual(self.poll(now=60).get("send"), "settling")
        # a routine attention notice does not break the gap either
        state = self.load_state()
        state.setdefault("pending_events", []).append(
            {"kind": "attention", "ticket": "HD-04", "status": "idle", "summary": "idle 20 min", "queued_at": 61})
        self.state.write_text(json.dumps(state), encoding="utf-8")
        self.assertEqual(self.poll(now=62).get("send"), "settling")
        self.assertEqual(self.poll(now=302).get("send"), "delivered")
        message = self.sent()[-1]["message"]
        self.assertIn("impl two", message)
        self.assertIn("idle 20 min", message)
        # a failure inside the next gap goes at once
        self.write_thread("child-3", "failed", "boom")
        self.track("HD-03", "child-3", now=302)
        self.assertEqual(self.poll(now=303).get("send"), "delivered")
        self.assertIn("boom", self.sent()[-1]["message"])
        # so does a non-routine attention (e.g. a finished page window)
        state = self.load_state()
        state.setdefault("pending_events", []).append(
            {"kind": "attention", "ticket": "HD-05", "status": "window-finished", "summary": "window done", "queued_at": 304})
        self.state.write_text(json.dumps(state), encoding="utf-8")
        self.assertEqual(self.poll(now=305).get("send"), "delivered")

    def test_min_wake_gap_edges(self) -> None:
        mod = load_watch_module()
        ev_idle = {"kind": "attention", "status": "idle", "ticket": "T", "queued_at": 0}
        ev_done = {"kind": "completed", "status": "completed", "ticket": "T", "queued_at": 0}
        # without a gap, idle/window-started stay urgent exactly as before
        self.assertTrue(mod.urgent(ev_idle, {}))
        self.assertFalse(mod.urgent(ev_idle, {"min_wake_gap_seconds": 300}))
        # a clock moved back does not hold
        state = {"min_wake_gap_seconds": 300, "last_delivered_at": 100_000}
        self.assertFalse(mod.settling(state, [ev_done], 10_000))
        self.assertTrue(mod.settling(state, [ev_done], 100_100))
        self.assertFalse(mod.settling(state, [ev_done], 100_300))
        # a delivery confirmed by recover_send starts the gap
        state = {"min_wake_gap_seconds": 300, "last_delivered_at": 1.0, "parent_thread": "p",
                 "pending_send": {"marker": "m", "status": "intent", "event_n": 1, "saved_at": 1000.0},
                 "pending_events": [ev_done, ev_idle]}
        mod.marker_in_parent_messages = lambda *a, **k: True
        state["last_delivered_at"] = time.time() + 86_400  # recorded before a clock rollback
        before = time.time()
        self.assertEqual(mod.recover_send(state, [])["send"], "delivered")
        self.assertGreaterEqual(state["last_delivered_at"], before)  # restarted at confirmation, not kept
        self.assertLess(state["last_delivered_at"], before + 60)
        self.assertTrue(mod.settling(state, state["pending_events"], time.time() + 1))

    def test_closed_ticket_in_receipt_stops_observation_and_drops_backlog(self) -> None:
        receipt = self.base / "run.json"
        receipt.write_text(json.dumps({"tickets": {"HD-01": {"state": "RUNNING"}}}), encoding="utf-8")
        self.init_watch(run_receipt=receipt, now=1)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=1)
        self.poll(now=2)
        self.assertEqual(len(self.load_state()["pending_events"]), 1)
        receipt.write_text(json.dumps({"tickets": {"HD-01": {"status": "ACCEPTED"}}}), encoding="utf-8")
        self.write_thread("parent-1", "completed", "idle")
        data = self.poll(now=3)
        state = self.load_state()
        self.assertEqual((state["pending_events"], state["tracked"]), ([], {}))
        self.assertEqual(self.sent(), [])
        self.assertIsNone(data.get("send"))

    def test_watch_stop_file_interrupts_wait(self) -> None:
        self.init_watch(poll_interval=30)
        stop = self.base / "stop-later"
        watch = subprocess.Popen(
            [PY, str(SCRIPT), "--state", str(self.state), "watch",
             "--poll-interval", "2", "--stop-file", str(stop)],
            cwd=str(self.base), env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
        )
        try:
            time.sleep(0.4)
            stop.write_text("1", encoding="utf-8")
            out, err = watch.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            watch.kill()
            watch.communicate(timeout=5)
            self.fail("watch did not stop after stop-file during wait")
        self.assertEqual(watch.returncode, 0, err)
        data = json.loads(out)
        self.assertEqual(data.get("stopped"), "stop-file")
        self.assertGreaterEqual(data.get("cycles") or 0, 1)


def _env(direction, message):
    return json.dumps({"direction": direction, "message": message}, ensure_ascii=False)


def grok_completed_log(path: Path, session="sess-1", rpc_id=4, reason="end_turn", prompt_id="p-1") -> None:
    lines = [
        _env("send", {"jsonrpc": "2.0", "id": 3, "method": "session/new", "params": {"cwd": "/w", "mcpServers": []}}),
        _env("receive", {"jsonrpc": "2.0", "id": 3, "result": {"sessionId": session}}),
        _env("send", {"jsonrpc": "2.0", "id": rpc_id, "method": "session/prompt",
                      "params": {"sessionId": session, "prompt": [{"type": "text", "text": "go"}]}}),
        _env("receive", {"jsonrpc": "2.0", "method": "_x.ai/session/prompt_complete",
                         "params": {"sessionId": session, "promptId": prompt_id, "stopReason": reason}}),
        _env("receive", {"jsonrpc": "2.0", "id": rpc_id, "result": {
            "stopReason": reason, "_meta": {"sessionId": session, "promptId": prompt_id},
        }}),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def grok_running_log(path: Path, session="sess-1", rpc_id=4) -> None:
    lines = [
        _env("send", {"jsonrpc": "2.0", "id": 3, "method": "session/new", "params": {"cwd": "/w", "mcpServers": []}}),
        _env("receive", {"jsonrpc": "2.0", "id": 3, "result": {"sessionId": session}}),
        _env("send", {"jsonrpc": "2.0", "id": rpc_id, "method": "session/prompt",
                      "params": {"sessionId": session, "prompt": [{"type": "text", "text": "go"}]}}),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class HostWatchGrokTest(unittest.TestCase):
    setUp = HostWatchTest.setUp
    run_watch = HostWatchTest.run_watch
    payload = HostWatchTest.payload
    write_thread = HostWatchTest.write_thread
    write_config = HostWatchTest.write_config
    calls = HostWatchTest.calls
    sent = HostWatchTest.sent
    load_state = HostWatchTest.load_state
    init_watch = HostWatchTest.init_watch
    track = HostWatchTest.track
    poll = HostWatchTest.poll

    def track_grok(self, ticket: str, session: str, log: Path, evidence: str = "evidence/ld10.txt",
                   wrapper_pid: int | None = None, now: float | None = None) -> dict:
        args = ["track-grok", "--ticket", ticket, "--session", session, "--log", str(log), "--evidence", evidence]
        if wrapper_pid is not None:
            args.extend(["--wrapper-pid", str(wrapper_pid)])
        if now is not None:
            args = ["--now", str(now), *args]
        result = self.run_watch(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return self.payload(result)

    def test_grok_completed_holds_when_parent_busy_and_does_not_use_thread_id(self) -> None:
        self.init_watch()
        log = self.base / "grok.ndjson"
        grok_completed_log(log)
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=1)
        data = self.poll(now=2)
        self.assertEqual(len(data.get("events") or []), 1)
        event = data["events"][0]
        self.assertEqual(event["kind"], "completed")
        self.assertEqual(event["session"], "sess-1")
        self.assertEqual(event["provider"], "grok")
        self.assertNotIn("thread", event)
        self.assertEqual(data.get("send"), "held_busy")
        self.assertEqual(self.sent(), [])
        invoked = [item["argv"] for item in self.calls()]
        self.assertTrue(all(row[0] == "thread" for row in invoked))
        self.assertFalse(any(len(row) > 2 and row[1] == "read" and row[2] == "sess-1" for row in invoked))
        self.assertFalse(any(len(row) > 2 and row[1] == "send" and row[2] == "sess-1" for row in invoked))

    def test_grok_duplicate_terminal_does_not_reawake(self) -> None:
        self.init_watch()
        log = self.base / "grok.ndjson"
        grok_completed_log(log)
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=1)
        first = self.poll(now=2)
        self.assertEqual(len(first.get("events") or []), 1)
        second = self.poll(now=3)
        self.assertEqual(second.get("events"), [])
        self.assertEqual(len(self.load_state().get("pending_events") or []), 1)

    def test_grok_new_turn_is_not_the_old_outcome(self) -> None:
        self.init_watch()
        log = self.base / "grok.ndjson"
        grok_completed_log(log, rpc_id=4, prompt_id="p-1")
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=1)
        self.poll(now=2)
        with log.open("a", encoding="utf-8") as fh:
            fh.write(_env("send", {"jsonrpc": "2.0", "id": 8, "method": "session/prompt",
                                   "params": {"sessionId": "sess-1", "prompt": [{"type": "text", "text": "rework"}]}}) + "\n")
        mid = self.poll(now=3)
        self.assertEqual(mid.get("events"), [])
        with log.open("a", encoding="utf-8") as fh:
            fh.write(_env("receive", {"jsonrpc": "2.0", "id": 8, "result": {
                "stopReason": "end_turn", "_meta": {"promptId": "p-2"},
            }}) + "\n")
        done = self.poll(now=4)
        self.assertEqual(len(done.get("events") or []), 1)
        self.assertEqual(done["events"][0]["kind"], "completed")
        self.assertIn("p-2", done["events"][0]["summary"])

    def test_grok_cancelled_rejected_unanswered_and_error(self) -> None:
        self.init_watch()
        self.write_thread("parent-1", "running", "busy")
        cases = [
            ("cancelled", "cancelled", [
                _env("send", {"jsonrpc": "2.0", "id": 5, "method": "session/cancel", "params": {"sessionId": "sess-1"}}),
                _env("receive", {"jsonrpc": "2.0", "id": 4, "result": {"stopReason": "cancelled"}}),
            ]),
            ("rejected", "rejected", [
                _env("receive", {"jsonrpc": "2.0", "id": 9, "method": "session/request_permission",
                                 "params": {"sessionId": "sess-1", "toolCall": {"title": "x"},
                                            "options": [{"optionId": "reject-once", "kind": "reject_once"}]}}),
                _env("send", {"jsonrpc": "2.0", "id": 9, "result": {"outcome": {"outcome": "selected", "optionId": "reject-once"}}}),
                _env("receive", {"jsonrpc": "2.0", "id": 4, "result": {"stopReason": "end_turn"}}),
            ]),
            ("unanswered", "unanswered", [
                _env("receive", {"jsonrpc": "2.0", "id": 9, "method": "session/request_permission",
                                 "params": {"sessionId": "sess-1", "toolCall": {"title": "x"},
                                            "options": [{"optionId": "allow-once", "kind": "allow_once"}]}}),
                _env("send", {"jsonrpc": "2.0", "id": 9, "result": {"outcome": {"outcome": "cancelled"}}}),
                _env("receive", {"jsonrpc": "2.0", "id": 4, "result": {"stopReason": "end_turn"}}),
            ]),
            ("error", "failed", [
                _env("receive", {"jsonrpc": "2.0", "id": 4, "error": {"code": -1, "message": "boom"}}),
            ]),
        ]
        for name, kind, extra in cases:
            log = self.base / f"{name}.ndjson"
            grok_running_log(log)
            with log.open("a", encoding="utf-8") as fh:
                fh.write("\n".join(extra) + "\n")
            self.track_grok(name, "sess-1", log, now=10)
            data = self.poll(now=11)
            self.assertEqual(data["events"][0]["kind"], kind, name)
            self.assertNotEqual(data["events"][0]["kind"], "completed", name)

    def test_grok_missing_process_is_unknown(self) -> None:
        self.init_watch()
        log = self.base / "gone.ndjson"
        grok_running_log(log)
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, wrapper_pid=999999999, now=1)
        data = self.poll(now=2)
        self.assertEqual(data["events"][0]["kind"], "unknown_status")
        self.assertFalse(self.load_state()["tracked"]["LD-10"].get("terminal"))

    def test_grok_auto_permission_is_not_attention(self) -> None:
        self.init_watch()
        log = self.base / "auto.ndjson"
        grok_running_log(log)
        with log.open("a", encoding="utf-8") as fh:
            fh.write(_env("receive", {"jsonrpc": "2.0", "method": "_x.ai/session_notification",
                                      "params": {"sessionId": "sess-1",
                                                 "update": {"sessionUpdate": "interaction_resolved",
                                                            "tool_call_id": "t1"}}}) + "\n")
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=1)
        data = self.poll(now=2)
        self.assertEqual(data.get("events"), [])
        self.assertEqual(data.get("called_host"), False)

    def test_grok_real_pending_permission_is_attention(self) -> None:
        self.init_watch()
        log = self.base / "perm.ndjson"
        grok_running_log(log)
        with log.open("a", encoding="utf-8") as fh:
            fh.write(_env("receive", {"jsonrpc": "2.0", "id": 11, "method": "session/request_permission",
                                      "params": {"sessionId": "sess-1", "toolCall": {"title": "write"},
                                                 "options": [{"optionId": "allow-once", "kind": "allow_once"}]}}) + "\n")
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=1)
        data = self.poll(now=2)
        self.assertEqual(data["events"][0]["kind"], "attention")

    def test_empty_run_receipt_calls_neither_host_nor_model(self) -> None:
        receipt = self.base / "empty-run.json"
        receipt.write_text(json.dumps({"claims": {}, "active_operations": {}}), encoding="utf-8")
        self.init_watch(run_receipt=receipt)
        data = self.poll(now=1)
        self.assertEqual(data.get("called_host"), False)
        self.assertEqual(data.get("events"), [])
        self.assertEqual(self.calls(), [])
        self.assertEqual(self.payload(self.run_watch("status")).get("tracked"), [])

    def test_run_receipt_claim_and_resume_update_same_ticket(self) -> None:
        log1 = self.base / "first.ndjson"
        log2 = self.base / "resume.ndjson"
        grok_completed_log(log1, session="sess-live", prompt_id="old")
        grok_running_log(log2, session="sess-live", rpc_id=6)
        receipt = self.base / "run.json"
        receipt.write_text(json.dumps({
            "claims": {
                "LD-01": {"status": "ACCEPTED", "session": "codex-session"},
                "LD-10": {
                    "status": "IN_PROGRESS",
                    "executor": {"provider": "grok-cli"},
                    "sessionId": "sess-live",
                    "log": str(log1),
                    "wrapper_pid": 11,
                    "evidence": "evidence/ld10",
                },
            },
            "active_operations": {},
        }), encoding="utf-8")
        self.init_watch(run_receipt=receipt, now=1)
        self.write_thread("parent-1", "running", "busy")
        first = self.poll(now=2)
        self.assertEqual(first["events"][0]["ticket"], "LD-10")
        self.assertEqual(first["events"][0]["session"], "sess-live")
        receipt.write_text(json.dumps({
            "claims": {
                "LD-10": {
                    "status": "IN_PROGRESS",
                    "executor": {"provider": "grok-cli"},
                    "sessionId": "sess-live",
                    "log": str(log2),
                    "evidence": "evidence/ld10",
                },
            },
            "active_operations": {
                "LD-10": {"sessionId": "sess-live", "log": str(log2)},
            },
        }), encoding="utf-8")
        second = self.poll(now=3)
        self.assertEqual(second.get("events"), [])
        tracked = self.load_state()["tracked"]["LD-10"]
        self.assertEqual(tracked["log"], str(log2))
        self.assertEqual(tracked["session"], "sess-live")
        self.assertFalse(tracked.get("terminal"))

    def test_grok_uncertain_send_does_not_resend(self) -> None:
        self.init_watch(host_timeout=1)
        log = self.base / "done.ndjson"
        grok_completed_log(log)
        self.write_thread("parent-1", "completed", "idle")
        self.track_grok("LD-10", "sess-1", log, now=1)
        self.write_config(send="timeout", send_sleep=5)
        first = self.poll(now=2, expect=0)
        self.assertEqual(first.get("send"), "uncertain")
        self.write_config(send="ok")
        second = self.poll(now=3)
        self.assertEqual(second.get("send"), "uncertain")
        self.assertEqual(self.sent(), [])

    def test_grok_direction_check_after_interval(self) -> None:
        self.init_watch(direction_interval=3600)
        log = self.base / "run.ndjson"
        grok_running_log(log)
        self.write_thread("parent-1", "running", "busy")
        self.track_grok("LD-10", "sess-1", log, now=100)
        early = self.poll(now=100)
        self.assertEqual(early.get("events"), [])
        due = self.poll(now=3700)
        self.assertEqual(len(due.get("events") or []), 1)
        self.assertEqual(due["events"][0]["kind"], "direction")

    def test_corrupt_run_receipt_does_not_break_codex_poll(self) -> None:
        receipt = self.base / "bad-run.json"
        receipt.write_text("{not-json", encoding="utf-8")
        self.init_watch(run_receipt=receipt)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "completed", "done")
        self.track("HD-01", "child-1", now=1)
        data = self.poll(now=2)
        self.assertEqual(data["events"][0]["kind"], "completed")

    def test_help_lists_track_grok(self) -> None:
        result = subprocess.run(
            [PY, str(SCRIPT), "--help"], capture_output=True, text=True, encoding="utf-8",
            check=False, timeout=10, env=self.env,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("track-grok", result.stdout)

    def test_grok_idle_running_does_not_call_host(self) -> None:
        self.init_watch()
        log = self.base / "run.ndjson"
        grok_running_log(log)
        self.track_grok("LD-10", "sess-1", log, now=1)
        data = self.poll(now=2)
        self.assertEqual(data.get("called_host"), False)
        self.assertEqual(data.get("events"), [])
        self.assertEqual(self.calls(), [])

    def test_track_grok_mutex_with_watcher(self) -> None:
        self.init_watch()
        log = self.base / "run.ndjson"
        grok_running_log(log)
        self.write_thread("parent-1", "running", "busy")
        self.write_thread("child-1", "running", "work")
        self.track("HD-01", "child-1", now=1)
        self.write_config(read_sleep=3)
        watch = subprocess.Popen(
            [PY, str(SCRIPT), "--state", str(self.state), "watch", "--max-cycles", "1"],
            cwd=str(self.base), env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8",
        )
        try:
            deadline = time.time() + 8
            while time.time() < deadline:
                if (self.fake_dir / "calls.jsonl").is_file():
                    break
                time.sleep(0.05)
            result = self.run_watch("track-grok", "--ticket", "LD-10", "--session", "sess-1", "--log", str(log))
            busy = self.payload(result)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(busy["registration"]["status"], "queued")
            self.assertTrue(Path(busy["request"]).exists())
        finally:
            try:
                watch.communicate(timeout=12)
            except subprocess.TimeoutExpired:
                watch.kill()
                watch.communicate(timeout=5)
        self.write_config(read_sleep=0)
        self.poll(now=200)
        self.assertIn("LD-10", self.load_state()["tracked"])
        self.assertEqual(json.loads(Path(busy["request"]).with_suffix(".ack").read_text())["status"], "applied")



if __name__ == "__main__":
    unittest.main()

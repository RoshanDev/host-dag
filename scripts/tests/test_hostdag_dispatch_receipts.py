"""B1: delayed dispatches and terminal receipts, using only temporary runs."""
import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import hostdag_dag_view as v


def load_watch():
    spec = importlib.util.spec_from_file_location("receipt_watch", SCRIPTS / "host-dag-watch.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class DispatchReceiptsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run = self.root / "run.json"
        self.ticket = {"state": "RUNNING", "status": "CLAIMED", "executor": "worker",
                       "updated_at": "version-A"}
        self.data = {"batches": {"B": {"tickets": {"T": self.ticket}}}}
        self.write_run()
        self.w = load_watch()
        self.state = {"state_path": str(self.root / "watch.json"), "run_receipt": str(self.run),
                      "tracked": {}, "pending_events": []}
        self.rec = {"thread": "worker"}
        self.failed = {"turnId": "A", "text": "stream disconnected"}

    def write_run(self):
        self.run.write_text(json.dumps(self.data))

    def backoff(self):
        with mock.patch.object(self.w, "run_host") as host, mock.patch.object(self.w, "alert"):
            self.w.heal_failed_worker(self.state, "T", self.rec, "failed", self.failed, 100, [])
            host.assert_not_called()

    def test_queued_retry_revalidates_authorization_freeze_and_ticket_version(self):
        for change in ("cancel", "waiting", "version", "executor", "freeze", "missing-run"):
            with self.subTest(change=change):
                self.rec.clear()
                self.rec["thread"] = "worker"
                self.ticket.update(state="RUNNING", executor="worker", updated_at="version-A")
                self.write_run()
                freeze = self.root / "control/locks/board-freeze.json"
                freeze.unlink(missing_ok=True)
                self.backoff()
                if change == "cancel":
                    self.ticket["state"] = "CANCELED"
                elif change == "waiting":
                    self.ticket["state"] = "WAITING_RESOURCE"
                elif change == "version":
                    self.ticket["updated_at"] = "version-B"
                elif change == "executor":
                    self.ticket["executor"] = "replacement"
                elif change == "freeze":
                    freeze.parent.mkdir(parents=True, exist_ok=True)
                    freeze.write_text(json.dumps({"owner": "window", "exempt": []}))
                self.write_run()
                if change == "missing-run":
                    self.run.unlink()
                with mock.patch.object(self.w, "run_host") as host, mock.patch.object(self.w, "alert"):
                    self.w.heal_failed_worker(self.state, "T", self.rec, "failed", self.failed, 300, [])
                    host.assert_not_called()
                self.assertNotIn("auto_retries", self.rec)

    def test_unchanged_queued_retry_passes_context_to_final_native_check(self):
        self.backoff()
        with mock.patch.object(self.w, "run_host", return_value={"turn": {"id": "B"}}) as host, \
                mock.patch.object(self.w, "alert"):
            self.w.heal_failed_worker(self.state, "T", self.rec, "failed", self.failed, 300, [])
        args = host.call_args.args[1]
        self.assertIn("--dispatch-context", args)
        context = json.loads(Path(args[args.index("--dispatch-context") + 1]).read_text())
        self.assertEqual(context["updated_at"], "version-A")
        self.assertEqual(context["turn_id"], "A")
        self.assertEqual(self.rec["auto_retries"], 1)

    def test_a_completion_notice_retains_identity_while_b_is_running(self):
        payload = {"status": "completed", "result": {"turnId": "A", "text": "done"}}
        with mock.patch.object(self.w, "run_host", return_value=payload):
            self.w.observe_ticket(self.state, "T", self.rec, 100, [])
            self.w.observe_ticket(self.state, "T", self.rec, 101, [])
        self.assertEqual(len(self.state["pending_events"]), 1)
        payload = {"status": "running", "result": {"turnId": "B", "text": ""}}
        with mock.patch.object(self.w, "run_host", return_value=payload), \
                mock.patch.object(self.w, "heal_running_worker", return_value=False):
            self.w.observe_ticket(self.state, "T", self.rec, 102, [])
        self.assertFalse(self.rec["terminal"])
        event = self.state["pending_events"][0]
        self.assertEqual(event.get("turnId"), "A")
        self.assertIn("turn=A", self.w.format_message("marker", [event]))

    def test_same_text_in_distinct_turns_is_not_coalesced(self):
        event = {"kind": "completed", "ticket": "T", "thread": "worker", "status": "completed", "summary": "done"}
        self.state["pending_events"] = [{**event, "turnId": "A"}, {**event, "turnId": "B"}, {**event, "turnId": "B"}]
        self.w.coalesce_pending(self.state)
        self.assertEqual([e["turnId"] for e in self.state["pending_events"]], ["A", "B"])

    def test_late_conditional_ticket_write_cannot_settle_new_execution(self):
        before = copy.deepcopy(self.data)
        with self.assertRaisesRegex(v.ViewError, "changed"):
            v.apply_ticket_update(self.data, "B", "T", {"state": "REVIEWING", "expected_updated_at": "old-A"})
        self.assertEqual(self.data, before)

    def test_ticket_versions_change_even_within_one_second(self):
        self.ticket["updated_at"] = "2026-10-27T17:10:00+08:00"
        with mock.patch.object(v, "now_iso", return_value=self.ticket["updated_at"]):
            changed = v.apply_ticket_update(self.data, "B", "T", {"blocker": "B started"})
        self.assertNotEqual(changed["updated_at"], self.ticket["updated_at"])

    def test_cli_compare_and_set_rejects_stale_completion_without_writing(self):
        before = self.run.read_bytes()
        command = [sys.executable, str(SCRIPTS / "host-dag-view.py"), "--run", str(self.root),
                   "ticket", "set", "--batch", "B", "--ticket", "T", "--state", "REVIEWING",
                   "--expected-updated-at"]
        stale = subprocess.run([*command, "old-A"], capture_output=True, text=True, timeout=10)
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("changed", stale.stderr)
        self.assertEqual(self.run.read_bytes(), before)
        current = subprocess.run([*command, "version-A"], capture_output=True, text=True, timeout=10)
        self.assertEqual(current.returncode, 0, current.stdout + current.stderr)
        self.assertEqual(json.loads(current.stdout)["T"]["state"], "REVIEWING")

    def test_guard_accepts_only_current_unfinished_reviewer_and_explicit_exemption(self):
        import hostdag_dispatch_guard as guard
        self.ticket.update(state="REVIEWING", reviews={"spec": {"thread": "reviewer"}})
        self.write_run()
        context = guard.capture(str(self.run), "T-review-spec", "reviewer", "A")
        freeze = self.root / "control/locks/board-freeze.json"
        freeze.parent.mkdir(parents=True)
        freeze.write_text(json.dumps({"exempt": ["T"]}))
        guard.check(context, "reviewer")
        self.ticket["reviews"]["spec"]["verdict"] = "PASS"
        self.write_run()
        with self.assertRaisesRegex(guard.DispatchError, "no longer owns"):
            guard.check(context, "reviewer")


if __name__ == "__main__":
    unittest.main()

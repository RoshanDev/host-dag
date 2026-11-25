from __future__ import annotations

import importlib.util
import json
import tempfile
from unittest.mock import patch
import unittest
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("watch", SCRIPTS / "host-dag-watch.py")
assert spec is not None
watch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(watch)  # type: ignore[union-attr]

NOW = 1_800_000_000.0
OLD = "2027-01-15T07:00:00+00:00"  # one hour before NOW


def run(**tickets: dict) -> dict[str, Any]:
    return {"batches": {"V17": {"tickets": tickets}}}


class IdleTicketTest(unittest.TestCase):
    def test_wait_deadline_wakes_once_despite_metadata_refresh(self) -> None:
        state = {"tracked": {}}
        rec = {"state": "WAITING_RESOURCE", "updated_at": OLD, "round": 2,
               "attempt": 2, "candidate": "abc",
               "blocker": "WAIT-UNTIL:2027-01-15T09:00:00+00:00 quota"}
        data = run(A=rec)
        self.assertEqual(watch.idle_ticket_events(state, data, NOW), [])
        self.assertEqual(len(watch.idle_ticket_events(state, data, NOW + 3600)), 1)
        rec.update(updated_at="2027-01-15T09:00:00+00:00",
                   blocker="WAIT-UNTIL:2027-01-15T09:15:00+00:00 still quota")
        self.assertEqual(watch.idle_ticket_events(state, data, NOW + 7200), [])
        # Survives watcher restart; a new candidate re-arms the reminder.
        state = json.loads(json.dumps(state))
        self.assertEqual(watch.idle_ticket_events(state, data, NOW + 86400), [])
        rec["candidate"] = "def"
        self.assertEqual(len(watch.idle_ticket_events(state, data, NOW + 86400)), 1)

    def test_wait_rearms_after_real_work_and_other_events_are_preserved(self) -> None:
        state = {"tracked": {}}
        data = run(A={"state": "WAITING_RESOURCE", "updated_at": OLD})
        self.assertEqual(len(watch.idle_ticket_events(state, data, NOW)), 1)
        self.assertEqual(watch.idle_ticket_events(state, data, NOW + 3600), [])
        state["tracked"]["A"] = {"terminal": False}
        self.assertEqual(watch.idle_ticket_events(state, data, NOW + 7200), [])
        state["tracked"]["A"]["terminal"] = True
        self.assertEqual(len(watch.idle_ticket_events(state, data, NOW + 10800)), 1)
        watch.queue_event(state, {"kind": "failed", "ticket": "A"})
        watch.queue_event(state, {"kind": "completed", "ticket": "B"})
        watch.coalesce_pending(state)
        self.assertEqual([e["kind"] for e in state["pending_events"]], ["failed", "completed"])

    def test_future_wait_removes_only_stale_idle_pending_events(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            receipt = Path(tmp) / "run.json"
            receipt.write_text(json.dumps(run(A={"state": "WAITING_RESOURCE", "updated_at": OLD,
                "blocker": "WAIT-UNTIL:2027-01-15T09:00:00+00:00 quota"})))
            events = [{"kind": "attention", "status": "idle", "ticket": "A"},
                      {"kind": "completed", "ticket": "A"}, {"kind": "failed", "ticket": "B"}]
            state = {"run_receipt": str(receipt), "pending_events": events.copy()}
            with patch.object(watch, "state_alerts"):
                watch.check_idle_tickets(state, NOW)
                self.assertEqual(state["pending_events"], events[1:])
                state.update(pending_events=events.copy(), pending_send={"status": "uncertain"})
                watch.check_idle_tickets(state, NOW)
                self.assertEqual(state["pending_events"], events)

    def test_ticket_started_ahead_of_its_dependency_is_still_watched(self) -> None:
        state: dict[str, Any] = {"tracked": {}}
        data = run(**{"KV18-07": {"state": "WAITING_RESOURCE", "round": 10, "updated_at": OLD,
                                  "blocker": "WAIT-UNTIL:2027-01-15T07:30:00+00:00 quota"},
                      "KV18-10": {"state": "WAITING_RESOURCE", "deps": ["KV18-07"], "round": 1, "attempt": 4,
                                  "updated_at": OLD, "blocker": "WAIT-UNTIL:2027-01-15T07:30:00+00:00 slot"},
                      "KV18-09": {"state": "WAITING_DEPENDENCY", "deps": ["KV18-07"], "updated_at": OLD}})
        self.assertIn("KV18-10", [e["ticket"] for e in watch.idle_ticket_events(state, data, NOW)])
        self.assertNotIn("KV18-09", [e["ticket"] for e in watch.idle_ticket_events(state, data, NOW + 7200)])

    def test_forgotten_ticket_with_finished_reviews_is_nudged_once(self) -> None:
        state: dict[str, Any] = {"tracked": {"KV17-04": {"terminal": True}, "KV17-04-review-spec": {"terminal": True}}}
        data = run(**{"KV17-03": {"status": "ACCEPTED"},
                      "KV17-04": {"status": "CLAIMED", "outcome": "READY_FOR_ACCEPTANCE", "deps": ["KV17-03"],
                                  "updated_at": OLD}})
        events = watch.idle_ticket_events(state, data, NOW)
        self.assertEqual([e["ticket"] for e in events], ["KV17-04"])
        self.assertEqual(watch.idle_ticket_events(state, data, NOW + 60), [])
        self.assertEqual(len(watch.idle_ticket_events(state, data, NOW + 31 * 60)), 1)

    def test_running_work_waiting_deps_and_declared_waits_are_left_alone(self) -> None:
        state: dict[str, Any] = {"tracked": {"T-03": {"terminal": False}}}
        data = run(**{
            "T-03": {"status": "CLAIMED", "updated_at": OLD},
            "KV17-05": {"deps": ["KV17-04"], "updated_at": OLD},
            "KV17-04": {"status": "CLAIMED", "updated_at": OLD,
                        "blocker": "WAIT-UNTIL:2027-01-15T09:00:00+00:00 等共享锁"},
            "KV17-06": {"status": "CLAIMED", "outcome": "NEEDS_DECISION", "updated_at": OLD},
            "KV17-08": {"status": "CLAIMED", "outcome": "EXTERNALLY_BLOCKED", "updated_at": OLD},
            "KV17-07": {"status": "ACCEPTED", "updated_at": OLD},
        })
        self.assertEqual(watch.idle_ticket_events(state, data, NOW), [])

    def test_ready_but_never_dispatched_ticket_is_nudged(self) -> None:
        data = run(**{"KV17-04": {"status": "ACCEPTED"}, "KV17-05": {"deps": ["KV17-04"], "updated_at": OLD}})
        events = watch.idle_ticket_events({"tracked": {}}, data, NOW)
        self.assertEqual(events[0]["ticket"], "KV17-05")
        self.assertIn("未派", events[0]["summary"])

    def test_idle_counts_from_the_last_dependency_acceptance(self) -> None:
        just = "2027-01-15T07:59:50+00:00"  # ten seconds before NOW
        data = run(**{"KVT2-R-02": {"status": "ACCEPTED", "accepted_at": just},
                      "KVT2-R-03": {"deps": ["KVT2-R-02"], "updated_at": OLD}})
        self.assertEqual(watch.idle_ticket_events({"tracked": {}}, data, NOW), [])
        events = watch.idle_ticket_events({"tracked": {}}, data, NOW + 21 * 60)
        self.assertIn("已 21 分钟", events[0]["summary"])



class ResolvedWaitTest(unittest.TestCase):
    def test_answered_decisions_accepted_tickets_and_free_page_are_nudged_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "advisor").mkdir()
            (root / "control").mkdir()
            (root / "advisor/log.jsonl").write_text(json.dumps(
                {"key": "advisor:KV18-02:maint-orphan", "verdict": "decided", "at": NOW - 3000}) + "\n")
            (root / "control/api-page-validation-pause.json").write_text(json.dumps({"owner": None, "status": "RESUMED"}))
            data = {"batches": {"V18": {"tickets": {
                "KV18-02": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD,
                            "wait_on": "decision:advisor:KV18-02:maint-orphan"},
                "T18-06": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD,
                            "wait_on": "ticket:T-05"},
                "KV18-04": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD, "wait_on": "page-window"},
                "KV18-07": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD,
                            "wait_on": "decision:advisor:other"}}},
                    "B1": {"tickets": {"T-05": {"status": "ACCEPTED"}}}}}
            (root / "run.json").write_text(json.dumps(data))
            state = {"state_path": str(root / "watch.json")}
            events = watch.resolved_wait_events(state, data, NOW)
            self.assertEqual(sorted((e["ticket"], e["status"]) for e in events),
                             [("KV18-02", "wait-released"), ("KV18-04", "resolved-wait"), ("T18-06", "wait-released")])
            run = json.loads((root / "run.json").read_text())["batches"]["V18"]["tickets"]
            self.assertNotIn("wait_on", run["KV18-02"])
            self.assertEqual(run["KV18-02"]["pending_action"]["kind"], "apply_decision")
            self.assertEqual(run["T18-06"]["pending_action"]["kind"], "continue")
            self.assertEqual(run["KV18-02"]["resolved_waits"][0]["wait_on"], "decision:advisor:KV18-02:maint-orphan")
            self.assertIn("wait_on", run["KV18-07"])  # unanswered question: untouched
            # Released tickets are no longer waiting; the page wait is only re-nudged later.
            fresh = json.loads((root / "run.json").read_text())
            self.assertEqual(watch.resolved_wait_events(state, fresh, NOW + 60), [])

    def test_released_wait_keeps_a_legal_state(self) -> None:
        """core-mirror: T-IMG-1 / T-PW-2 came out of an automatic release with state None."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "advisor").mkdir()
            (root / "advisor/log.jsonl").write_text(json.dumps(
                {"key": "advisor:Q1", "verdict": "decided", "at": NOW - 3000}) + "\n")
            data = {"batches": {"B2": {"tickets": {
                "CLAIMED-RES": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD,
                                "wait_on": "ticket:UP"},
                "FRESH-DEP": {"state": "WAITING_DEPENDENCY", "deps": ["UP"], "updated_at": OLD, "wait_on": "ticket:UP"},
                "ASKED": {"state": "WAITING_DEPENDENCY", "attempt": 1, "updated_at": OLD,
                          "wait_on": "decision:advisor:Q1"},
                "NO-STATE": {"status": "CLAIMED", "blocker": "WAIT-DEPENDENCY: UP", "updated_at": OLD,
                             "wait_on": "ticket:UP"},
                "UP": {"status": "ACCEPTED", "state": "ACCEPTED"}}}}}
            (root / "run.json").write_text(json.dumps(data))
            events = watch.resolved_wait_events({"state_path": str(root / "watch.json")}, data, NOW)
            self.assertEqual(sorted(e["ticket"] for e in events), ["ASKED", "CLAIMED-RES", "FRESH-DEP", "NO-STATE"])
            run = json.loads((root / "run.json").read_text())["batches"]["B2"]["tickets"]
            self.assertEqual({n: (t.get("state"), t.get("status"), t.get("outcome")) for n, t in run.items() if n != "UP"}, {
                "CLAIMED-RES": ("RUNNING", "CLAIMED", None),
                "FRESH-DEP": ("CREATED", None, None),
                "ASKED": ("RUNNING", "CLAIMED", None),  # dispatched before (attempt 1): between rounds
                "NO-STATE": (None, "CLAIMED", None)})  # nothing written: still derived from status
            self.assertTrue(all("wait_on" not in t for t in run.values()))
            self.assertTrue(all(t["state"] in watch.ts.STATES for t in run.values() if "state" in t))

    def test_lock_waits_are_checked_against_the_real_flock(self) -> None:
        # 2026-10-20 T-05: the board said T17-INTEGRATE held devcluster-shared; the lock was free.
        import fcntl
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "control").mkdir()
            for name in ("devcluster-shared.lock", "host-b.lock"):
                (root / "control" / name).write_text("")
            data = {"batches": {"B1": {"tickets": {
                "T-05": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD,
                           "wait_on": "devcluster-shared", "wait_holder": "T17-INTEGRATE"},
                "T-06": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD, "wait_on": "host:host-a"},
                "T-07": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD, "wait_on": "host:host-b"}}}}}
            (root / "run.json").write_text(json.dumps(data))
            state = {"state_path": str(root / "watch.json")}
            held = open(root / "control/host-b.lock", "a")
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                self.assertEqual(watch.resolved_wait_events(state, data, NOW), [])  # first sighting only
                events = watch.resolved_wait_events(state, data, NOW + 11 * 60)
                # devcluster-shared free for 11 min -> nudged; host-a held -> not; host-b has no lock file -> not.
                self.assertEqual([(e["ticket"], e["status"]) for e in events], [("T-05", "resolved-wait")])
                self.assertIn("T17-INTEGRATE", events[0]["summary"])
                self.assertIn("wait_on", json.loads((root / "run.json").read_text())["batches"]["B1"]["tickets"]["T-05"])
            finally:
                held.close()

    def test_release_is_skipped_when_the_ticket_changed_since_it_was_observed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "advisor").mkdir()
            (root / "advisor/log.jsonl").write_text(json.dumps({"key": "q", "verdict": "decided", "at": NOW - 3000}) + "\n")
            observed = {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD, "wait_on": "decision:q"}
            newer = {**observed, "updated_at": "2027-01-15T07:59:00+00:00"}
            (root / "run.json").write_text(json.dumps({"batches": {"B": {"tickets": {"T": newer}}}}))
            self.assertIsNone(watch.release_wait(root, "B", "T", observed, "decision", "q",
                                                 {"q": NOW - 3000}, NOW))
            self.assertIn("wait_on", json.loads((root / "run.json").read_text())["batches"]["B"]["tickets"]["T"])


class PageQueueTest(unittest.TestCase):
    """Stage A #4: approved page-window specs run when the page is free, one at a time."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "control").mkdir()
        self.page({"owner": None, "status": "RESUMED"})
        self.calls: list[tuple[str, ...]] = []
        self.check_ok, self.active = True, False

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def page(self, record):
        (self.root / "control/api-page-validation-pause.json").write_text(json.dumps(record))

    def spec(self, name: str) -> str:
        d = self.root / name
        d.mkdir(exist_ok=True)
        (d / "spec.json").write_text(json.dumps({"owner": name, "grant": {"allowRealWindow": True}}))
        return str(d / "spec.json")

    def write(self, tickets: dict) -> dict:
        data = {"batches": {"V18": {"tickets": tickets}}}
        (self.root / "run.json").write_text(json.dumps(data))
        return data

    def cli(self, *args):
        self.calls.append(args)
        ok = self.check_ok if args[0] == "check" else True
        return __import__("subprocess").CompletedProcess(args, 0 if ok else 1, "" if ok else "spec refused", "")

    def tick(self, now=NOW):
        data = json.loads((self.root / "run.json").read_text())
        return watch.page_queue_events({"state_path": str(self.root / "watch.json")}, data, now,
                                       run_cli=self.cli, units_active=lambda owner: self.active)

    def req(self, spec, rid, priority=0, at="2027-01-15T07:00:00+00:00", **extra):
        return {"page_request": {"id": rid, "spec": spec, "owner": Path(spec).parent.name, "priority": priority,
                                 "approved_at": at, "status": "pending", **extra},
                "status": "CLAIMED", "state": "WAITING_RESOURCE", "wait_on": "page-window"}

    def tickets(self):
        return json.loads((self.root / "run.json").read_text())["batches"]["V18"]["tickets"]

    def test_highest_priority_runs_first_and_only_one_at_a_time(self) -> None:
        self.write({"A": self.req(self.spec("a"), "pr-a"), "B": self.req(self.spec("b"), "pr-b", priority=5)})
        events = self.tick()
        self.assertEqual([(e["ticket"], e["status"]) for e in events], [("B", "window-started")])
        self.assertEqual([c[0] for c in self.calls], ["check", "run"])
        self.assertEqual(self.tickets()["B"]["page_request"]["status"], "claimed")
        self.assertEqual(self.tickets()["A"]["page_request"]["status"], "pending")
        self.active = True
        self.assertEqual(self.tick(NOW + 60), [])  # B still running: A waits

    def test_busy_page_starts_nothing(self) -> None:
        self.write({"A": self.req(self.spec("a"), "pr-a")})
        self.page({"owner": "SOMEONE", "status": "PAUSED"})
        self.assertEqual(self.tick(), [])
        self.assertEqual(self.calls, [])

    def test_wait_resource_requeues_and_other_results_wake_the_coordinator(self) -> None:
        spec = self.spec("a")
        self.write({"A": {**self.req(spec, "pr-a"), "page_request": {
            **self.req(spec, "pr-a")["page_request"], "status": "claimed", "claimed_at_ts": NOW - 100}}})
        (self.root / "a/result.json").write_text(json.dumps({"at": NOW - 10, "outcome": "WAIT_RESOURCE", "busy": ["guest"]}))
        self.assertEqual(self.tick(), [])
        req = self.tickets()["A"]["page_request"]
        self.assertEqual((req["status"], req["next_check_ts"]), ("pending", NOW + watch.PAGE_RETRY_SECONDS))
        self.write({"A": {**self.req(spec, "pr-a"), "page_request": {
            **self.req(spec, "pr-a")["page_request"], "status": "claimed", "claimed_at_ts": NOW - 100}}})
        (self.root / "a/result.json").write_text(json.dumps({"at": NOW - 10, "outcome": "CLEAN", "status": "STEPS_DONE"}))
        events = self.tick()
        self.assertEqual(events[0]["status"], "window-finished")
        t = self.tickets()["A"]
        self.assertEqual((t["page_request"]["status"], t["pending_action"]["outcome"]), ("done", "CLEAN"))
        self.assertNotIn("wait_on", t)

    def test_lost_run_is_never_relaunched(self) -> None:
        spec = self.spec("a")
        self.write({"A": {**self.req(spec, "pr-a"), "page_request": {
            **self.req(spec, "pr-a")["page_request"], "status": "claimed", "claimed_at_ts": NOW - 1000}}})
        events = self.tick()
        self.assertEqual(events[0]["status"], "window-lost")
        self.assertEqual(self.tickets()["A"]["page_request"]["status"], "needs_attention")
        self.assertEqual(self.calls, [])

    def test_failed_check_is_not_launched(self) -> None:
        self.write({"A": self.req(self.spec("a"), "pr-a")})
        self.check_ok = False
        events = self.tick()
        self.assertEqual(events[0]["status"], "window-check-failed")
        self.assertEqual([c[0] for c in self.calls], ["check"])

    def test_withdrawn_request_is_not_claimed(self) -> None:
        spec = self.spec("a")
        data = {"batches": {"V18": {"tickets": {"A": self.req(spec, "pr-a")}}}}
        self.write({"A": self.req(spec, "pr-OTHER")})  # replaced on disk after `data` was read
        events = watch.page_queue_events({"state_path": str(self.root / "watch.json")}, data, NOW,
                                         run_cli=self.cli, units_active=lambda owner: False)
        self.assertEqual(events, [])
        self.assertEqual([c[0] for c in self.calls], ["check"])


class PageMetricsTest(unittest.TestCase):
    def test_occupant_and_request_phase_changes_are_logged_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "control").mkdir()
            reg = root / "control/api-page-validation-pause.json"
            reg.write_text(json.dumps({"owner": "W-1", "status": "PAUSED"}))
            req = lambda rid, pr, st="pending": {"page_request": {"id": rid, "status": st, "priority": pr,
                                                                    "approved_at": "2027-01-15T07:00:00+00:00"}}
            data = {"batches": {"V18": {"tickets": {"A": req("pr-a", 0), "B": req("pr-b", 5)}}}}
            state = {"state_path": str(root / "watch.json")}
            rows = watch.page_metrics_tick(state, data, NOW)
            self.assertEqual([(r["event"], r.get("reason"), r.get("holder")) for r in rows],
                             [("page", None, None), ("request", "page-held", "W-1"), ("request", "page-held", "W-1")])
            self.assertEqual(watch.page_metrics_tick(state, data, NOW + 30), [])  # nothing changed
            reg.write_text(json.dumps({"owner": None, "status": "RESUMED"}))
            rows = watch.page_metrics_tick(state, data, NOW + 60)
            reasons = {r.get("ticket"): (r.get("reason"), r.get("holder")) for r in rows if r["event"] == "request"}
            self.assertEqual(reasons, {"B": ("eligible", None), "A": ("behind-in-queue", "pr-b")})
            lines = (root / "control/page-window-metrics.jsonl").read_text().splitlines()
            self.assertEqual(len(lines), 6)


class LimitRedeliveryTest(unittest.TestCase):
    """Coordinator turns lost to a Claude usage limit are resent once after the reset (2026-10-19 16:06)."""

    def test_reset_time_parsing(self) -> None:
        import datetime as _dt
        started = _dt.datetime(2026, 10, 19, 16, 6).timestamp()
        self.assertEqual(_dt.datetime.fromtimestamp(watch._reset_at("You've hit your session limit · resets 4:10pm (Asia/Taipei)", started)).strftime("%H:%M"), "16:10")
        self.assertEqual(watch._reset_at("no time", started), started + 600)
        late = _dt.datetime(2026, 10, 19, 23, 0).timestamp()
        self.assertGreater(watch._reset_at("resets 1am", late), late)

    def test_lost_turns_are_held_then_resent_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            host = Path(tmp)
            (host / "turns").mkdir()
            turns = []
            for i, (err, text) in enumerate([(True, "You've hit your session limit · resets 4:10pm"), (False, "ok")]):
                out = host / "turns" / f"claude-1.t{i}.out.json"
                out.write_text(json.dumps({"is_error": err, "api_error_status": 429 if err else None, "result": text}))
                (host / "turns" / f"claude-1.t{i}.in.txt").write_text(f"packet {i}")
                turns.append({"id": f"t{i}", "out": str(out), "started": NOW + i, "status": "completed"})
            (host / "claude-1.json").write_text(json.dumps({"turns": turns}))
            sent = []
            def fake_host(state, args, called):
                if args[:2] == ["thread", "read"]:
                    return {"status": "completed"}
                sent.append(args)
                return {}
            saved = (watch.CLAUDE_HOST_STATE, watch.run_host, watch.alert)
            watch.CLAUDE_HOST_STATE, watch.run_host = host, fake_host
            watch.alert = lambda *a, **k: True
            try:
                state = {"parent_thread": "claude-1", "limit_watch_since": NOW - 10}
                early = watch.redeliver_limited(state, NOW + 5, [])
                self.assertEqual(early["send"], "held_limit")
                later = watch._reset_at("resets 4:10pm", NOW) + 120
                self.assertEqual(watch.redeliver_limited(state, later, [])["send"], "redelivered")
                self.assertIn("packet 0", sent[0][-1])
                self.assertNotIn("packet 1", sent[0][-1])  # the successful turn is not resent
                self.assertIsNone(watch.redeliver_limited(state, later + 60, []))  # only once
            finally:
                watch.CLAUDE_HOST_STATE, watch.run_host, watch.alert = saved


class StateDriftTest(unittest.TestCase):
    def test_live_executor_under_a_waiting_state_is_nudged_once(self) -> None:
        state: dict[str, Any] = {"tracked": {"KV17-05": {"terminal": False}, "KV18-07": {"terminal": True}}}
        data = run(**{
            "KV17-05": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD},
            "KV18-07": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": OLD},
            "T-05": {"status": "CLAIMED", "state": "RUNNING", "updated_at": OLD}})
        state["tracked"]["T-05"] = {"terminal": False}
        events = watch.state_drift_events(state, data, NOW)
        self.assertEqual([e["ticket"] for e in events], ["KV17-05"])
        self.assertIn("--state RUNNING", events[0]["summary"])
        self.assertEqual(watch.state_drift_events(state, data, NOW + 60), [])

    def test_fresh_waiting_state_gets_a_grace_period(self) -> None:
        state: dict[str, Any] = {"tracked": {"KV17-05": {"terminal": False}}}
        fresh = "2027-01-15T07:55:00+00:00"  # five minutes before NOW
        data = run(KV17_05={"status": "CLAIMED", "state": "WAITING_RESOURCE", "updated_at": fresh})
        data["batches"]["V17"]["tickets"] = {"KV17-05": data["batches"]["V17"]["tickets"].pop("KV17_05")}
        self.assertEqual(watch.state_drift_events(state, data, NOW), [])

if __name__ == "__main__":
    unittest.main()

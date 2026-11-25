from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import hostdag_dag_view as v  # noqa: E402
import hostdag_self_heal as heal  # noqa: E402
import hostdag_ticket_state as ts  # noqa: E402


def load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / file)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


watch = load("watch", "host-dag-watch.py")
# The host-dag skill copies the watcher and board but not the quota picker.
quota = load("quota", "host-dag-quota.py") if (SCRIPTS / "host-dag-quota.py").exists() else None

NOW = 1_800_000_000.0
OLD = "2027-01-15T07:00:00+00:00"  # one hour before NOW


def run(**tickets: Any) -> dict[str, Any]:
    return {"batches": {"V18": {"title": "V18 验收", "tickets": tickets}}}


class DeriveStateTest(unittest.TestCase):
    def test_old_fields_map_to_one_state(self) -> None:
        tickets = {
            "A": {"status": "ACCEPTED"},
            "B": {"status": "SUPERSEDED"},
            "C": {"status": "CLAIMED", "outcome": "NEEDS_DECISION"},
            "D": {"status": "CLAIMED", "outcome": "EXTERNALLY_BLOCKED"},
            "E": {"status": "CLAIMED", "outcome": "READY_FOR_ACCEPTANCE"},
            "F": {"status": "CLAIMED", "reviews": {"spec": {"thread": "t"}}},
            "G": {"status": "CLAIMED"},
            "H": {"deps": ["G"]},
            "I": {"deps": ["A"]},
            "J": {"status": "CLAIMED", "blocker": "WAIT-RESOURCE: MemAvailable 9GiB < 12GiB"},
            "K": {"status": "CLAIMED", "blocker": "WAIT-UNTIL:2026-10-15T20:57:35+08:00 WAIT-DEPENDENCY: 等KV17-05"},
            "L": {"status": "CLAIMED", "blocker": "WAIT-UNTIL:2026-10-15T20:57:35+08:00 等共享锁"},
            "M": {"status": "CLAIMED", "blocker": "WAIT-USER: 2403 ISO 不能挂给 host-b"},
        }
        got = {name: ts.derive_state(rec, tickets) for name, rec in tickets.items()}
        self.assertEqual(got, {
            "A": "ACCEPTED", "B": "CANCELED", "C": "NEEDS_USER", "D": "EXTERNALLY_BLOCKED", "E": "REVIEWING",
            "F": "REVIEWING", "G": "RUNNING", "H": "WAITING_DEPENDENCY", "I": "CREATED",
            "J": "WAITING_RESOURCE", "K": "WAITING_DEPENDENCY", "L": "WAITING_RESOURCE", "M": "NEEDS_USER",
        })

    def test_written_state_wins_and_attempt_falls_back_to_round(self) -> None:
        rec = {"status": "CLAIMED", "state": "integrated", "round": 4}
        self.assertEqual(ts.derive_state(rec), "INTEGRATED")
        self.assertEqual(ts.attempt_of(rec), 4)
        self.assertEqual(ts.attempt_of({**rec, "attempt": 1}), 1)
        self.assertEqual(ts.derive_state({"state": "bogus", "status": "ACCEPTED"}), "ACCEPTED")

    def test_transitions_and_finished_batches(self) -> None:
        before = ts.snapshot(run(**{"KV18-01": {"status": "CLAIMED"}, "KV18-02": {"status": "ACCEPTED"}}))
        after = ts.snapshot(run(**{"KV18-01": {"status": "ACCEPTED", "state": "ACCEPTED", "accept_scope": "SCOPED"},
                                   "KV18-02": {"status": "ACCEPTED"}, "KV18-03": {"state": "CANCELED"}}))
        changes = ts.transitions(before, after)
        self.assertEqual([(c["ticket"], c["from"], c["to"]) for c in changes],
                         [("KV18-01", "RUNNING", "ACCEPTED"), ("KV18-03", None, "CANCELED")])
        self.assertEqual(changes[0]["scope"], "SCOPED")
        self.assertEqual(ts.finished_batches(before, after), ["V18"])
        self.assertEqual(ts.finished_batches(after, after), [])


class StateAlertsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.raised: list[tuple[str, str]] = []
        self.state: dict[str, Any] = {}

    def notify(self, state: dict, key: str, title: str, body: str, now: float, desktop: bool = True) -> bool:
        self.raised.append((key, title))
        return True

    def step(self, data: dict) -> list[str]:
        self.raised.clear()
        heal.state_alerts(self.state, data, NOW, self.notify)
        return [k for k, _ in self.raised]

    def test_first_poll_only_records_then_pushes_transitions(self) -> None:
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"}, "KV18-02": {"status": "CLAIMED"}})), [])
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"},
                                          "KV18-02": {"status": "CLAIMED", "outcome": "NEEDS_DECISION",
                                                      "blocker": "A级：ISO 不能挂"}})), ["needs_user:KV18-02:1"])
        self.assertIn("需要你决定", self.raised[0][1])
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"}, "KV18-02": {"status": "CLAIMED"}})), [])
        # Second entry into NEEDS_USER is a new question.
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"},
                                          "KV18-02": {"state": "NEEDS_USER"}})), ["needs_user:KV18-02:2"])
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"},
                                          "KV18-02": {"state": "ACCEPTED", "accept_scope": "REDUCED"}})),
                         ["accepted:KV18-02", "result:V18"])
        self.assertEqual(self.raised[0][1], "KV18-02 已验收（缩减）")
        # ACCEPTED -> INTEGRATED is not news.
        self.assertEqual(self.step(run(**{"KV18-01": {"status": "ACCEPTED"},
                                          "KV18-02": {"state": "INTEGRATED"}})), [])

    def test_backfilled_ticket_or_batch_is_not_pushed(self) -> None:
        self.step(run(**{"KV18-01": {"status": "CLAIMED"}}))
        data = run(**{"KV18-01": {"status": "CLAIMED"}})
        data["batches"]["V19"] = {"tickets": {"KV19-01": {"status": "ACCEPTED"}}}
        self.assertEqual(self.step(data), [])

    def test_admission_bounce_is_not_a_transition(self) -> None:
        waiting = {"status": "CLAIMED", "round": 2, "blocker": "WAIT-RESOURCE: 内存不足"}
        self.step(run(**{"KV18-02": waiting}))
        self.assertEqual(self.step(run(**{"KV18-02": {**waiting, "round": 3}})), [])


class TelegramRoutingTest(unittest.TestCase):
    def test_wording_fallback_only_for_tickets_the_state_machine_does_not_cover(self) -> None:
        self.assertTrue(heal.telegram_worthy("coordinator:x", "KV18-01 需你决定（A级）"))
        self.assertFalse(heal.telegram_worthy("coordinator:x", "KV18-01 需你决定（A级）", {"KV18-01"}))
        self.assertTrue(heal.telegram_worthy("coordinator:x", "host-a 需你重启", {"KV18-01"}))
        self.assertTrue(heal.telegram_worthy("needs_user:KV18-01:1", "KV18-01 需要你决定"))
        self.assertFalse(heal.telegram_worthy("idle:KV18-01", "KV18-01 没人在跑"))

    def test_push_identity_is_shared_with_old_prefixed_entries(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            (run_dir / "telegram-sent.json").write_text(json.dumps(["coordinator:result:KVT2-R"]))
            self.assertFalse(heal.first_telegram(run_dir, "result:KVT2-R", "批次 KVT2-R 全部验收"))
            self.assertTrue(heal.first_telegram(run_dir, "accepted:KV18-01", "KV18-01 已验收"))
            self.assertFalse(heal.first_telegram(run_dir, "coordinator:KV18-01-accepted", "KV18-01 限定验收完成"))
            self.assertTrue(heal.first_telegram(run_dir, "needs_user:KV18-02:1", "KV18-02 需要你决定"))
            self.assertFalse(heal.first_telegram(run_dir, "needs_user:KV18-02:1", "KV18-02 需要你决定"))
            self.assertTrue(heal.first_telegram(run_dir, "needs_user:KV18-02:2", "KV18-02 需要你决定"))


class IdleWaitingTest(unittest.TestCase):
    def test_declared_wait_without_time_is_nudged_after_the_waiting_timeout(self) -> None:
        half = "2027-01-15T07:30:00+00:00"  # 30 minutes before NOW
        data = run(**{"KV18-02": {"status": "CLAIMED", "blocker": "WAIT-RESOURCE: 内存不足", "updated_at": half}})
        self.assertEqual(watch.idle_ticket_events({"tracked": {}}, data, NOW), [])
        events = watch.idle_ticket_events({"tracked": {}}, data, NOW + 31 * 60)
        self.assertIn("WAITING_RESOURCE", events[0]["summary"])

    def test_needs_user_and_failed_are_never_nudged(self) -> None:
        data = run(**{"A": {"state": "NEEDS_USER", "updated_at": OLD}, "B": {"state": "FAILED", "updated_at": OLD}})
        self.assertEqual(watch.idle_ticket_events({"tracked": {}}, data, NOW + 86400), [])


class WriterTest(unittest.TestCase):
    def test_state_write_keeps_old_fields_consistent(self) -> None:
        r = run(**{"KV18-01": {"status": "CLAIMED", "round": 3}})
        t = v.apply_ticket_update(r, "V18", "KV18-01", {"state": "needs_user"})
        self.assertEqual((t["state"], t["status"], t["outcome"]), ("NEEDS_USER", "CLAIMED", "NEEDS_DECISION"))
        t = v.apply_ticket_update(r, "V18", "KV18-01", {"state": "ACCEPTED", "accept_scope": "scoped"})
        self.assertEqual((t["status"], t["accept_scope"]), ("ACCEPTED", "SCOPED"))
        self.assertNotIn("outcome", t)
        self.assertIn("accepted_at", t)
        self.assertEqual(v.display_state(r["batches"]["V18"], "KV18-01"), ("ok", "限定"))
        with self.assertRaises(v.ViewError):
            v.apply_ticket_update(r, "V18", "KV18-01", {"state": "DONE-ish"})

    def test_attempt_counts_dispatches_and_old_writes_drop_the_state(self) -> None:
        r = run(**{"KV18-02": {"status": "CLAIMED", "round": 4}})
        t = v.apply_ticket_update(r, "V18", "KV18-02", {"attempt": "1", "state": "WAITING_RESOURCE", "wait_on": "heavy-slot"})
        self.assertEqual((t["attempt"], t["round"]), (1, 4))
        self.assertEqual(v.display_state(r["batches"]["V18"], "KV18-02"), ("res", ""))
        t = v.apply_ticket_update(r, "V18", "KV18-02", {"attempt": "+1", "state": "RUNNING"})
        self.assertEqual(t["attempt"], 2)
        t = v.apply_ticket_update(r, "V18", "KV18-02", {"outcome": "EXTERNALLY_BLOCKED"})
        self.assertNotIn("state", t)
        self.assertEqual(ts.derive_state(t), "EXTERNALLY_BLOCKED")


@unittest.skipIf(quota is None, "host-dag-quota.py is not shipped with this copy")
class QuotaAttemptTest(unittest.TestCase):
    def args(self, **kw: Any) -> argparse.Namespace:
        return argparse.Namespace(**{"attempt": None, "ticket": None, "run_receipt": "/nonexistent", "round_no": None,
                                     **kw})

    def test_attempt_replaces_round_for_escalation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "run.json"
            path.write_text(json.dumps(run(**{"KV18-02": {"status": "CLAIMED", "round": 7, "attempt": 1},
                                              "KV18-03": {"status": "CLAIMED", "round": 7}})))
            assert quota is not None
            self.assertEqual(quota.escalation_count(self.args(ticket="KV18-02", run_receipt=str(path), round_no=8)),
                             (2, "attempt"))
            self.assertEqual(quota.escalation_count(self.args(ticket="V18/KV18-03", run_receipt=str(path), round_no=8)),
                             (8, "round"))
            self.assertEqual(quota.escalation_count(self.args(attempt=6, round_no=2)), (6, "attempt"))
        readings = {"codex": {"percent": 80}, "grok": {"percent": 0}, "cursor": {"percent": 40}}
        self.assertEqual(quota.select(readings, "executor", 2, "attempt")["executor"], "claude")
        got = quota.select(readings, "executor", 6, "attempt")
        self.assertEqual((got["executor"], got["attempt"]), ("claude", 6))


class ReplayTest(unittest.TestCase):
    def test_replay_rebuilds_the_day_from_one_snapshot(self) -> None:
        day = time.mktime((2026, 10, 15, 0, 0, 0, 0, 0, -1))
        stamp = lambda h, m: time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(day + h * 3600 + m * 60))  # noqa: E731
        data = run(**{"KV18-01": {"status": "ACCEPTED", "accepted_at": stamp(16, 6)},
                      "KV18-02": {"status": "CLAIMED", "deps": ["KV18-01"]}})
        data["batches"]["V17"] = {"tickets": {"KV17-06": {"status": "ACCEPTED", "accepted_at": stamp(17, 42)}}}
        alerts = [
            {"at": day + 13 * 3600 + 8 * 60, "key": "coordinator:KV18-01-iso-not-shared", "title": "KV18-01 需你决定（A级）"},
            {"at": day + 16 * 3600 + 7 * 60, "key": "coordinator:KV18-01-accepted-scoped", "title": "KV18-01 限定验收完成"},
            {"at": day + 12 * 3600, "key": "coordinator:T18-06-round1-stop", "title": "T18-06 round1 停笔"},
        ]
        got = [(p["via"], p["key"]) for p in ts.replay(data, alerts, day)]
        self.assertEqual(got, [("wording-fallback", "KV18-01-iso-not-shared"), ("state", "accepted:KV18-01"),
                               ("state", "accepted:KV17-06")])




class LiveOverrideTest(unittest.TestCase):
    def test_live_executor_beats_a_stale_waiting_state(self) -> None:
        run = {"batches": {"V17": {"tickets": {
            "KV17-05": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "blocker": "WAIT-RESOURCE: mem"},
            "KV18-07": {"status": "CLAIMED", "state": "WAITING_RESOURCE", "blocker": "WAIT-RESOURCE: mem"}}}}}
        watch = {"tracked": {"KV17-05": {"terminal": False}, "KV18-07": {"terminal": True},
                             "KV17-05-review-spec": {"terminal": False}}}
        v.mark_live(run, watch)
        b = run["batches"]["V17"]
        self.assertEqual(v.display_state(b, "KV17-05"), ("run", "状态未更新"))
        self.assertEqual(v.display_state(b, "KV18-07"), ("res", ""))

if __name__ == "__main__":
    unittest.main()

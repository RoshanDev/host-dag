from __future__ import annotations

import argparse
import contextlib
import io
import importlib.util
import json
import sys
import tempfile
import signal
import unittest
from pathlib import Path
from unittest import mock

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import hostdag_cli_watch  # noqa: E402
import test_hostdag_host_watch as host_watch_tests  # noqa: E402

# Borrow the watcher harness methods without re-collecting its test class here.
_harness = vars(host_watch_tests.HostWatchTest)


def load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Project quota/model policy is deliberately outside this generic skill's sync.
quota = load("hostdag_quota", "host-dag-quota.py") if (SCRIPTS / "host-dag-quota.py").exists() else None
agent = load("hostdag_cli_agent", "host-dag-cli-agent.py")


def reading(percent: float) -> dict:
    return {"percent": percent}


@unittest.skipIf(quota is None, "project quota policy not synced")
class QuotaPolicyTest(unittest.TestCase):
    def test_codex_below_switch_line(self) -> None:
        sel = quota.select({"codex": reading(74.9), "grok": reading(10), "cursor": reading(10)})
        self.assertEqual((sel["status"], sel["executor"], sel["model"], sel["effort"]),
                         ("OK", "codex", "gpt-6-astra", "medium"))

    def test_codex_at_75_switches_to_claude(self) -> None:
        sel = quota.select({"codex": reading(75), "grok": reading(99), "cursor": reading(10)})
        self.assertEqual((sel["executor"], sel["model"]), ("claude", "opus[1m]"))
        self.assertEqual([s["executor"] for s in sel["skipped"]], ["codex", "grok", "cursor"])

    def test_retired_providers_are_not_fallbacks_even_with_quota(self) -> None:
        sel = quota.select({"codex": reading(100), "grok": reading(0), "cursor": reading(0),
                            "claude": {"fiveHour": 90, "weekly": 95}})
        self.assertEqual((sel["status"], sel["executor"]), ("WAIT", None))
        self.assertEqual([s["executor"] for s in sel["skipped"]], ["codex", "grok", "cursor", "claude"])

    def test_claude_near_limit_keeps_codex_until_exhausted(self) -> None:
        sel = quota.select({"codex": reading(90), "claude": {"fiveHour": 90, "weekly": 95}})
        self.assertEqual((sel["executor"], sel["model"]), ("codex", "gpt-6-astra"))

    def test_unreadable_active_provider_stops_instead_of_guessing(self) -> None:
        sel = quota.select({"codex": {"error": "usage HTTP 401"}, "grok": reading(0)})
        self.assertEqual((sel["status"], sel["blockedAt"]), ("UNREADABLE", "codex"))
        self.assertIn("401", sel["reason"])

    def test_unreadable_later_step_does_not_block_earlier_choice(self) -> None:
        sel = quota.select({"codex": reading(10), "grok": {"error": "x"}, "cursor": {"error": "y"}})
        self.assertEqual(sel["executor"], "codex")

    def test_blip_is_retried_before_counting_as_unreadable(self) -> None:
        answers = iter([{"error": "exit 1: wham/usage"}, {"percent": 60.0}])
        slept: list[float] = []
        reading = quota.with_retries(lambda: next(answers), sleep=slept.append)
        self.assertEqual((reading["percent"], reading["attempts"], slept), (60.0, 2, [5]))

    def test_persistent_error_stays_unreadable_after_all_retries(self) -> None:
        calls: list[int] = []
        reading = quota.with_retries(lambda: calls.append(1) or {"error": "down"}, sleep=lambda _s: None)
        self.assertEqual((reading["error"], reading["attempts"], len(calls)), ("down", 3, 3))

    def test_cursor_percent_prefers_provider_total(self) -> None:
        summary = {"individualUsage": {"plan": {"used": 40000, "limit": 40000, "totalPercentUsed": 34.5}}}
        self.assertEqual(quota.cursor_percent(summary), 34.5)
        self.assertEqual(quota.cursor_percent({"individualUsage": {"plan": {"used": 5, "limit": 20}}}), 25.0)
        self.assertIsNone(quota.cursor_percent({}))

    def test_cursor_reads_the_pool_of_its_executor_model(self) -> None:
        # Cursor bills Auto/Composer/Grok and third-party named models (Opus, Fable, GPT) from separate pools.
        self.assertEqual(quota.cursor_pool("claude-opus-5-5-medium"), "api")
        self.assertEqual(quota.cursor_pool("claude-fable-5-thinking-high"), "api")
        self.assertEqual(quota.cursor_pool("grok-4.7-xhigh"), "auto")
        self.assertEqual(quota.cursor_pool("composer-2"), "auto")
        summary = {"individualUsage": {"plan": {"used": 40000, "limit": 40000, "autoPercentUsed": 98.0,
                                                "apiPercentUsed": 5.17, "totalPercentUsed": 34.7}}}
        self.assertEqual(quota.cursor_percent(summary, "api"), 5.17)
        self.assertEqual(quota.cursor_percent(summary, "auto"), 98.0)
        self.assertIsNone(quota.cursor_percent({"individualUsage": {"plan": {"used": 5, "limit": 20}}}, "api"))

    def test_grok_nanosecond_expiry(self) -> None:
        self.assertTrue(quota._expired("2000-01-01T00:00:00.123456789Z"))
        self.assertFalse(quota._expired("2999-01-01T00:00:00.123456789Z"))


class CliAgentTest(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory(prefix="cliagent-")
        self.out = Path(self._td.name)
        self.addCleanup(self._td.cleanup)
        (self.out / "prompt.md").write_text("do the ticket")

    def supervise_with(self, script: str) -> dict:
        run = {"provider": "cursor", "model": "claude-opus-5-5-medium", "cwd": str(self.out),
               "promptFile": str(self.out / "prompt.md"), "unit": "u", "log": str(self.out / agent.LOG_FILE),
               "result": str(self.out / agent.RESULT_FILE)}
        (self.out / agent.RUN_FILE).write_text(json.dumps(run))
        with mock.patch.object(agent, "agent_argv", return_value=([sys.executable, "-c", script], None)):
            agent.cmd_supervise(argparse.Namespace(out=str(self.out)))
        return json.loads((self.out / agent.RESULT_FILE).read_text())

    def test_supervise_restores_callers_signal_handlers(self) -> None:
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        try:
            self.supervise_with("print('done')")
            for sig, handler in previous.items():
                self.assertIs(signal.getsignal(sig), handler)
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

    def test_completed_run_records_final_result(self) -> None:
        result = self.supervise_with(
            "import json;print(json.dumps({'type':'assistant'}));"
            "print(json.dumps({'type':'result','subtype':'success','is_error':False,'result':'REVIEW_READY','session_id':'s1'}))")
        self.assertEqual((result["status"], result["resultText"], result["sessionId"]),
                         ("COMPLETED", "REVIEW_READY", "s1"))

    def test_quota_error_is_its_own_status(self) -> None:
        result = self.supervise_with(
            "import json,sys;print(json.dumps({'type':'result','subtype':'error','is_error':True,"
            "'result':'You have hit your usage limit for this month'}));sys.exit(1)")
        self.assertEqual(result["status"], "QUOTA_EXHAUSTED")

    def test_quota_text_in_a_successful_answer_is_not_exhaustion(self) -> None:
        result = self.supervise_with(
            "import json;print(json.dumps({'type':'result','subtype':'success','is_error':False,"
            "'result':'Added a rate limit reason to the tooltip'}))")
        self.assertEqual(result["status"], "COMPLETED")

    def test_tmux_only_classifies_confirmed_quota_errors(self) -> None:
        for confirmed, text in [(True, "You've hit your limit · resets 2pm"),
                                (False, 'Unknown terminal after fixing quota handling')]:
            with self.subTest(confirmed=confirmed):
                log = self.out / agent.LOG_FILE
                log.write_text(json.dumps({'type': 'result', 'transport': 'tmux', 'is_error': True,
                    'subtype': 'error', 'result': text, 'quota_exhausted': confirmed,
                    'reason': 'quota_exhausted' if confirmed else 'unknown_terminal',
                    'process_exit_code': None}) + '\n')
                summary = agent.final_result(log)
                self.assertEqual(summary['quotaExhausted'], confirmed)
                self.assertIsNone(summary['processExitCode'])

    def test_crash_without_result_is_failed(self) -> None:
        result = self.supervise_with("import sys;sys.exit(2)")
        self.assertEqual((result["status"], result["exitCode"]), ("FAILED", 2))

    def test_argv_uses_headless_stream_json(self) -> None:
        argv, stdin = agent.agent_argv("cursor", "claude-opus-5-5-medium", "/w", "p")
        self.assertIn("--print" if "--print" in argv else "-p", argv)
        self.assertEqual(argv[argv.index("--model") + 1], "claude-opus-5-5-medium")
        self.assertEqual(argv[argv.index("--workspace") + 1], "/w")
        self.assertIsNone(stdin)
        argv, stdin = agent.agent_argv("claude", "opus", "/w", "p")
        self.assertEqual(argv[argv.index("--permission-mode") + 1], "bypassPermissions")
        self.assertEqual(stdin, "p")


class CliObserveTest(unittest.TestCase):
    def setUp(self) -> None:
        self._td = tempfile.TemporaryDirectory(prefix="cliobs-")
        self.out = Path(self._td.name)
        self.addCleanup(self._td.cleanup)
        self.run_file = self.out / "cli-run.json"
        self.run_file.write_text(json.dumps({"unit": "host-dag-cli-x", "status": "RUNNING",
                                             "result": str(self.out / "cli-result.json")}))
        self.rec = {"provider": "cursor", "run_file": str(self.run_file)}

    def test_running_while_unit_active(self) -> None:
        snap = hostdag_cli_watch.observe_record(self.rec, is_active=lambda _unit: True)
        self.assertEqual(snap["status"], "RUNNING")
        self.assertIsNone(hostdag_cli_watch.event_kind(snap["status"]))

    def test_lost_when_unit_gone_without_result(self) -> None:
        snap = hostdag_cli_watch.observe_record(self.rec, is_active=lambda _unit: False)
        self.assertEqual(hostdag_cli_watch.event_kind(snap["status"]), "failed")

    def test_quota_exhausted_needs_coordinator_attention(self) -> None:
        (self.out / "cli-result.json").write_text(json.dumps({"status": "QUOTA_EXHAUSTED", "exitCode": 1}))
        snap = hostdag_cli_watch.observe_record(self.rec, is_active=lambda _unit: False)
        self.assertEqual(hostdag_cli_watch.event_kind(snap["status"]), "attention")


class CliWatchIntegrationTest(unittest.TestCase):
    setUp = _harness['setUp']
    run_watch = _harness['run_watch']
    payload = _harness['payload']
    write_thread = _harness['write_thread']
    write_config = _harness['write_config']
    calls = _harness['calls']
    sent = _harness['sent']
    load_state = _harness['load_state']
    init_watch = _harness['init_watch']
    poll = _harness['poll']

    def test_completed_cursor_run_wakes_parent_without_thread_reads(self) -> None:
        self.init_watch()
        out = self.base / "KVF-02"
        out.mkdir()
        run_file = out / "cli-run.json"
        run_file.write_text(json.dumps({"unit": "host-dag-cli-none", "status": "RUNNING",
                                        "result": str(out / "cli-result.json")}))
        (out / "cli-result.json").write_text(json.dumps({
            "status": "COMPLETED", "exitCode": 0, "finishedAt": "t1", "resultText": "REVIEW_READY"}))
        self.write_thread("parent-1", "running", "busy")
        track = self.run_watch("--now", "1", "track-cli", "--ticket", "KVF-02", "--provider", "cursor",
                               "--run-file", str(run_file), "--evidence", str(out))
        self.assertEqual(track.returncode, 0, track.stderr)
        data = self.poll(now=2)
        event = data["events"][0]
        self.assertEqual((event["kind"], event["provider"], event["ticket"]), ("completed", "cursor", "KVF-02"))
        self.assertNotIn("thread", event)
        invoked = [item["argv"] for item in self.calls()]
        self.assertFalse(any("KVF-02" in " ".join(row) for row in invoked))
        second = self.poll(now=40)
        self.assertEqual(second.get("events") or [], [])

    def test_codex_track_replaces_a_previous_cli_registration(self) -> None:
        self.init_watch()
        run_file = self.base / "cli-run.json"
        run_file.write_text("{}")
        self.run_watch("track-cli", "--ticket", "KVF-03", "--provider", "claude", "--run-file", str(run_file))
        result = self.run_watch("track", "--ticket", "KVF-03", "--thread", "t-1")
        self.assertEqual(result.returncode, 0, result.stderr)
        rec = self.load_state()["tracked"]["KVF-03"]
        self.assertEqual(rec["thread"], "t-1")
        self.assertNotIn("run_file", rec)
        self.assertNotIn("provider", rec)


if __name__ == "__main__":
    unittest.main()


class DispatchRegistersExecutor(unittest.TestCase):
    """2026-10-22: `run` writes the new unit onto the ticket so the watcher tracks it."""

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.run_dir = root / "run"
        self.run_dir.mkdir()
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {
            "V17": {"tickets": {"T-1": {"status": "SUPERSEDED", "deps": []}}},
            "V18": {"tickets": {"T-1": {"status": "CLAIMED", "executor": "old-unit", "deps": []}}}}}))
        self.out = root / "out"
        self.prompt = root / "prompt.md"
        self.prompt.write_text("x")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def dispatch(self, *extra: str) -> dict:
        real_run = agent.subprocess.run

        def fake_run(argv, *a, **kw):
            if argv and argv[0] == "systemd-run":
                return mock.Mock(returncode=0, stderr="", stdout="")
            return real_run(argv, *a, **kw)
        out = io.StringIO()
        with mock.patch.object(agent.subprocess, "run", side_effect=fake_run), contextlib.redirect_stdout(out):
            rc = agent.main(["run", "--provider", "claude", "--ticket", "T-1", "--cwd", self.tmp.name,
                             "--model", "opus", "--prompt-file", str(self.prompt), "--out", str(self.out),
                             "--run", str(self.run_dir), *extra])
        self.assertEqual(rc, 0)
        return json.loads(out.getvalue().strip().splitlines()[-1])

    def ticket(self, batch: str) -> dict:
        return json.loads((self.run_dir / "run.json").read_text())["batches"][batch]["tickets"]["T-1"]

    def test_registers_unit_and_evidence_on_the_open_copy(self) -> None:
        res = self.dispatch()
        self.assertTrue(res["registration"]["registered"], res["registration"])
        self.assertEqual(res["registration"]["batch"], "V18")
        t = self.ticket("V18")
        self.assertEqual(t["executor"], res["unit"])
        self.assertEqual(t["evidence"], str(self.out.resolve() / "cli-run.json"))
        self.assertEqual(t["model"], "opus")
        self.assertNotIn("executor", self.ticket("V17"))

    def test_no_register_leaves_ticket(self) -> None:
        res = self.dispatch("--no-register")
        self.assertFalse(res["registration"]["registered"])
        self.assertEqual(self.ticket("V18")["executor"], "old-unit")

    def test_unknown_ticket_reports_but_dispatch_succeeds(self) -> None:
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {"V18": {"tickets": {}}}}))
        res = self.dispatch()
        self.assertFalse(res["registration"]["registered"])
        self.assertIn("not a ticket", res["registration"]["reason"])


class DispatchSideRunGuard(DispatchRegistersExecutor):
    def write_ticket(self, **rec) -> None:
        (self.run_dir / "run.json").write_text(json.dumps({"batches": {"V18": {"tickets": {
            "T-1": {"status": "CLAIMED", "executor": "host-dag-cli-claude-T-1-old", "deps": [], **rec}}}}}))

    def test_waiting_resource_with_holder_is_a_side_run(self) -> None:
        self.write_ticket(state="WAITING_RESOURCE", wait_on="host:build-1", wait_holder="kv18-08-soak.service")
        res = self.dispatch()
        self.assertFalse(res["registration"]["registered"])
        self.assertIn("WAITING_RESOURCE", res["registration"]["reason"])
        self.assertTrue(json.loads((self.out / "cli-run.json").read_text())["sideRun"])
        self.assertEqual(self.ticket("V18")["executor"], "host-dag-cli-claude-T-1-old")

    def test_live_current_executor_is_not_displaced(self) -> None:
        self.write_ticket(state="RUNNING")
        with mock.patch.object(agent, "unit_active", return_value=True):
            res = self.dispatch()
        self.assertFalse(res["registration"]["registered"])
        self.assertIn("still running", res["registration"]["reason"])

    def test_force_register_replaces_main_line(self) -> None:
        self.write_ticket(state="WAITING_RESOURCE", wait_on="host:build-1", wait_holder="soak")
        res = self.dispatch("--force-register")
        self.assertTrue(res["registration"]["registered"], res["registration"])

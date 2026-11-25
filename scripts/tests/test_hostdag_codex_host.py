"""Exercise the public CLI against a protocol peer, without model calls."""
import json
import fcntl
import hashlib
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from pathlib import Path

from websockets.sync.server import unix_serve


SCRIPT = Path(__file__).resolve().parents[1] / "host-dag-codex-host.py"


class NativeHostTests(unittest.TestCase):
    def setUp(self):
        # Unix socket paths have a small kernel limit; artifact roots can be long.
        self.tmp = tempfile.TemporaryDirectory(prefix="host-dag-native-", dir="/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.socket = str(Path(self.tmp.name) / "host.sock")
        self.calls = []
        self.thread = {
            "id": "worker", "historyMode": "legacy", "cwd": self.tmp.name,
            "status": {"type": "idle"}, "turns": [{
                "id": "turn-1", "status": "completed", "items": [
                    {"id": "a1", "type": "agentMessage", "phase": "final_answer", "text": "完成 café"},
                ],
            }],
        }
        self.respond = self.default_response
        self.server = unix_serve(self.handle, self.socket, compression=None)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.addCleanup(self.stop)

    def stop(self):
        self.server.shutdown()
        self.worker.join(timeout=5)

    def handle(self, connection):
        for raw in connection:
            request = json.loads(raw)
            self.calls.append(request)
            if "id" not in request:
                continue
            # Interleave a notification: clients cannot assume one receive = response.
            connection.send(json.dumps({"method": "thread/status/changed", "params": {"threadId": "other"}}))
            result = self.respond(request)
            if result is None:
                connection.close()
                return
            connection.send(json.dumps({"id": request["id"], **result}))

    def default_response(self, request):
        if request["method"] == "initialize":
            return {"result": {"platformOs": "linux"}}
        if request["method"] == "thread/read":
            return {"result": {"thread": self.thread}}
        if request["method"] == "thread/turns/list":
            # Page whatever this peer's (possibly overridden) read serves, newest first.
            params = request["params"]
            self.assertEqual((params["itemsView"], params["sortDirection"]), ("full", "desc"))
            read = self.respond({"method": "thread/read", "params": {"threadId": params["threadId"]}})
            if "result" not in read:
                return read
            turns = list(reversed(read["result"]["thread"].get("turns") or []))
            start = int(params.get("cursor") or 0)
            end = start + params["limit"]
            return {"result": {"data": turns[start:end], "nextCursor": str(end) if end < len(turns) else None}}
        self.fail("unexpected RPC: " + request["method"])

    def cli(self, *args, expected=0):
        env = dict(os.environ, HOST_DAG_CODEX_SOCKET=self.socket,
                   HOST_DAG_CODEX_LOCK_DIR=self.tmp.name, HOST_DAG_TOKEN_GUARD_STATE=str(Path(self.tmp.name)/'guard-index'))
        result = subprocess.run([sys.executable, str(SCRIPT), *args],
                                env=env, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return json.loads(result.stdout)

    def test_service_tier_payloads_and_dispatch_receipts(self):
        original = self.respond

        def response(request):
            if request["method"] == "thread/start":
                return {"result": {"thread": self.thread, "cwd": self.tmp.name,
                                   "model": "m", "reasoningEffort": "medium",
                                   "sandbox": {"type": "dangerFullAccess"},
                                   "approvalPolicy": "never", "approvalsReviewer": "user"}}
            if request["method"] == "thread/resume":
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                return {"result": {"turn": {"id": "tier-turn", "status": "inProgress"}}}
            return original(request)

        self.respond = response
        for action in ("start", "send"):
            for tier in (None, "priority", "default"):
                with self.subTest(action=action, tier=tier):
                    self.calls.clear()
                    receipt = Path(self.tmp.name) / "cli-run.json"
                    argv = ["thread", action]
                    if action == "start":
                        argv += ["--cwd", self.tmp.name, "--model", "m", "--effort", "medium",
                                 "--sandbox", "danger-full-access", "--approval-policy", "never"]
                    else:
                        argv += ["worker"]
                    if tier is not None:
                        argv += ["--service-tier", tier]
                    result = self.cli(*argv, "--message", "probe", "--run-file", str(receipt))
                    self.assertEqual(json.loads(receipt.read_text()), result)
                    expected = {} if tier is None else {"serviceTier": tier}
                    self.assertEqual({k: v for k, v in result.items() if k == "serviceTier"}, expected)
                    mutations = [c for c in self.calls if c["method"] in ("thread/start", "turn/start")]
                    self.assertEqual([c["method"] for c in mutations],
                                     ["thread/start", "turn/start"] if action == "start" else ["turn/start"])
                    for call in mutations:
                        params = call["params"]
                        self.assertEqual({k: v for k, v in params.items() if k == "serviceTier"}, expected)
                        self.assertNotIn("service_tier", params.get("config", {}))
                    for call in self.calls:
                        if call["method"] not in ("thread/start", "turn/start"):
                            self.assertNotIn("serviceTier", call.get("params", {}))

    def test_run_file_write_failure_preserves_success_without_retry_signal(self):
        original = self.respond

        def response(request):
            if request["method"] == "thread/start":
                return {"result": {"thread": self.thread, "cwd": self.tmp.name,
                                   "model": "m", "reasoningEffort": "medium",
                                   "sandbox": {"type": "dangerFullAccess"},
                                   "approvalPolicy": "never", "approvalsReviewer": "user"}}
            if request["method"] == "thread/resume":
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                return {"result": {"turn": {"id": "accepted-turn", "status": "inProgress"}}}
            return original(request)

        self.respond = response
        # A directory is deterministically unwritable as a file, even under root.
        receipt = Path(self.tmp.name) / "cli-run.json"
        receipt.mkdir()
        for action in ("start", "send"):
            with self.subTest(action=action):
                self.calls.clear()
                argv = ["thread", action]
                if action == "start":
                    argv += ["--cwd", self.tmp.name, "--model", "m", "--effort", "medium",
                             "--sandbox", "danger-full-access", "--approval-policy", "never"]
                else:
                    argv += ["worker"]
                env = dict(os.environ, HOST_DAG_CODEX_SOCKET=self.socket,
                           HOST_DAG_CODEX_LOCK_DIR=self.tmp.name, HOST_DAG_TOKEN_GUARD_STATE=str(Path(self.tmp.name)/'guard-index'))
                proc = subprocess.run([sys.executable, str(SCRIPT), *argv,
                                       "--service-tier", "priority", "--message", "probe",
                                       "--run-file", str(receipt)],
                                      env=env, capture_output=True, text=True, timeout=10)
                self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
                result = json.loads(proc.stdout)  # Exactly one complete success receipt.
                self.assertEqual(result["threadId"], "worker")
                self.assertEqual(result["turn"], {"id": "accepted-turn", "status": "inProgress"})
                self.assertEqual(result["serviceTier"], "priority")
                self.assertNotIn("error", result)
                if action == "start":
                    self.assertEqual(result["thread"], self.thread)
                warning = json.loads(proc.stderr)
                self.assertEqual(warning["warning"]["code"], "RUN_FILE_WRITE_FAILED")
                self.assertEqual(warning["warning"]["path"], str(receipt))
                self.assertTrue(warning["dispatchSucceeded"])
                self.assertFalse(warning["retryDispatch"])
                self.assertEqual((warning["threadId"], warning["turnId"], warning["serviceTier"]),
                                 ("worker", "accepted-turn", "priority"))
                self.assertNotIn("RUNTIME_UNREACHABLE", proc.stdout + proc.stderr)
                methods = [c["method"] for c in self.calls]
                self.assertEqual(methods.count("turn/start"), 1)
                self.assertEqual(methods.count("thread/start"), int(action == "start"))

    def test_invalid_service_tier_is_rejected_before_connecting(self):
        for action in ("start", "send"):
            argv = ["thread", action]
            if action == "start":
                argv += ["--cwd", self.tmp.name, "--model", "m", "--effort", "medium",
                         "--sandbox", "danger-full-access", "--approval-policy", "never"]
            else:
                argv += ["worker"]
            result = subprocess.run([sys.executable, str(SCRIPT), "--socket", self.socket,
                                     *argv, "--service-tier", "fast", "--message", "probe"],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
            self.assertIn("invalid choice", result.stderr)
            self.assertEqual(self.calls, [])

    def test_completed_result_and_turn_identity_survive_client_reconnect(self):
        first = self.cli("thread", "read", "worker")
        self.assertEqual(first["status"], "completed")
        self.assertEqual(first["result"]["text"], "完成 café")
        self.assertEqual(first["result"]["turnId"], "turn-1")
        self.assertEqual(first["result"]["availability"], "available")
        self.assertEqual(self.cli("thread", "read", "worker"), first)

    def test_account_rate_limits_is_a_read_only_passthrough(self):
        limits = {"rateLimits": {"primary": {"usedPercent": 37, "windowDurationMins": 10080, "resetsAt": 1}}}

        def respond(request):
            if request["method"] == "account/rateLimits/read":
                return {"result": limits}
            return self.default_response(request)

        self.respond = respond
        self.assertEqual(self.cli("account", "rate-limits"), limits)
        methods = [c.get("method") for c in self.calls]
        self.assertIn("account/rateLimits/read", methods)
        self.assertFalse({"turn/start", "thread/start", "thread/goal/set"} & set(methods))

    def test_reads_never_hydrate_full_history(self):
        self.thread["turns"] = [{"id": f"turn-{n}", "status": "completed", "items": [
            {"type": "userMessage", "id": f"u{n}", "content": [{"type": "text", "text": f"m{n}"}]}]}
            for n in range(12)]
        self.assertEqual(self.cli("thread", "read", "worker")["result"]["turnId"], "turn-11")
        page = self.cli("thread", "read", "worker", "--view", "messages", "--limit", "3", "--cursor", "6")
        self.assertEqual([m["text"] for m in page["messages"]], ["m5", "m4", "m3"])
        self.assertEqual(page["nextCursor"], "9")
        reads = [c["params"] for c in self.calls if c.get("method") == "thread/read"]
        self.assertTrue(reads and all(r["includeTurns"] is False for r in reads))
        # The status read fences one turn; the receipt page stops after two of three pages.
        pages = [c["params"]["limit"] for c in self.calls if c.get("method") == "thread/turns/list"]
        self.assertEqual(pages, [1, 1, 5, 5])

    def test_turn_finishing_between_calls_is_reread_not_unknown(self):
        self.thread["turns"][-1]["status"] = "inProgress"
        original = self.respond
        def response(request):
            # The turn completes after its page is served but before the status read.
            if request["method"] == "thread/read" and request["params"].get("includeTurns") is False:
                result = original(request)
                self.thread["turns"][-1]["status"] = "completed"
                return result
            return original(request)
        self.respond = response
        self.assertEqual(self.cli("thread", "read", "worker")["status"], "completed")

    def test_busy_send_does_not_steer_or_resume_the_parent(self):
        self.thread["status"] = {"type": "active", "activeFlags": []}
        result = self.cli("thread", "send", "worker", "--message", "notice", expected=1)
        self.assertEqual(result["error"]["code"], "THREAD_BUSY")
        self.assertFalse(any(c["method"] in {"turn/start", "thread/resume"} for c in self.calls))

    def reply(self, **kwargs):
        return self.cli('thread', 'send', 'worker', '--steer-if-busy', '--reply-to', 'q-1',
                        '--decision-version', '2', '--ticket', 'T', '--message', 'decision body', **kwargs)

    def activate(self):
        self.thread['status'] = {'type': 'active', 'activeFlags': []}
        self.thread['turns'][-1]['status'] = 'inProgress'

    def test_reply_steers_active_turn_with_expected_id_and_marker(self):
        self.activate()
        original = self.respond
        self.respond = lambda r: {'result': {'turnId': 'turn-1'}} if r['method'] == 'turn/steer' else original(r)
        result = self.reply()
        self.assertEqual(result['delivery'], {'status': 'ACCEPTED', 'method': 'steer', 'turnId': 'turn-1'})
        mutations = [c for c in self.calls if c['method'] in {'turn/steer', 'turn/start', 'thread/resume'}]
        self.assertEqual([c['method'] for c in mutations], ['turn/steer'])
        self.assertEqual(mutations[0]['params'], {'threadId': 'worker', 'expectedTurnId': 'turn-1',
            'input': [{'type': 'text', 'text': '[reply-to q-1 decision-v2 ticket=T]\ndecision body'}]})

    def test_reply_idle_still_resumes_and_starts_once(self):
        original = self.respond
        def response(request):
            if request['method'] == 'thread/resume':
                return {'result': {'thread': self.thread}}
            if request['method'] == 'turn/start':
                return {'result': {'turn': {'id': 'turn-2', 'status': 'inProgress'}}}
            return original(request)
        self.respond = response
        self.assertEqual(self.reply()['delivery'], {'status': 'ACCEPTED', 'method': 'start', 'turnId': 'turn-2'})
        self.assertEqual([c['method'] for c in self.calls if c['method'] in
                         {'turn/steer', 'turn/start', 'thread/resume'}], ['thread/resume', 'turn/start'])

    def test_reply_turn_change_returns_without_any_mutation(self):
        for change in ('completed', 'replaced'):
            with self.subTest(change=change):
                self.calls.clear()
                self.activate()
                self.thread['turns'][-1]['id'] = 'turn-1'
                original = self.default_response
                reads = 0
                def response(request):
                    nonlocal reads
                    if request['method'] == 'thread/read' and request['params'].get('includeTurns') is False:
                        reads += 1
                        if reads == 2:
                            if change == 'completed':
                                self.thread['status'] = {'type': 'idle'}
                                self.thread['turns'][-1]['status'] = 'completed'
                            else:
                                self.thread['turns'][-1]['id'] = 'turn-2'
                    return original(request)
                self.respond = response
                self.assertEqual(self.reply(expected=1)['error']['code'], 'TURN_CHANGED')
                self.assertFalse(any(c['method'] in {'turn/start', 'turn/steer', 'thread/resume'} for c in self.calls))

    def test_reply_read_errors_and_unstable_busy_do_not_steer(self):
        for code in ('TIMEOUT', 'UNKNOWN_STATUS', 'permission denied', 'unstable-busy'):
            with self.subTest(code=code):
                self.calls.clear()
                self.thread['status'] = {'type': 'active' if code == 'unstable-busy' else 'systemError'}
                self.thread['turns'][-1]['status'] = 'completed'
                def response(request):
                    if code in {'TIMEOUT', 'permission denied'} and request['method'] == 'thread/read':
                        return {'error': {'code': -32000, 'message': code}}
                    return self.default_response(request)
                self.respond = response
                result = self.reply(expected=1)
                self.assertIn(result['error']['code'], {'NATIVE_RPC_ERROR', 'UNKNOWN_STATUS', 'THREAD_BUSY'})
                self.assertFalse(any(c['method'] in {'turn/start', 'turn/steer', 'thread/resume'} for c in self.calls))

    def test_reply_lost_steer_response_never_falls_back_to_start(self):
        self.activate()
        original = self.respond
        self.respond = lambda r: None if r['method'] == 'turn/steer' else original(r)
        self.assertEqual(self.reply(expected=1)['error']['code'], 'RUNTIME_UNREACHABLE')
        self.assertEqual(sum(c['method'] == 'turn/steer' for c in self.calls), 1)
        self.assertFalse(any(c['method'] in {'turn/start', 'thread/resume'} for c in self.calls))

    def test_reply_usage_rejected_before_connect(self):
        for flags in (['--steer-if-busy'], ['--steer-if-busy', '--reply-to', 'q'],
                      ['--steer-if-busy', '--decision-version', '1']):
            result = self.cli('thread', 'send', 'worker', '--message', 'x', *flags, expected=1)
            self.assertEqual(result['error']['code'], 'PROTOCOL_ERROR')
        self.assertEqual(self.calls, [])

    def test_reply_competing_writer_never_steers(self):
        self.activate()
        key = hashlib.sha256((self.socket + '\0worker').encode()).hexdigest()
        with (Path(self.tmp.name)/(key+'.lock')).open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertEqual(self.reply(expected=1)['error']['code'], 'THREAD_BUSY')
        self.assertFalse(any(c['method'] in {'turn/start', 'turn/steer', 'thread/resume'} for c in self.calls))

    def test_reply_steer_native_error_never_retries_or_starts(self):
        self.activate()
        original = self.respond
        self.respond = lambda r: {'error': {'code': -32000, 'message': 'permission denied'}} \
            if r['method'] == 'turn/steer' else original(r)
        self.assertEqual(self.reply(expected=1)['error']['code'], 'NATIVE_RPC_ERROR')
        self.assertEqual(sum(c['method'] == 'turn/steer' for c in self.calls), 1)
        self.assertFalse(any(c['method'] in {'turn/start', 'thread/resume'} for c in self.calls))

    def replay_rpc_boundary_queue(self, change):
        from test_hostdag_send_when_idle import Clock, FAKE
        import hostdag_send_when_idle as sender
        root = Path(self.tmp.name)/change
        run = root/'run'
        run.mkdir(parents=True)
        (run/'run.json').write_text(json.dumps({'batches': {}}))
        (run/'watch.json').write_text(json.dumps({'parent_thread': 'worker'}))
        fake = root/'fake'
        fake.mkdir()
        message = root/'message.md'
        message.write_text('same coordinator decision')
        clock = Clock()
        self.activate()
        rejected = False
        landed = []
        original = self.respond
        def response(request):
            nonlocal rejected
            method = request['method']
            if method == 'turn/steer' and not rejected:
                rejected = True
                if change == 'ended':
                    self.thread['status'] = {'type': 'idle'}
                    self.thread['turns'][-1]['status'] = 'completed'
                    error = 'no active turn to steer'
                else:
                    self.thread['turns'][-1]['id'] = 'turn-2'
                    error = 'expected active turn id `turn-1` but found `turn-2`'
                return {'error': {'code': -32600, 'message': error}}
            if method == 'thread/resume':
                return {'result': {'thread': self.thread}}
            if method in {'turn/start', 'turn/steer'}:
                landed.append((method, request['params']))
                if method == 'turn/start':
                    return {'result': {'turn': {'id': 'new-start', 'status': 'inProgress'}}}
                return {'result': {'turnId': 'turn-2'}}
            return original(request)
        self.respond = response
        common = ['--run', str(run), '--host', str(FAKE), '--send-timeout', '3', '--read-timeout', '3']
        send = [*common, '--steer-if-busy', '--reply-to', 'q', '--decision-version', '1', '--ticket', 'T',
                'worker', str(message)]
        env = {'FAKE_HOST_DIR': str(fake), 'FAKE_NATIVE_SOCKET': self.socket,
               'HOST_DAG_CODEX_LOCK_DIR': self.tmp.name, 'HOST_DAG_TOKEN_GUARD_STATE': str(root/'guard'),
               'HOST_DAG_RUN': str(run), 'HOST_DAG_SEND_NOTIFY_CMD': 'true'}
        def next_drain(delay):
            raise sender.LeaveQueued('test: next retry belongs to the drain')
        with patch.dict(os.environ, env), patch('hostdag_send_retry.time.time', clock.time):
            rc = sender.main(send, sleep=next_drain, clock=clock.time)
            self.assertTrue(rejected, 'replay must reach the native steer refusal, not a setup failure')
            self.assertEqual(rc, sender.EX_QUEUED, 'F1: explicit not-submitted RPC refusal left retry queue')
            paths = list((run/'control/parent-tools/send-when-idle-queue').glob('*.json'))
            self.assertEqual(len(paths), 1)
            before = json.loads(paths[0].read_text())
            self.assertEqual(before['status'], 'queued')
            self.assertEqual(landed, [])
            self.assertEqual(sum(c['method'] == 'turn/steer' for c in self.calls), 1)
            clock.now += 1
            self.assertEqual(sender.main([*common, '--drain'], clock=clock.time), 0)
            after = json.loads(paths[0].read_text())
            self.assertEqual((after['key'], after['reply']), (before['key'], before['reply']))
            self.assertEqual(after['status'], 'delivered')
            self.assertEqual(after['attempts_total'], 2)
            expected = 'turn/start' if change == 'ended' else 'turn/steer'
            self.assertEqual([method for method, _ in landed], [expected])
            self.assertEqual(after['delivery']['method'], 'start' if change == 'ended' else 'steer')
            if change != 'ended':
                self.assertEqual(landed[0][1]['expectedTurnId'], 'turn-2')
            self.assertTrue(landed[0][1]['input'][0]['text'].startswith('[reply-to q decision-v1 ticket=T]\n'))
            self.assertEqual(sender.main([*common, '--drain'], clock=clock.time), 0)
            self.assertEqual(len(landed), 1)

    def test_F1_rpc_turn_ended_stays_queued_then_drain_starts(self):
        self.replay_rpc_boundary_queue('ended')

    def test_F1_rpc_turn_replaced_stays_queued_then_drain_steers_new_id(self):
        self.replay_rpc_boundary_queue('replaced')

    def test_F1_only_exact_steer_not_submitted_errors_are_retryable(self):
        cases = [(-32600, 'permission denied'), (-32603, 'no active turn to steer'),
                 (-32600, 'prefix: no active turn to steer'),
                 (-32600, 'expected active turn id `turn-1` but found `turn-2`; permission denied')]
        self.activate()
        for code, message in cases:
            with self.subTest(code=code, message=message):
                self.calls.clear()
                original = self.default_response
                self.respond = lambda r: {'error': {'code': code, 'message': message}} \
                    if r['method'] == 'turn/steer' else original(r)
                self.assertEqual(self.reply(expected=1)['error']['code'], 'NATIVE_RPC_ERROR')
                self.assertEqual(sum(c['method'] == 'turn/steer' for c in self.calls), 1)
                self.assertFalse(any(c['method'] == 'turn/start' for c in self.calls))
        # Even the exact text is not retryable on another RPC method.
        self.thread['status'] = {'type': 'idle'}
        self.thread['turns'][-1]['status'] = 'completed'
        self.respond = lambda r: {'error': {'code': -32600, 'message': 'no active turn to steer'}} \
            if r['method'] == 'thread/resume' else self.default_response(r)
        self.assertEqual(self.reply(expected=1)['error']['code'], 'NATIVE_RPC_ERROR')

    def test_completed_read_does_not_hide_native_send_errors(self):
        for method in ("thread/resume", "turn/start"):
            for message in ("Thread already has an active writer", "native dispatch failed"):
                with self.subTest(method=method, message=message):
                    self.calls.clear()
                    def response(request):
                        if request["method"] == method:
                            return {"error": {"code": -32000, "message": message}}
                        if request["method"] == "thread/resume":
                            return {"result": {"thread": self.thread}}
                        return self.default_response(request)
                    self.respond = response
                    self.assertEqual(self.cli("thread", "read", "worker")["status"], "completed")
                    result = self.cli("thread", "send", "worker", "--message", "saved notice", expected=1)
                    self.assertEqual(result["error"]["code"], "NATIVE_RPC_ERROR")
                    self.assertIn(message, result["error"]["message"])
                    if method == "thread/resume":
                        self.assertFalse(any(c["method"] == "turn/start" for c in self.calls))

    def test_late_page_for_a_cannot_be_combined_with_b_status(self):
        original = self.respond
        changed = False
        def response(request):
            nonlocal changed
            if request["method"] == "thread/read" and request["params"].get("includeTurns") is False and not changed:
                changed = True
                self.thread["turns"].append({"id": "turn-B", "status": "inProgress", "items": []})
                self.thread["status"] = {"type": "active"}
            return original(request)
        self.respond = response
        result = self.cli("thread", "read", "worker")
        self.assertEqual((result["status"], result["result"]["turnId"]), ("running", "turn-B"))

    def test_send_rechecks_ticket_version_after_resume(self):
        run = Path(self.tmp.name) / "run.json"
        data = {"batches": {"B": {"tickets": {"T": {
            "state": "RUNNING", "status": "CLAIMED", "executor": "worker", "updated_at": "version-A",
        }}}}}
        run.write_text(json.dumps(data))
        context = Path(self.tmp.name) / "dispatch-context.json"
        context.write_text(json.dumps({"run_receipt": str(run), "batch": "B", "ticket": "T",
                                       "updated_at": "version-A", "thread": "worker"}))
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                data["batches"]["B"]["tickets"]["T"]["updated_at"] = "version-B"
                run.write_text(json.dumps(data))
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                return {"result": {"turn": {"id": "wrong", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--message", "queued",
                          "--dispatch-context", str(context), expected=1)
        self.assertEqual(result["error"]["code"], "DISPATCH_STALE")
        self.assertFalse(any(c["method"] == "turn/start" for c in self.calls))

    def test_incremental_send_verdict_is_delivered_once_even_without_a_new_track(self):
        from unittest import mock
        from test_hostdag_dispatch_receipts import load_watch
        root = Path(self.tmp.name)
        review = root / "review-incremental"
        review.mkdir()
        run = root / "run.json"
        run.write_text(json.dumps({"batches": {"B": {"tickets": {"T": {
            "state": "REVIEWING", "status": "CLAIMED", "evidence": str(root),
            "updated_at": "2020-01-01T00:00:00+00:00",
        }}}}}))
        def response(request):
            if request["method"] == "thread/resume":
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                (review / "verdict.json").write_text(json.dumps({"verdict": "PASS", "candidate": "abcdef123"}))
                return {"result": {"turn": {"id": "incremental", "status": "inProgress"}}}
            return self.default_response(request)
        self.respond = response
        self.cli("thread", "send", "worker", "--message", "incremental review")
        watch = load_watch()
        state = {"state_path": str(root / "watch.json"), "run_receipt": str(run),
                 "parent_thread": "parent", "tracked": {}, "pending_events": []}
        with mock.patch.object(watch, "run_host", return_value={"status": "running"}), \
                mock.patch.object(watch, "alert"), mock.patch.object(watch, "idle_ticket_events", return_value=[]):
            self.assertEqual(watch.poll_once(state, 100)["send"], "held_busy")
        self.assertEqual([e["status"] for e in state["pending_events"]], ["review-verdict"])
        state = json.loads((root / "watch.json").read_text())
        with mock.patch.object(watch, "run_host", return_value={"status": "completed"}) as host, \
                mock.patch.object(watch, "alert"), mock.patch.object(watch, "idle_ticket_events", return_value=[]):
            self.assertEqual(watch.poll_once(state, 101)["send"], "delivered")
            messages = [c.args[1][-1] for c in host.call_args_list if c.args[1][:2] == ["thread", "send"]]
            self.assertEqual(len(messages), 1)
            self.assertIn("PASS", messages[0])
            self.assertIsNone(watch.poll_once(state, 102)["send"])
        # The watcher notifies; only the coordinator may accept the ticket.
        self.assertEqual(json.loads(run.read_text())["batches"]["B"]["tickets"]["T"]["state"], "REVIEWING")

    def test_queued_send_rechecks_freeze_authorization_and_turn_at_submission(self):
        root = Path(self.tmp.name)
        run, context = root / "run.json", root / "context.json"
        freeze = root / "control/locks/board-freeze.json"
        freeze.parent.mkdir(parents=True)
        for change, code in (("freeze", "DISPATCH_FROZEN"), ("cancel", "DISPATCH_UNAUTHORIZED"),
                             ("turn", "TURN_CHANGED")):
            with self.subTest(change=change):
                self.calls.clear()
                freeze.unlink(missing_ok=True)
                self.thread["turns"] = [{"id": "A", "status": "completed", "items": []}]
                ticket = {"state": "RUNNING", "status": "CLAIMED", "executor": "worker", "updated_at": "v1"}
                data = {"batches": {"B": {"tickets": {"T": ticket}}}}
                run.write_text(json.dumps(data))
                context.write_text(json.dumps({"run_receipt": str(run), "batch": "B", "ticket": "T",
                                               "updated_at": "v1", "thread": "worker", "turn_id": "A"}))
                def response(request):
                    if request["method"] == "thread/resume":
                        if change == "freeze":
                            freeze.write_text(json.dumps({"exempt": []}))
                        elif change == "cancel":
                            ticket["state"] = "CANCELED"
                            run.write_text(json.dumps(data))
                        else:
                            self.thread["turns"] = [{"id": "B", "status": "completed", "items": []}]
                        return {"result": {"thread": self.thread}}
                    return self.default_response(request)
                self.respond = response
                result = self.cli("thread", "send", "worker", "--message", "queued A",
                                  "--dispatch-context", str(context), expected=1)
                self.assertEqual(result["error"]["code"], code)
                self.assertFalse(any(c["method"] == "turn/start" for c in self.calls))

    def test_send_rejoins_stored_thread_without_changing_its_settings(self):
        self.thread["status"] = {"type": "notLoaded"}
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                self.assertEqual(request["params"], {"threadId": "worker"})
                return {"result": {"thread": self.thread, "model": "retained-model"}}
            if request["method"] == "turn/start":
                self.assertEqual(request["params"], {"threadId": "worker", "input": [{"type": "text", "text": "notice"}]})
                return {"result": {"turn": {"id": "turn-2", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--message", "notice")
        self.assertEqual(result["turn"]["id"], "turn-2")

    def test_send_effort_override_only_rides_on_turn_start(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                self.assertEqual(request["params"], {"threadId": "worker"})
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                self.assertEqual(request["params"], {
                    "threadId": "worker", "input": [{"type": "text", "text": "notice"}], "effort": "medium",
                })
                return {"result": {"turn": {"id": "turn-2", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--effort", "medium", "--message", "notice")
        self.assertEqual(result["turn"]["id"], "turn-2")

    def test_send_writable_roots_ride_on_turn_start_as_workspace_policy(self):
        extra = Path(self.tmp.name) / "repo.git"
        extra.mkdir()
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                self.assertEqual(request["params"], {
                    "threadId": "worker", "input": [{"type": "text", "text": "notice"}],
                    "sandboxPolicy": {"type": "workspaceWrite", "writableRoots": [str(extra)]},
                })
                return {"result": {"turn": {"id": "turn-2", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--writable-root", str(extra), "--message", "notice")
        self.assertEqual(result["turn"]["id"], "turn-2")

    def test_start_writable_roots_need_workspace_write(self):
        extra = Path(self.tmp.name) / "repo.git"
        extra.mkdir()
        original = self.respond
        def response(request):
            if request["method"] == "thread/start":
                return {"result": {"thread": self.thread, "cwd": self.tmp.name, "model": "m", "reasoningEffort": "medium", "sandbox": {"type": "workspaceWrite"}, "approvalPolicy": "on-request", "approvalsReviewer": "auto_review"}}
            if request["method"] == "turn/start":
                self.assertEqual(request["params"]["sandboxPolicy"], {"type": "workspaceWrite", "writableRoots": [str(extra)]})
                return {"result": {"turn": {"id": "new-turn", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "start", "--cwd", self.tmp.name, "--model", "m", "--effort", "medium", "--sandbox", "workspace-write", "--approval-policy", "on-request", "--approvals-reviewer", "auto_review", "--writable-root", str(extra), "--message", "probe")
        self.assertEqual(result["turn"]["id"], "new-turn")
        refused = self.cli("thread", "start", "--cwd", self.tmp.name, "--model", "m", "--effort", "medium", "--sandbox", "read-only", "--approval-policy", "never", "--writable-root", str(extra), "--message", "probe", expected=1)
        self.assertEqual(refused["error"]["code"], "PROTOCOL_ERROR")
        self.assertFalse(any(c["method"] == "thread/start" for c in self.calls[-3:]))

    def test_start_full_access_matches_the_global_codex_setting(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/start":
                self.assertEqual((request["params"]["sandbox"], request["params"]["approvalPolicy"]), ("danger-full-access", "never"))
                return {"result": {"thread": self.thread, "cwd": self.tmp.name, "model": "m", "reasoningEffort": "medium", "sandbox": {"type": "dangerFullAccess"}, "approvalPolicy": "never", "approvalsReviewer": "user"}}
            if request["method"] == "turn/start":
                self.assertNotIn("sandboxPolicy", request["params"])
                return {"result": {"turn": {"id": "new-turn", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "start", "--cwd", self.tmp.name, "--model", "m", "--effort", "medium", "--sandbox", "danger-full-access", "--approval-policy", "never", "--message", "probe")
        self.assertEqual(result["sandbox"]["type"], "dangerFullAccess")

    def test_send_can_lift_a_scoped_thread_to_full_access(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                self.assertEqual(request["params"], {
                    "threadId": "worker", "input": [{"type": "text", "text": "notice"}],
                    "sandboxPolicy": {"type": "dangerFullAccess"}, "approvalPolicy": "never",
                })
                return {"result": {"turn": {"id": "turn-2", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--sandbox", "danger-full-access", "--message", "notice")
        self.assertEqual(result["turn"]["id"], "turn-2")

    def test_interruption_requires_the_requested_turn_to_be_terminal(self):
        self.thread["status"] = {"type": "active", "activeFlags": []}
        self.thread["turns"][-1]["status"] = "inProgress"
        original = self.respond
        def response(request):
            if request["method"] == "turn/interrupt":
                self.assertEqual(request["params"], {"threadId": "worker", "turnId": "turn-1"})
                self.thread["status"] = {"type": "idle"}
                self.thread["turns"][-1]["status"] = "interrupted"
                return {"result": {}}
            return original(request)
        self.respond = response
        self.assertEqual(self.cli("thread", "cancel", "worker", "--turn-id", "turn-1")["status"], "cancelled")

    def test_unknown_and_failed_status_cannot_be_completed(self):
        self.thread["turns"][-1]["status"] = "failed"
        self.assertEqual(self.cli("thread", "read", "worker")["status"], "failed")
        self.thread["status"] = {"type": "new-upstream-status"}
        self.assertEqual(self.cli("thread", "read", "worker")["status"], "unknown")

    def test_start_pins_history_and_returns_effective_execution_binding(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/start":
                params = request["params"]
                self.assertEqual(params["historyMode"], "legacy")
                self.assertFalse(params["allowProviderModelFallback"])
                self.assertEqual(params["config"]["model_reasoning_effort"], "medium")
                # 2026-10-28: the user allows executors to use their own Chrome.
                self.assertNotIn("plugins", params["config"])
                self.assertNotIn("mcp_servers", params["config"])
                return {"result": {"thread": self.thread, "cwd": self.tmp.name, "model": "gpt-6-astra", "reasoningEffort": "medium", "sandbox": {"type": "readOnly"}, "approvalPolicy": "never", "approvalsReviewer": "user"}}
            if request["method"] == "turn/start":
                return {"result": {"turn": {"id": "new-turn", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "start", "--cwd", self.tmp.name, "--model", "gpt-6-astra", "--effort", "medium", "--sandbox", "read-only", "--approval-policy", "never", "--message", "probe")
        self.assertEqual(result["threadId"], "worker")
        self.assertEqual(result["sandbox"]["type"], "readOnly")

    def test_unsupported_history_fails_without_resume_or_conversion(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/read":
                return {"error": {"code": -32601, "message": "list_turns is not supported yet"}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "read", "worker", expected=1)
        self.assertEqual(result["error"]["code"], "NATIVE_RPC_ERROR")
        self.assertFalse(any(c["method"] == "thread/resume" for c in self.calls))

    def test_stale_cancel_cannot_interrupt_a_new_turn(self):
        result = self.cli("thread", "cancel", "worker", "--turn-id", "old-turn", expected=1)
        self.assertEqual(result["error"]["code"], "TURN_CHANGED")
        self.assertFalse(any(c["method"] == "turn/interrupt" for c in self.calls))

    def test_interrupt_ack_alone_is_not_a_confirmed_stop(self):
        self.thread["status"] = {"type": "active", "activeFlags": []}
        self.thread["turns"][-1]["status"] = "inProgress"
        original = self.respond
        self.respond = lambda r: {"result": {}} if r["method"] == "turn/interrupt" else original(r)
        result = self.cli("--timeout", "0.2", "thread", "cancel", "worker", "--turn-id", "turn-1", expected=1)
        self.assertEqual(result["error"]["code"], "STOP_UNCONFIRMED")

    def test_client_approval_request_is_reported_without_answering_it(self):
        original = self.respond
        self.respond = lambda r: {"method": "item/commandExecution/requestApproval", "params": {}} if r["method"] == "thread/read" else original(r)
        result = self.cli("thread", "read", "worker", expected=1)
        self.assertEqual(result["error"]["code"], "ATTENTION_REQUIRED")
        self.assertTrue(all("method" in r for r in self.calls))

    def test_send_rechecks_unknown_state_after_resume(self):
        original = self.respond
        def response(request):
            if request["method"] == "thread/resume":
                self.thread["status"] = {"type": "systemError"}
                return {"result": {"thread": self.thread}}
            if request["method"] == "turn/start":
                return {"result": {"turn": {"id": "unsafe-turn", "status": "inProgress"}}}
            return original(request)
        self.respond = response
        result = self.cli("thread", "send", "worker", "--message", "notice", expected=1)
        self.assertEqual(result["error"]["code"], "UNKNOWN_STATUS")
        self.assertFalse(any(r["method"] == "turn/start" for r in self.calls))

    def test_failed_native_turns_remain_terminal_and_distinct_in_watcher(self):
        self.thread["turns"][-1].update(status="failed", items=[], error={"message": "model request failed"})
        original = self.respond
        def response(request):
            if request["method"] == "thread/read" and request["params"]["threadId"] == "parent":
                return {"result": {"thread": {"id": "parent", "status": {"type": "active", "activeFlags": []}, "turns": []}}}
            return original(request)
        self.respond = response
        state = str(Path(self.tmp.name) / "watch.json")
        env = dict(os.environ, HOST_DAG_CODEX_SOCKET=self.socket, HOST_DAG_CODEX_LOCK_DIR=self.tmp.name, HOST_DAG_TOKEN_GUARD_STATE=str(Path(self.tmp.name)/'guard-index'))
        def watch(*args):
            p = subprocess.run([sys.executable, str(SCRIPT.with_name("host-dag-watch.py")),
                                "--state", state, *args], env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
            return json.loads(p.stdout)
        watch("init", "--parent-thread", "parent", "--host-cli", str(SCRIPT), "--host-cwd", self.tmp.name)
        watch("track", "--ticket", "NATIVE-ERROR", "--thread", "worker")
        first = watch("poll")
        self.assertEqual(first["events"][0]["kind"], "failed")
        self.assertIn("model request failed", first["events"][0]["summary"])
        self.assertTrue(watch("status")["tracked"][0]["terminal"])
        self.thread["turns"][-1]["id"] = "turn-2"
        self.assertEqual(watch("poll")["events"][0]["kind"], "failed")
        self.assertEqual(watch("poll")["events"], [])

    def goal_peer(self, status):
        self.goal = None if status is None else {"threadId": "worker", "objective": "dag", "status": status}
        original = self.respond
        def response(request):
            if request["method"] == "thread/goal/get":
                return {"result": {"goal": self.goal}}
            if request["method"] == "thread/goal/set":
                self.assertEqual(set(request["params"]), {"threadId", "status"})
                self.goal = {**self.goal, "status": request["params"]["status"]}
                return {"result": {"goal": self.goal}}
            return original(request)
        self.respond = response

    def goal_writes(self):
        return [c["params"] for c in self.calls if c.get("method") == "thread/goal/set"]

    def test_pause_and_resume_only_move_between_active_and_paused(self):
        self.goal_peer("active")
        paused = self.cli("goal", "pause", "worker")
        self.assertTrue(paused["changed"])
        self.assertEqual(paused["goal"]["status"], "paused")
        self.assertFalse(self.cli("goal", "pause", "worker")["changed"])
        self.assertEqual(self.cli("goal", "resume", "worker")["goal"]["status"], "active")
        self.assertEqual(self.goal_writes(), [{"threadId": "worker", "status": "paused"},
                                              {"threadId": "worker", "status": "active"}])

    def test_goal_edges_never_rewrite_terminal_or_missing_goals(self):
        for status in ("complete", "blocked", "budgetLimited", None):
            self.goal_peer(status)
            self.assertFalse(self.cli("goal", "pause", "worker")["changed"])
            self.assertFalse(self.cli("goal", "resume", "worker")["changed"])
        self.assertEqual(self.goal_writes(), [])
        self.assertIsNone(self.cli("goal", "read", "worker")["goal"])

    def test_message_readback_only_acknowledges_actual_user_input(self):
        self.thread["turns"][0]["items"].insert(0, {"type": "userMessage", "id": "u1", "content": [{"type": "text", "text": "MARKER-original"}]})
        self.thread["turns"].append({"id": "turn-2", "status": "completed", "items": [
            {"type": "userMessage", "id": "u2", "content": [{"type": "text", "text": "second"}]},
            {"type": "agentMessage", "id": "a2", "text": "MARKER-not-a-receipt"}]})
        first = self.cli("thread", "read", "worker", "--view", "messages", "--limit", "1")
        self.assertEqual(first["messages"][0]["text"], "second")
        second = self.cli("thread", "read", "worker", "--view", "messages", "--limit", "1", "--cursor", first["nextCursor"])
        self.assertEqual(second["messages"][0]["text"], "MARKER-original")
        self.assertIsNone(second["nextCursor"])


if __name__ == "__main__":
    unittest.main()

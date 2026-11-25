from __future__ import annotations

import importlib.util
import tempfile
import unittest
import json
from unittest import mock
from typing import Any
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("bridge", SCRIPTS / "host-dag-telegram-bridge.py")
assert spec is not None
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)  # type: ignore[union-attr]

CHAT = "100000001"
THREAD = "parent-thread"


def update(uid: int, chat: str, text: str, quoted: str = "") -> dict:
    message = {"message_id": uid * 10, "chat": {"id": int(chat)}, "text": text}
    if quoted:
        message["reply_to_message"] = {"text": quoted}
    return {"update_id": uid, "message": message}


class BridgeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.scratch = Path(self.tmp.name)
        self.sent: list[dict] = []
        self.host_calls: list[list[str]] = []
        self.busy = False
        self.reply = {"turnId": "t0", "text": "old"}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def api(self, method: str, params: dict, timeout: float) -> dict:
        self.sent.append(params)
        return {"ok": True}

    def host(self, args: list[str]) -> dict:
        self.host_calls.append(args)
        if args[:2] == ["thread", "send"]:
            if self.busy:
                return {"error": "THREAD_BUSY"}
            self.forwarded = Path(args[3]).read_text(encoding="utf-8")
            return {"turn": {"id": "t1"}}
        return {"result": self.reply}

    def test_only_the_users_chat_is_queued(self) -> None:
        state: dict[str, Any] = {"offset": 0}
        bridge.take_updates(state, [update(5, "999", "hack"), update(6, CHAT, "同意", "KV17-03 需决定")],
                            CHAT, self.api, self.host, THREAD)
        self.assertEqual(state["offset"], 7)
        self.assertEqual([i["text"] for i in state["queue"]], ["同意"])
        self.assertEqual(state["queue"][0]["quoted"], "KV17-03 需决定")

    def test_busy_coordinator_is_retried_then_answer_pushed_back(self) -> None:
        state: dict[str, Any] = {"offset": 0}
        bridge.take_updates(state, [update(1, CHAT, "保留旧 PVC", "KV17-03 需决定")],
                            CHAT, self.api, self.host, THREAD)
        self.busy = True
        bridge.deliver(state, CHAT, self.api, self.host, THREAD, self.scratch)
        self.assertEqual(state["queue"][0]["status"], "queued")
        self.busy = False
        bridge.deliver(state, CHAT, self.api, self.host, THREAD, self.scratch)
        self.assertEqual(state["queue"][0]["status"], "sent")
        self.assertIn("保留旧 PVC", self.forwarded)
        self.assertIn("KV17-03 需决定", self.forwarded)
        self.assertIn("Telegram 用户回复", self.forwarded)
        self.reply = {"turnId": "t1", "text": ""}
        bridge.deliver(state, CHAT, self.api, self.host, THREAD, self.scratch)
        self.assertEqual(len(state["queue"]), 1)
        self.reply = {"turnId": "t1", "text": "已续做 round3"}
        bridge.deliver(state, CHAT, self.api, self.host, THREAD, self.scratch)
        self.assertEqual(state["queue"], [])
        self.assertTrue(self.sent[-1]["text"].endswith("已续做 round3"))

    def test_status_command_answers_without_forwarding(self) -> None:
        state: dict[str, Any] = {"offset": 0}
        bridge.take_updates(state, [update(2, CHAT, "/status")], CHAT, self.api, self.host, THREAD)
        self.assertEqual(state.get("queue", []), [])
        self.assertEqual(self.sent[-1]["text"], "old")
        self.assertFalse(any(c[:2] == ["thread", "send"] for c in self.host_calls))


class ReasonixFaultBridgeTest(unittest.TestCase):
    def test_cli_watcher_alert_queue_dedup_failed_send_and_recovery(self):
        from test_hostdag_cli_agent import load
        import hostdag_reasonix_faults as faults
        import hostdag_self_heal as heal
        watcher = load('watch_reasonix_bridge_test', 'host-dag-watch.py')
        with tempfile.TemporaryDirectory() as tmp, \
             mock.patch.object(heal.subprocess, 'run'), \
             mock.patch.object(heal, 'is_quiet_hours', return_value=False), \
             mock.patch.object(heal, 'send_telegram', return_value=False) as transport:
            root = Path(tmp); home = root / 'home'; home.mkdir()
            state = {'state_path': str(root / 'watch.json')}
            fault = faults.record_failure(home, {'kind': 'balance', 'httpStatus': 402}, 'BK-B3', state['state_path'])
            for ticket in ('BK-B3', 'SECOND-TICKET', 'BK-B3-RETRY'):
                out = root / ticket; out.mkdir()
                run = out / 'cli-run.json'
                run.write_text(json.dumps({'provider': 'reasonix', 'unit': 'fake-unit', 'result': str(out / 'cli-result.json')}))
                (out / 'cli-result.json').write_text(json.dumps({'status': 'QUOTA_EXHAUSTED', 'exitCode': 1,
                    'resultText': '', 'credentialFault': fault}))
                watcher.observe_cli_ticket(state, ticket, {'provider': 'reasonix', 'run_file': str(run)}, 10)
            queue = root / 'advisor/deferred.jsonl'
            self.assertEqual(len(queue.read_text().splitlines()), 1)
            self.assertFalse((root / 'telegram-sent.json').exists())
            transport.assert_not_called()  # Desktop/queued does not mean sent.
            self.assertEqual(bridge.flush_deferred(root, 11), 0)
            self.assertEqual(json.loads(queue.read_text())['status'], 'undelivered')
            self.assertFalse((root / 'telegram-sent.json').exists())
            transport.return_value = True
            self.assertEqual(bridge.flush_deferred(root, 12), 1)
            self.assertEqual(len(json.loads((root / 'telegram-sent.json').read_text())), 1)
            body = transport.call_args.args[1]
            self.assertIn('DeepSeek 官方 API 余额/额度不足，Reasonix任务已停止重试，请更换或充值API key', body)
            self.assertIn('BK-B3', body); self.assertIn('最小探针', body)
            faults.notify(state, 'ANOTHER', fault, 20)
            self.assertFalse(queue.exists(), 'Acknowledged fault is deduplicated across tickets')
            token = faults.begin_recovery(home)
            self.assertTrue(faults.finish_recovery(home, token, {'status': 'COMPLETED', 'sawResult': True, 'sessionId': 'probe'}))
            faults.notify(state, 'STALE', fault, 21)
            self.assertFalse(queue.exists())
            auth = faults.record_failure(home, {'kind': 'authentication', 'httpStatus': 401}, 'NEW', state['state_path'])
            faults.notify(state, 'NEW', auth, 22)
            item = json.loads(queue.read_text())
            self.assertIn('key认证失败', item['title'])
            self.assertNotIn('余额', item['body'])
            self.assertNotEqual(auth['id'], fault['id'])
            token = faults.begin_recovery(home)
            self.assertTrue(faults.finish_recovery(home, token, {'status': 'COMPLETED', 'sawResult': True}))
            before = transport.call_count
            self.assertEqual(bridge.flush_deferred(root, 23), 0)
            self.assertEqual(transport.call_count, before, 'Do not send a recovered incident still queued after a failed send')

    def test_http_200_without_api_success_is_not_sent(self):
        import hostdag_self_heal as heal
        import io
        response = mock.MagicMock()
        response.status = 200
        response.__enter__.return_value = response
        opener = mock.MagicMock()
        opener.open.return_value = response
        with mock.patch.object(heal, 'telegram_config', return_value={'TELEGRAM_BOT_TOKEN': 'fake', 'TELEGRAM_CHAT_ID': 'fake'}), \
             mock.patch('urllib.request.build_opener', return_value=opener), \
             mock.patch.object(heal.time, 'sleep'), \
             mock.patch.object(heal.json, 'load', return_value={'ok': False}):
            self.assertFalse(heal.send_telegram('fixture', 'fixture'))


if __name__ == "__main__":
    unittest.main()

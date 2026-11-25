"""Sender behaviour against a fake host; no live run, host, or systemd unit."""
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
import hostdag_send_when_idle as sender

FAKE = SCRIPTS/'tests/fixtures/incidents/20261104-send-silent-drop/fake_host.py'
THREAD = 'th-1'
BODY = 'notice: review finished'


class Clock:
    def __init__(self):
        self.now = 1000.0
        self.delays = []

    def time(self):
        return self.now

    def sleep(self, delay):
        self.delays.append(delay)
        self.now += delay


class Case(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.run_dir = self.root/'run'
        (self.run_dir/'control/parent-tools').mkdir(parents=True)
        (self.run_dir/'run.json').write_text(json.dumps({'batches':{}}))
        (self.run_dir/'watch.json').write_text(json.dumps({'parent_thread':THREAD}))
        self.fake = self.root/'fake'
        self.fake.mkdir()
        self.message = self.root/'m.md'
        self.message.write_text(BODY)
        self.clock = Clock()
        env = patch.dict(os.environ, {'FAKE_HOST_DIR':str(self.fake), 'HOST_DAG_SEND_NOTIFY_CMD':'true'})
        env.start()
        self.addCleanup(env.stop)

    def mode(self, send=None, read=None):
        if send:
            (self.fake/'send_mode').write_text(send)
        if read:
            (self.fake/'read_mode').write_text(read)

    def call(self, *extra):
        argv = ['--run', str(self.run_dir), '--host', str(FAKE), '--send-timeout', '1', '--read-timeout', '1',
                '--settle', '30', *extra]
        self.err = io.StringIO()
        self.out = io.StringIO()
        with contextlib.redirect_stderr(self.err), contextlib.redirect_stdout(self.out):
            return sender.main(argv, sleep=self.clock.sleep, clock=self.clock.time)

    def send(self, *extra):
        return self.call(*extra, THREAD, str(self.message))

    def drain(self):
        return self.call('--drain')

    def receipt(self):
        return json.loads(next((self.run_dir/'control/parent-tools/send-when-idle-queue').glob('*.json')).read_text())

    def events(self):
        path = self.run_dir/'control/parent-tools/send.log'
        return [json.loads(line)['event'] for line in path.read_text().splitlines()]

    def calls(self, action):
        return (self.fake/'calls.log').read_text().split().count(action)

    def landed(self):
        path = self.fake/'messages.json'
        return len(json.loads(path.read_text())) if path.exists() else 0

    def seed_message(self):
        (self.fake/'messages.json').write_text(json.dumps([{'id':'late', 'text':BODY}]))


class Delivery(Case):
    def test_plain_send_is_delivered_and_spool_removed(self):
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.landed(), 1)
        self.assertEqual(self.receipt()['status'], 'delivered')
        self.assertEqual(list((self.run_dir/'control/parent-tools/send-when-idle-queue').glob('*.msg')), [])
        self.assertIn('inProgress', self.out.getvalue())

    def test_busy_waits_then_delivers(self):
        self.mode(send='busy')
        def recover(delay):
            self.clock.delays.append(delay)
            self.mode(send='ok')
        self.clock.sleep = recover
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.clock.delays, [60])
        self.assertEqual(self.landed(), 1)

    def test_receipt_and_log_never_hold_the_body(self):
        self.mode(send='hang', read='hang')
        self.send()
        self.assertNotIn(BODY, json.dumps(self.receipt()))
        self.assertNotIn(BODY, (self.run_dir/'control/parent-tools/send.log').read_text())

    def test_rerun_inside_window_does_not_double_send(self):
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.landed(), 1)
        self.assertIn('ALREADY_DELIVERED', self.out.getvalue())
        self.assertIn('already-delivered', self.events())

    def test_reuse_of_a_file_later_is_a_new_send(self):
        self.assertEqual(self.send(), 0)
        self.clock.now += 3600
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.landed(), 2)

    def test_resend_flag_sends_inside_window(self):
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.send('--resend'), 0)
        self.assertEqual(self.landed(), 2)


class SilentDrop(Case):
    def test_timeout_verified_absent_is_nonzero_logged_queued_then_redelivered_once(self):
        self.mode(send='hang')
        self.assertEqual(self.send('--max-timeouts', '1'), sender.EX_QUEUED)
        self.assertEqual(self.receipt()['status'], 'queued')
        self.assertIn('uncertain', self.events())
        self.assertIn('left-queued', self.events())
        self.assertIn('SEND-NOT-DELIVERED', self.err.getvalue())
        self.assertEqual(self.landed(), 0)
        self.mode(send='ok')
        self.message.unlink()
        self.assertEqual(self.drain(), 0)
        self.assertEqual(self.landed(), 1)
        self.assertEqual(self.receipt()['status'], 'delivered')

    def test_timeout_replays_only_after_settle(self):
        self.mode(send='hang')
        self.send('--max-timeouts', '1')
        self.assertEqual(self.clock.delays, [30])

    def test_timeout_but_message_landed_is_delivered_without_replay(self):
        self.mode(send='hang-delivered')
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.calls('send'), 1)
        self.assertEqual(self.landed(), 1)
        self.assertEqual(self.receipt()['status'], 'delivered')

    def test_late_landing_during_settle_is_not_replayed(self):
        self.mode(send='hang')
        def land_late(delay):
            self.clock.now += delay
            self.seed_message()
        self.clock.sleep = land_late
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.calls('send'), 1)

    def test_host_timeout_error_is_uncertain_not_failed(self):
        self.mode(send='host-timeout')
        self.assertEqual(self.send('--max-timeouts', '1'), sender.EX_QUEUED)
        self.assertIn('uncertain', self.events())
        self.assertEqual(self.receipt()['reason'], 'send timed out; verified not delivered')

    def test_empty_output_exit_zero_is_not_success(self):
        self.mode(send='empty')
        self.assertEqual(self.send('--max-timeouts', '1'), sender.EX_QUEUED)
        self.assertEqual(self.receipt()['status'], 'queued')

    def test_unreadable_thread_stays_uncertain_and_never_replays(self):
        self.mode(send='hang')
        self.message.write_text(BODY)
        self.mode(read='hang')
        self.assertEqual(self.send(), sender.EX_QUEUED)
        self.assertEqual(self.receipt()['status'], 'uncertain')
        self.assertEqual(self.calls('send'), 1)
        # Still unreadable on the next pass: the drain does not replay either.
        self.mode(send='ok')
        self.clock.now += 300
        self.assertEqual(self.drain(), sender.EX_QUEUED)
        self.assertEqual(self.calls('send'), 1)

    def test_drain_finds_landed_message_and_does_not_replay(self):
        self.mode(send='hang', read='hang')
        self.assertEqual(self.send(), sender.EX_QUEUED)
        self.mode(send='ok', read='ok')
        (self.fake/'read_mode').write_text('ok')
        self.seed_message()
        self.assertEqual(self.drain(), 0)
        self.assertEqual(self.calls('send'), 1)
        self.assertEqual(self.receipt()['status'], 'delivered')

    def test_drain_replays_after_read_proves_absent(self):
        self.mode(send='hang', read='hang')
        self.assertEqual(self.send(), sender.EX_QUEUED)
        self.mode(send='ok', read='ok')
        self.clock.now += 300
        self.assertEqual(self.drain(), 0)
        self.assertEqual(self.calls('send'), 2)
        self.assertEqual(self.landed(), 1)

    def test_old_identical_message_is_not_mistaken_for_this_send(self):
        self.seed_message()
        self.mode(send='hang')
        self.assertEqual(self.send('--max-timeouts', '1'), sender.EX_QUEUED)
        self.assertEqual(self.receipt()['status'], 'queued')

    def test_interrupted_sending_receipt_is_verified_first(self):
        self.mode(send='hang', read='hang')
        self.send()
        path = next((self.run_dir/'control/parent-tools/send-when-idle-queue').glob('*.json'))
        record = json.loads(path.read_text())
        record['status'] = 'sending'
        path.write_text(json.dumps(record))
        self.mode(send='ok', read='ok')
        self.seed_message()
        self.assertEqual(self.send(), 0)
        self.assertEqual(self.calls('send'), 1)


class Failure(Case):
    def test_permanent_host_error_is_nonzero_and_logged(self):
        self.mode(send='unknown-status')
        self.assertEqual(self.send(), 1)
        self.assertEqual(self.receipt()['status'], 'failed')
        self.assertIn('failed', self.events())
        self.assertIn('SEND-FAILED', self.err.getvalue())

    def test_empty_message_file_is_refused(self):
        self.message.write_text('  \n')
        self.assertEqual(self.send(), 2)

    def test_missing_message_file_is_nonzero(self):
        self.message.unlink()
        self.assertEqual(self.send(), 1)

    def test_drain_expires_loudly_without_sending(self):
        self.mode(send='hang')
        self.send('--max-timeouts', '1')
        self.mode(send='ok')
        self.clock.now += 7 * 3600
        with patch.object(sender.Queue, 'notify') as notify:
            self.assertEqual(self.drain(), 1)
        notify.assert_called_once()
        self.assertEqual(self.receipt()['status'], 'expired')
        self.assertEqual(self.landed(), 0)

    def test_drain_does_not_block_on_a_busy_thread(self):
        self.mode(send='hang')
        self.send('--max-timeouts', '1')
        self.mode(send='busy')
        self.assertEqual(self.drain(), sender.EX_QUEUED)
        self.assertEqual(self.receipt()['status'], 'queued')
        self.assertEqual(self.clock.delays, [30])

    def test_drain_skips_an_entry_owned_by_a_live_sender(self):
        import fcntl
        self.mode(send='hang')
        self.send('--max-timeouts', '1')
        self.mode(send='ok')
        lock_path = next((self.run_dir/'control/parent-tools/send-when-idle-queue').glob('*.lock'))
        with lock_path.open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            self.assertEqual(self.drain(), 0)
        self.assertEqual(self.landed(), 0)

    def test_empty_queue_drain_is_clean(self):
        self.assertEqual(self.drain(), 0)

    def test_usage_errors(self):
        with self.assertRaises(SystemExit):
            self.call(THREAD)
        with self.assertRaises(SystemExit):
            self.call('--drain', THREAD, str(self.message))



class ReviewCapStop(Case):
    def ticket(self, count, first):
        (self.run_dir/'run.json').write_text(json.dumps({'batches':{'B':{'tickets':{'T':{
            'kind':'tool','critical_path':True,'root_ticket':'T','candidate':'fixture-sha','state':'REVIEWING',
            'review_round_count':count,'first_reviewing_at':first,
            'reviews':{'spec':{'thread':THREAD,'verdict':'FAIL'}}}}}}}))

    def test_registered_reviewer_fourth_send_is_refused_before_host_rpc(self):
        self.ticket(3,self.clock.now-1)
        self.assertNotEqual(self.send(),0)
        self.assertEqual(self.calls('send'),0)
        self.assertEqual(self.landed(),0)
        self.assertIn('REVIEW_CAP',self.err.getvalue())

    def test_six_hours_without_pass_is_refused_before_host_rpc(self):
        self.clock.now=23000
        self.ticket(1,1000)
        self.assertNotEqual(self.send(),0)
        self.assertEqual(self.calls('send'),0)
        self.assertEqual(self.landed(),0)

    def test_under_budget_registered_reviewer_can_still_send(self):
        self.ticket(1,self.clock.now-1)
        self.assertEqual(self.send(),0)
        self.assertEqual(self.calls('send'),1)


if __name__ == '__main__':
    unittest.main()

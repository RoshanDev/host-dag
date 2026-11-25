import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hostdag_watch_registration as reg

THREAD = '01a143ab-ed4c-7a93-bdba-8e03f0852173'
OTHER = '01a143ab-ef31-7f61-9d63-9ff61bd10b95'


class RegistrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'watch.json'
        self.state = {'tracked': {}}
        reg.atomic(self.path, self.state)

    def request(self, **extra):
        return reg.enqueue(self.path, {'op': 'track', 'ticket': 'A', 'at': 1,
                                       'record': {'thread': THREAD}, **extra})

    def batch(self, **ticket):
        return {'batches': {'V': {'tickets': {'A': {'state': 'RUNNING', 'executor': THREAD, **ticket}}}}}

    def test_crash_before_ack_replays_without_losing_completion(self):
        request = self.request()
        real = reg.atomic
        def crash_ack(path, value):
            if path.suffix == '.ack':
                raise OSError('simulated crash')
            real(path, value)
        with patch.object(reg, 'atomic', side_effect=crash_ack):
            with self.assertRaises(OSError):
                reg.drain(self.state, self.path, real)
        recovered = json.loads(self.path.read_text())
        recovered['tracked']['A'].update(terminal=True, last_result_fingerprint='already delivered')
        reg.drain(recovered, self.path, real)
        self.assertTrue(recovered['tracked']['A']['terminal'])
        self.assertEqual(reg.receipt(request)['status'], 'applied')

    def test_multiple_requests_order_and_duplicate_identity(self):
        self.request()
        reg.drain(self.state, self.path, reg.atomic)
        self.state['tracked']['A']['terminal'] = True
        self.request()
        reg.drain(self.state, self.path, reg.atomic)
        self.assertTrue(self.state['tracked']['A']['terminal'])
        self.request(record={'thread': OTHER})
        self.request(op='untrack')
        reg.drain(self.state, self.path, reg.atomic)
        self.assertNotIn('A', self.state['tracked'])

    def test_missing_batch_registration_recovers_without_replaying(self):
        data = self.batch(reviews={'spec': {'thread': OTHER}})
        reg.reconcile(self.state, data, 10)
        self.assertEqual(self.state['tracked']['A-review-spec']['thread'], OTHER)
        self.state['tracked']['A']['terminal'] = True
        reg.reconcile(self.state, data, 20)
        self.assertTrue(self.state['tracked']['A']['terminal'])
        reg.reconcile(self.state, self.batch(executor=OTHER), 30)
        self.assertFalse(self.state['tracked']['A']['terminal'])
        self.assertEqual(self.state['tracked']['A']['thread'], OTHER)

    def test_untrack_suppression_and_closed_ticket_cleanup(self):
        data = self.batch()
        reg.reconcile(self.state, data, 1)
        self.request(op='untrack')
        reg.drain(self.state, self.path, reg.atomic)
        reg.reconcile(self.state, data, 2)
        self.assertNotIn('A', self.state['tracked'])
        reg.reconcile(self.state, self.batch(executor=OTHER), 3)
        self.assertIn('A', self.state['tracked'])
        reg.reconcile(self.state, self.batch(status='ACCEPTED'), 4)
        self.assertNotIn('A', self.state['tracked'])

    def test_grok_dispatch_and_incremental_review_recovered(self):
        evidence = self.path.with_name('dispatch.json')
        stdout = self.path.with_name('stdout')
        stdout.write_text('WRAPPER_PID 123\nSESSION abc\n')
        evidence.write_text(json.dumps({'provider': 'grok', 'log': '/example/log', 'stdout': str(stdout)}))
        reg.reconcile(self.state, self.batch(executor='host-dag-grok-A', evidence=str(evidence)), 1)
        self.assertEqual(self.state['tracked']['A']['session'], 'abc')
        self.assertEqual(self.state['tracked']['A']['wrapper_pid'], 123)
        review = self.path.with_name('review-incremental-dispatch.json')
        review.write_text(json.dumps({'threadId': OTHER}))
        reg.reconcile(self.state, self.batch(state='REVIEWING', evidence=str(review)), 2)
        self.assertEqual(self.state['tracked']['A-review-incremental']['thread'], OTHER)

    def test_waiting_history_not_replayed_and_missing_identity_visible(self):
        reg.reconcile(self.state, self.batch(state='WAITING_RESOURCE'), 1)
        self.assertEqual(self.state['tracked'], {})
        reg.reconcile(self.state, self.batch(executor='unknown-cli'), 2)
        self.assertIn('A', self.state['registration_gaps'])

    def test_auto_registered_cli_run_without_state_is_tracked(self):
        run = self.path.with_name('cli-run.json')
        run.write_text(json.dumps({'provider': 'claude', 'unit': 'host-dag-cli-claude-A-1', 'log': '/x/cli.ndjson'}))
        self.state['tracked']['A'] = {'provider': 'claude', 'run_file': '/old/cli-run.json', 'terminal': True}
        data = {'batches': {'V': {'tickets': {'A': {'status': 'CLAIMED', 'state': None,
                                                    'executor': 'host-dag-cli-claude-A-1', 'evidence': str(run)}}}}}
        reg.reconcile(self.state, data, 5)
        self.assertEqual(self.state['tracked']['A']['run_file'], str(run))
        self.assertFalse(self.state['tracked']['A']['terminal'])
        self.assertEqual(self.state['registration_gaps'], {})

    def test_round_directory_evidence_resolves_to_its_cli_run(self):
        rnd = self.path.with_name('run-1400')
        rnd.mkdir()
        (rnd / 'cli-run.json').write_text(json.dumps({'provider': 'claude', 'unit': 'u', 'log': '/x/cli.ndjson'}))
        data = {'batches': {'V': {'tickets': {'A': {'state': 'RUNNING', 'executor': 'u', 'evidence': str(rnd)}}}}}
        reg.reconcile(self.state, data, 5)
        self.assertEqual(self.state['tracked']['A']['run_file'], str(rnd / 'cli-run.json'))
        self.assertEqual(self.state['registration_gaps'], {})

    def test_malformed_request_does_not_block_good_request(self):
        bad = reg.inbox(self.path) / '000.json'
        reg.atomic(bad, {'op': 'oops'})
        self.request()
        reg.drain(self.state, self.path, reg.atomic)
        self.assertEqual(reg.receipt(bad)['status'], 'rejected')
        self.assertIn('A', self.state['tracked'])


if __name__ == '__main__':
    unittest.main()



class AgyObservationTest(unittest.TestCase):
    def test_agy_run_receipt_is_observable(self):
        from hostdag_cli_watch import observe_record
        with tempfile.TemporaryDirectory() as d:
            run = Path(d) / 'cli-run.json'
            run.write_text(json.dumps({'unit': 'host-dag-cli-agy-T-1', 'provider': 'agy', 'log': str(Path(d) / 'cli.ndjson')}))
            rec = reg.resolve('host-dag-cli-agy-T-1', d)
            self.assertEqual(rec['provider'], 'agy')
            self.assertEqual(rec['run_file'], str(run))
            self.assertEqual(observe_record(rec, is_active=lambda u: True)['status'], 'RUNNING')
            for code, want in ((0, 'COMPLETED'), (3, 'FAILED')):
                run.write_text(json.dumps({'unit': 'host-dag-cli-agy-T-1', 'provider': 'agy', 'log': 'x', 'finished': 1.0, 'exit_code': code}))
                self.assertEqual(observe_record(rec, is_active=lambda u: False)['status'], want)


if __name__ == '__main__':
    unittest.main()

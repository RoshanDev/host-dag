#!/usr/bin/env python3
"""Send a message file to a thread once idle; a lost send is never reported as success.

Usage: host-dag-send-when-idle.sh [--run RUN] THREAD MSGFILE
       host-dag-send-when-idle.sh [--run RUN] --drain

Exit: 0 delivered (or delivered moments ago); 75 still queued or uncertain, the
drain timer owns it; 1 permanent failure; 2 bad usage.

A send that times out, returns nothing, or reports an unknown mutation outcome
may still have reached the thread, so it is recorded `uncertain` and resolved by
`thread read --view messages` before any replay. The message is replayed only
when reads prove it absent after the settle window; if reads cannot answer, it
stays uncertain. Receipts and send.log hold identity, digest and status only.
The body lives in a private spool file until delivery, so the caller may delete
its copy.
"""
from hostdag_json import atomic_json, public_path, read_json
from hostdag_send_retry import deliver
import hostdag_reply as reply_message

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time


EX_QUEUED = 75
OPEN_STATES = {'queued', 'sending', 'uncertain'}
# The host's own words for "turn/start may or may not have landed".
UNKNOWN_OUTCOME_CODES = {'TIMEOUT', 'RUNTIME_UNREACHABLE'}
# Raised before turn/start, so nothing was submitted.
RETRY_CODES = {'TURN_CHANGED'}
ACCEPTED = re.compile(r'"(?:status|state)"\s*:\s*"inProgress"')
# Matches hostdag_send_retry.TRANSIENT, so deliver() applies its bounded backoff.
NOT_SUBMITTED = 'send temporarily unavailable: verified not delivered'
ACCEPTED_RECEIPT = '{"status":"inProgress","verified":"thread read"}'
PRUNE_AFTER = 7 * 86400


class LeaveQueued(Exception):
    """Stop this pass; the durable receipt keeps the message for the drain."""


def host_error_code(output):
    for line in reversed(output.strip().splitlines()):
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and isinstance(value.get('error'), dict):
            return str(value['error'].get('code') or 'UNKNOWN')
    return None


def run_host(host, argv, timeout):
    """Return (returncode, output); returncode None means the host was killed at the timeout."""
    env = dict(os.environ)
    env.setdefault('XDG_RUNTIME_DIR', f'/run/user/{os.getuid()}')
    env.setdefault('DBUS_SESSION_BUS_ADDRESS', f"unix:path={env['XDG_RUNTIME_DIR']}/bus")
    proc = subprocess.Popen([host, *argv], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, errors='replace', env=env, start_new_session=True)
    try:
        output, _ = proc.communicate(timeout=timeout)
        return proc.returncode, output
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        return None, ''


class Entry:
    """One queued message, processed while its lock file is held."""

    def __init__(self, queue, path, rec, drain):
        self.queue, self.path, self.rec, self.drain = queue, path, rec, drain
        self.spool = path.with_suffix('.msg')
        self.timeouts = 0
        self.blocker = None
        self.receipt = ''

    def persist(self, value):
        rec = self.rec
        rec.update(value)
        rec['updated_at'] = self.queue.clock()
        if value.get('status') == 'sending':
            rec['last_send_at'] = rec['updated_at']
            rec['attempts_total'] = rec.get('attempts_total', 0) + 1
            rec.pop('next_attempt_at', None)
        atomic_json(self.path, rec)

    def read_messages(self):
        args = self.queue.args
        code, output = run_host(self.rec['host'], ['thread', 'read', self.rec['thread'], '--view', 'messages',
                                                   '--limit', '25'], args.read_timeout)
        if code != 0:
            return None
        try:
            rows = json.loads(output)['messages']
        except (ValueError, KeyError, TypeError):
            return None
        return rows if isinstance(rows, list) else None

    def message_seen(self):
        """True/False from a readable thread; None when the thread cannot answer."""
        rows = self.read_messages()
        if rows is None:
            return None
        baseline = self.rec.get('baseline')
        text = self.spool.read_text(encoding='utf-8').strip()
        for row in rows:
            if baseline is not None and row.get('id') in baseline:
                continue
            # The token guard may append a pending instruction to the message.
            if text in (row.get('text') or ''):
                # Without a baseline an earlier delivery of these bytes looks identical.
                return None if baseline is None and self.rec.get('redelivery') else True
        return False

    def resolve(self):
        """Decide an uncertain send: 'delivered', 'absent', or 'unknown'. Never replays."""
        queue, args = self.queue, self.queue.args
        settled_at = self.rec.get('last_send_at', 0) + args.settle
        tries = 2 if self.drain else args.verify_tries
        for attempt in range(tries):
            seen = self.message_seen()
            if seen:
                return 'delivered'
            if seen is False and queue.clock() >= settled_at:
                return 'absent'
            if attempt + 1 < tries:
                # A slow server can still start the turn after the client was killed.
                queue.sleep(max(settled_at - queue.clock(), 1) if seen is False else max(args.settle / 4, 1))
        return 'unknown'

    def send(self):
        queue, args = self.queue, self.queue.args
        from hostdag_dispatch_guard import reserve_review, settle_review, review_fingerprint, DispatchError
        import uuid
        if not self.rec.get('review_token'):
            self.persist({'review_token':uuid.uuid4().hex})
        try:
            grant=reserve_review(args.run,thread=self.rec['thread'],token=self.rec['review_token'],
                fingerprint=review_fingerprint(self.rec['thread'],self.spool.read_text()),required=False,now=queue.clock())
        except DispatchError as exc:
            queue.log('review-cap-refused',self.rec,reason=exc.message,code=exc.code)
            print(exc.code+': '+exc.message,file=sys.stderr)
            return json.dumps({'error':{'code':exc.code,'message':exc.message}})
        self.persist({'review_grant':grant})
        forwarded=['--guard-run',str(args.run),'--review-reservation',grant['token']] if grant else []
        reply = self.rec.get('reply')
        if reply:
            forwarded += ['--steer-if-busy', '--reply-to', reply['question_id'],
                          '--decision-version', str(reply['decision_version']), '--ticket', reply['ticket']]
        if grant and grant.get('replay'):
            code, output = 0, json.dumps(grant['receipt'])
        else:
            code, output = run_host(self.rec['host'], ['thread', 'send', self.rec['thread'],
                                                       '--message-file', str(self.spool), *forwarded], args.send_timeout)
        if code == 0 and ACCEPTED.search(output):
            if reply:
                try:
                    delivery = json.loads(output)['delivery']
                    if delivery['status'] != 'ACCEPTED' or delivery['method'] not in {'start', 'steer'}:
                        raise ValueError('invalid reply receipt')
                    reply_message.accepted(delivery['method'], delivery['turnId'])
                    if json.loads(output)['turn']['id'] != delivery['turnId']:
                        raise ValueError('reply turn identities differ')
                except (ValueError, KeyError, TypeError):
                    return self.unknown('invalid acceptance receipt')
                self.persist({'status': 'delivered', 'delivery': delivery})
            settle_review(args.run,grant,'delivered',{'threadId':self.rec['thread'],'turn':{'status':'inProgress'}})
            self.receipt = output
            return output
        error = host_error_code(output)
        if code is None or not output.strip() or code == 0 or error in UNKNOWN_OUTCOME_CODES or (reply and error in {None, 'PROTOCOL_ERROR'}):
            reason = 'send-timeout' if code is None else (error or 'no-receipt')
            settle_review(args.run,grant,'uncertain')
            if reply:
                return self.unknown(reason)
            self.persist({'status':'uncertain', 'reason':reason})
            queue.log('uncertain', self.rec, reason=reason)
            return self.after_uncertain()
        if error in RETRY_CODES or error == 'THREAD_BUSY':
            settle_review(args.run,grant,'retryable')
            queue.log('retry', self.rec, reason=error)
            return output if error=='THREAD_BUSY' else NOT_SUBMITTED
        settle_review(args.run,grant,'uncertain')
        queue.log('host-error', self.rec, reason=error or f'exit-{code}')
        # A nonzero exit must not look delivered even if stdout holds a partial response.
        return output.replace('inProgress', 'sendFailed')

    def unknown(self, reason):
        self.settle_budget('uncertain')
        self.persist({'status': 'unknown', 'delivery': {'status': 'UNKNOWN'}, 'reason': reason})
        self.queue.log('unknown', self.rec, reason=reason)
        raise LeaveQueued('UNKNOWN: coordinator must inspect thread messages by reply marker; never replayed')

    def settle_budget(self,status):
        from hostdag_dispatch_guard import settle_review
        settle_review(self.queue.args.run,{'token':self.rec.get('review_token')},status,
                      {'threadId':self.rec['thread'],'turn':{'status':'inProgress'}} if status=='delivered' else None)

    def after_uncertain(self):
        queue, args = self.queue, self.queue.args
        verdict = self.resolve()
        queue.log('verified', self.rec, verdict=verdict)
        if verdict == 'delivered':
            self.settle_budget('delivered')
            self.receipt = ACCEPTED_RECEIPT
            return ACCEPTED_RECEIPT
        if verdict == 'unknown':
            raise LeaveQueued('uncertain: thread read could not confirm; not replayed')
        self.settle_budget('retryable')
        self.timeouts += 1
        if self.drain or self.timeouts >= args.max_timeouts:
            self.persist({'status':'queued', 'reason':'send timed out; verified not delivered'})
            raise LeaveQueued('queued: send timed out and thread read shows it was not delivered')
        return NOT_SUBMITTED

    def wait(self, delay):
        # deliver() has already persisted `queued`; a drain pass must not block on one thread.
        if self.drain:
            raise LeaveQueued('queued: retry on the next drain pass')
        self.queue.sleep(delay)

    def update(self, value):
        self.blocker = None if value == 'delivered' else value

    def finish_delivered(self):
        self.spool.unlink(missing_ok=True)
        self.queue.log('delivered', self.rec)
        print(self.receipt[-400:].strip() or ACCEPTED_RECEIPT)
        return 0

    def leave_queued(self, why):
        self.queue.log('left-queued', self.rec, status=self.rec.get('status'), reason=str(why))
        print(f"SEND-NOT-DELIVERED status={self.rec.get('status')} key={self.rec['key']}: {why}", file=sys.stderr)
        return EX_QUEUED

    def fail(self, status, reason):
        self.persist({'status':status, 'reason':reason})
        self.queue.log(status, self.rec, reason=reason)
        print(f"SEND-FAILED status={status} key={self.rec['key']}: {reason}", file=sys.stderr)
        self.queue.notify(f"{status}: thread {self.rec['thread']} key {self.rec['key']}: {reason}")
        return 1

    def process(self):
        if self.rec.get('reply') and self.rec.get('status') == 'unknown':
            return self.leave_queued('UNKNOWN: coordinator must inspect thread messages by reply marker; never replayed')
        if self.rec.get('reply') and self.rec.get('status') in {'sending', 'uncertain'}:
            # Even a readable absence is not proof a lost mutation will never land.
            try:
                self.unknown('previous reply outcome is unknown')
            except LeaveQueued as why:
                return self.leave_queued(why)
        if not self.spool.exists():
            return self.fail('failed', 'spooled message is missing')
        try:
            if self.rec.get('status') in {'sending', 'uncertain'}:
                # A previous process may have submitted this; look before any replay.
                verdict = self.resolve()
                self.queue.log('verified', self.rec, verdict=verdict)
                if verdict == 'delivered':
                    self.settle_budget('delivered')
                    self.persist({'status':'delivered'})
                    return self.finish_delivered()
                if verdict == 'unknown':
                    return self.leave_queued('uncertain: thread read could not confirm; not replayed')
                self.settle_budget('retryable')
            rc = deliver(self.send, self.update, self.persist, sleep=self.wait)
        except LeaveQueued as why:
            return self.leave_queued(why)
        if rc == 0:
            return self.finish_delivered()
        if 'exhausted' in (self.blocker or ''):
            self.persist({'status':'queued', 'reason':self.blocker})
            return self.leave_queued(self.blocker)
        return self.fail('failed', self.blocker or 'send failed')


class Queue:
    def __init__(self, args, sleep, clock):
        self.args, self.sleep, self.clock = args, sleep, clock
        self.directory = args.run/'control/parent-tools/send-when-idle-queue'
        self.log_path = args.run/'control/parent-tools/send.log'

    def log(self, event, rec, **detail):
        row = {'at':self.clock(), 'iso':time.strftime('%FT%T%z'), 'event':event, 'thread':rec.get('thread'),
               'key':rec.get('key'), 'attempts':rec.get('attempts_total', 0), **detail}
        try:
            with self.log_path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row, ensure_ascii=False) + '\n')
        except OSError as error:
            # The exit code still reports the outcome; do not lose the record quietly.
            print(f'send.log write failed: {type(error).__name__}: {json.dumps(row)}', file=sys.stderr)

    def notify(self, summary):
        command = os.environ.get('HOST_DAG_SEND_NOTIFY_CMD', 'notify-send')
        try:
            subprocess.run([command, '-u', 'critical', 'Host DAG send failed', summary],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
        except (OSError, subprocess.SubprocessError):
            pass

    def message_path(self, thread, digest, reply):
        legacy_key = hashlib.sha256(json.dumps([thread, digest]).encode()).hexdigest()[:24]
        legacy = self.directory/(legacy_key + '.json')
        if legacy.exists():
            old = read_json(legacy)
            if reply is not None and old.get('reply') == reply:
                # Existing reply receipts retain their path and owner across
                # the key change; ordinary sends retain their original key.
                return legacy
        if reply:
            key = hashlib.sha256(json.dumps([thread, digest, reply], sort_keys=True).encode()).hexdigest()[:24]
            return self.directory/(key + '.json')
        # With the switch off there is no explicit identity to derive a reply
        # key from. Find an existing owner before permitting an ordinary replay.
        owners = []
        for path in sorted(self.directory.glob('*.json')):
            old = read_json(path)
            if old.get('reply') and old.get('thread') == thread and old.get('message_digest') == digest:
                # A bare call must not let an ordinary legacy receipt or a
                # retryable reply mask a protected sibling, in either key format.
                status = old.get('status')
                priority = (0 if status in {'unknown', 'sending', 'uncertain'} else
                            1 if status in {'delivered', 'obsolete'} else 2)
                owners.append((priority, path))
        if owners:
            return min(owners)[1]
        return legacy

    def send_one(self, thread, message_file):
        args = self.args
        body = message_file.read_bytes()
        reply = reply_message.identity(args)
        if not body.decode('utf-8').strip():
            print(f'SEND-FAILED: empty message file {message_file}', file=sys.stderr)
            return 2
        digest = hashlib.sha256(body).hexdigest()
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.message_path(thread, digest, reply)
        key = path.stem
        with path.with_suffix('.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print(f'SEND-NOT-DELIVERED status=in-flight key={key}: another sender owns this message',
                      file=sys.stderr)
                return EX_QUEUED
            old = read_json(path) if path.exists() else None
            status = old.get('status') if old else None
            if old and old.get('reply'):
                # Rollback, --resend and the normal dedupe expiry cannot requeue
                # ACCEPTED/UNKNOWN/OBSOLETE decision replies.
                if reply and old['reply'] != reply:
                    print('SEND-FAILED: reply identity differs from the existing queue owner', file=sys.stderr)
                    return 2
                if status in {'delivered', 'obsolete'}:
                    print(f"{status.upper()} key={key}: retained reply receipt; not replayed")
                    return 0
                return Entry(self, path, old, False).process()
            if old and reply:
                print('SEND-FAILED: existing ordinary message entry; use a new explicit reply message', file=sys.stderr)
                return 2
            if status == 'delivered' and not args.resend and self.clock() - old['updated_at'] < args.dedupe_window:
                # A rerun right after success must not double-send; later reuse of a file is a new send.
                self.log('already-delivered', old)
                print(f'ALREADY_DELIVERED key={key} age={int(self.clock() - old["updated_at"])}s')
                return 0
            if status in OPEN_STATES:
                return Entry(self, path, old, False).process()
            rec = {'key':key, 'thread':thread, 'host':args.host, 'message_file':str(public_path(message_file)),
                   'message_digest':digest, 'created_at':self.clock(), 'redelivery':status == 'delivered'}
            if reply:
                rec['reply'] = reply
                body = reply_message.mark(body.decode('utf-8'), reply).encode('utf-8')
            entry = Entry(self, path, rec, False)
            entry.spool.touch(mode=0o600)
            entry.spool.write_bytes(body)
            rows = entry.read_messages()
            rec['baseline'] = None if rows is None else [row['id'] for row in rows if row.get('id')]
            entry.persist({'status':'queued'})
            self.log('queued', rec, baseline=rec['baseline'] is not None)
            return entry.process()

    def drain_one(self, path):
        with path.with_suffix('.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            rec = read_json(path)
            entry = Entry(self, path, rec, True)
            if rec.get('reply') and rec.get('status') in {'unknown', 'sending', 'uncertain'}:
                return entry.process()
            age = self.clock() - rec.get('updated_at', 0)
            if rec.get('status') not in OPEN_STATES:
                if age > PRUNE_AFTER and not rec.get('reply'):
                    entry.spool.unlink(missing_ok=True)
                    path.unlink()
                return 0
            if self.clock() - rec.get('created_at', 0) > self.args.max_age:
                return entry.fail('expired', f'undelivered after {int(self.args.max_age)}s; resend if still wanted')
            if rec.get('next_attempt_at', 0) > self.clock():
                return EX_QUEUED
            return entry.process()

    def drain(self):
        results = []
        for path in sorted(self.directory.glob('*.json')) if self.directory.is_dir() else []:
            try:
                results.append(self.drain_one(path))
            except (OSError, ValueError, KeyError) as error:
                self.log('drain-error', {'key':path.stem}, reason=type(error).__name__)
                print(f'SEND-FAILED key={path.stem}: unreadable entry: {type(error).__name__}', file=sys.stderr)
                results.append(1)
        return 1 if 1 in results else (EX_QUEUED if EX_QUEUED in results else 0)

    def cancel(self, key):
        if not re.fullmatch('[0-9a-f]{24}', key):
            raise ValueError('invalid queue entry key')
        path = self.directory / (key + '.json')
        with path.with_suffix('.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print('SEND-NOT-DELIVERED: owner still sending; cancel after it exits', file=sys.stderr)
                return EX_QUEUED
            rec = read_json(path)
            if not rec.get('reply'):
                raise ValueError('only explicit decision replies support obsolete receipts')
            if rec.get('status') != 'delivered':
                Entry(self, path, rec, False).persist({'status': 'obsolete', 'delivery': {'status': 'OBSOLETE'}})
                self.log('obsolete', rec)
            return 0


def parse(argv):
    environ = os.environ
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--run', type=Path, default=environ.get('HOST_DAG_RUN'))
    parser.add_argument('--host', default=environ.get('HOST_DAG_SEND_HOST', 'host-dag-codex-host'))
    reply_message.add_arguments(parser)
    parser.add_argument('--cancel-entry', metavar='KEY', help='coordinator marks a reply OBSOLETE; never requeues')
    parser.add_argument('--drain', action='store_true', help='retry open queue entries once each')
    parser.add_argument('--resend', action='store_true', help='deliberately send again inside the dedupe window')
    parser.add_argument('--send-timeout', type=float, default=float(environ.get('HOST_DAG_SEND_TIMEOUT', 60)))
    parser.add_argument('--read-timeout', type=float, default=float(environ.get('HOST_DAG_SEND_READ_TIMEOUT', 45)))
    # Longer than the slowest observed send (91s, 2026-11-04) so a late turn is seen before any replay.
    parser.add_argument('--settle', type=float, default=float(environ.get('HOST_DAG_SEND_SETTLE', 120)))
    parser.add_argument('--verify-tries', type=int, default=4)
    parser.add_argument('--max-timeouts', type=int, default=int(environ.get('HOST_DAG_SEND_MAX_TIMEOUTS', 2)))
    parser.add_argument('--dedupe-window', type=float, default=600)
    parser.add_argument('--max-age', type=float, default=6 * 3600, help='drain gives up loudly after this')
    parser.add_argument('thread', nargs='?')
    parser.add_argument('message_file', nargs='?', type=Path)
    args = parser.parse_args(argv)
    if args.run is None:
        parser.error('--run or HOST_DAG_RUN is required')
    args.run = Path(args.run)
    try:
        reply_message.identity(args)
    except ValueError as error:
        parser.error(str(error))
    if args.cancel_entry:
        if args.drain or args.thread or args.message_file or args.steer_if_busy:
            parser.error('--cancel-entry is used alone')
    elif args.drain == bool(args.thread and args.message_file) or (args.drain and args.thread):
        parser.error('give THREAD MSGFILE, or --drain alone')
    return args


def main(argv=None, *, sleep=time.sleep, clock=time.time):
    args = parse(argv)
    queue = Queue(args, sleep, clock)
    try:
        if args.cancel_entry:
            return queue.cancel(args.cancel_entry)
        return queue.drain() if args.drain else queue.send_one(args.thread, args.message_file)
    except (OSError, ValueError, KeyError) as error:
        print(f'SEND-FAILED: {type(error).__name__}: {error}; inspect {queue.directory}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())

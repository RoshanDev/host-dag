#!/usr/bin/env python3
"""Replacement for parent-tools/send-retry.sh; retains its five positional args.

The process owns the bounded retry queue. Its atomic receipt preserves pending
work on interruption; a restarted wrapper must inspect an `sending` receipt
before replaying because delivery may have succeeded. No message body or raw
host output is copied into receipts or blockers.
"""
from hostdag_json import public_path, atomic_json, read_json

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


TRANSIENT = re.compile(
    r'JSONDecodeError|FileNotFoundError|BlockingIOError|PermissionError|'
    r'cannot establish admission: OSError|'
    r'temporar(?:y|ily) (?:unavailable|unreadable)|'
    r'Resource temporarily unavailable|file (?:temporarily )?(?:unreadable|not readable)|'
    r'cannot read (?:file|.*\.json)|No such file or directory', re.I)


def deliver(send, update, persist, *, sleep=time.sleep):
    transient_retries = busy_retries = 0
    attempt = 0
    while True:
        attempt += 1
        persist({'status':'sending', 'attempt':attempt})
        output = send()
        if re.search(r'"(?:status|state)"\s*:\s*"inProgress"', output):
            # Record before ticket update: a failed update must not replay delivery.
            persist({'status':'delivered', 'attempt':attempt})
            update('delivered')
            return 0
        busy = 'THREAD_BUSY' in output
        transient = TRANSIENT.search(output)
        if busy and busy_retries < 59:
            busy_retries += 1
            delay = 60
        elif not busy and transient and transient_retries < 5:
            delay = 0.5 * 2 ** transient_retries
            transient_retries += 1
        else:
            reason = 'THREAD_BUSY exhausted' if busy else ('transient file/JSON error exhausted' if transient else 'non-transient failure')
            blocker = f'parent send failed: {reason}; attempts={attempt}'
            persist({'status':'failed', 'attempt':attempt, 'blocker':blocker})
            update(blocker)
            return 1
        persist({'status':'queued', 'attempt':attempt, 'next_attempt_at':time.time()+delay,
                 'reason':'THREAD_BUSY' if busy else 'transient file/JSON error'})
        sleep(delay)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', required=True, type=Path)
    parser.add_argument('--send-script', type=Path)
    parser.add_argument('--host-dag', default='host-dag')
    parser.add_argument('batch')
    parser.add_argument('ticket')
    parser.add_argument('thread')
    parser.add_argument('message_file', type=Path)
    parser.add_argument('blocker_on_success')
    args = parser.parse_args(argv)
    script = args.send_script or args.run/'control/parent-tools/v25-send.sh'
    identity = {'batch':args.batch, 'ticket':args.ticket, 'thread':args.thread,
                'message_file':str(public_path(args.message_file)),
                'message_digest':hashlib.sha256(args.message_file.read_bytes()).hexdigest()}
    key = hashlib.sha256(json.dumps(identity,sort_keys=True).encode()).hexdigest()[:24]
    directory = args.run/'control/parent-tools/send-retry-queue'
    directory.mkdir(parents=True,exist_ok=True)
    path = directory/(key+'.json')
    # Serialize this notification only; never hold a board/resource lock while waiting.
    import fcntl
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        def persist(value):
            atomic_json(path, {**identity, **value, 'updated_at':time.time()})
        def update(value):
            ok = value == 'delivered'
            blocker = args.blocker_on_success + ' (delivered)' if ok else value
            result = subprocess.run([args.host_dag,'--run',str(args.run),'ticket','set',
                                     '--batch',args.batch,'--ticket',args.ticket,
                                     '--state','RUNNING' if ok else 'REVIEWING','--blocker',blocker],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode:
                raise RuntimeError('ticket update failed; delivery receipt retained')
        if path.exists():
            old = read_json(path)
            if old.get('status') == 'delivered':
                update('delivered')
                return 0
            if old.get('status') == 'sending':
                update('parent send failed: uncertain previous delivery; inspect original thread before retry')
                return 1
        def send():
            result = subprocess.run(['bash',str(script),args.ticket,args.thread,str(args.message_file)],
                                    env={**os.environ,'BATCH':args.batch,'HOST_DAG_RUN':str(args.run)},
                                    capture_output=True,text=True)
            output = result.stdout + result.stderr
            # A nonzero exit must not look delivered even if stdout contains a partial response.
            return output if result.returncode == 0 else re.sub('inProgress','sendFailed',output)
        return deliver(send, update, persist)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f'parent send retry failed: {type(error).__name__}; inspect queue receipt',file=sys.stderr)
        sys.exit(1)

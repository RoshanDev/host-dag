#!/usr/bin/env python3
"""Stand-in for host-dag-codex-host; behaviour comes from files in $FAKE_HOST_DIR. No real host is touched.

send_mode: ok | hang | hang-delivered | busy | unknown-status | host-timeout | empty
read_mode: ok | hang
messages.json holds the thread's user messages, newest first.
"""
import json
import os
from pathlib import Path
import sys
import time
import subprocess

root = Path(os.environ['FAKE_HOST_DIR'])


def mode(name):
    path = root/name
    return path.read_text().strip() if path.exists() else 'ok'


def history():
    path = root/'messages.json'
    return json.loads(path.read_text()) if path.exists() else []


def land(argv):
    # Assumes one sender at a time, which the sender's per-message lock provides.
    text = Path(argv[argv.index('--message-file') + 1]).read_text()
    rows = history()
    rows.insert(0, {'id':f'msg-{len(rows) + 1}', 'role':'user', 'text':text})
    (root/'messages.json').write_text(json.dumps(rows))


argv = sys.argv[1:]
action = argv[1]
with (root/'calls.log').open('a') as log:
    log.write(action + '\n')
if os.environ.get('FAKE_NATIVE_SOCKET'):
    # Replays RPC-boundary races through the real adapter, not a guessed
    # adapter error code. The test owns the temporary Unix protocol peer.
    host = Path(__file__).resolve().parents[4]/'host-dag-codex-host.py'
    sys.exit(subprocess.call([sys.executable, str(host), '--socket', os.environ['FAKE_NATIVE_SOCKET'],
                              '--timeout', '1', *argv]))
if action == 'read':
    if mode('read_mode') == 'hang':
        time.sleep(600)
    print(json.dumps({'threadId':argv[2], 'messages':history()[:25], 'nextCursor':None}))
    sys.exit(0)
send = mode('send_mode')
reply = '--steer-if-busy' in argv
if send in {'active-reply', 'short-tool', 'turn-ended', 'turn-replaced', 'reply-lost'}:
    if not reply:
        print(json.dumps({'error': {'code': 'THREAD_BUSY', 'message': 'active turn'}}))
        sys.exit(1)
    if send in {'turn-ended', 'turn-replaced'}:
        (root/'send_mode').write_text('ok' if send == 'turn-ended' else 'active-reply')
        print(json.dumps({'error': {'code': 'TURN_CHANGED', 'message': 'observed turn ended'}}))
        sys.exit(1)
    if send == 'short-tool':
        time.sleep(0.1)
    land(argv)
    with (root/'rpc.log').open('a') as log:
        log.write('steer\n')
    if send == 'reply-lost':
        time.sleep(600)
    print(json.dumps({'threadId': argv[2], 'turn': {'id': 'active-1', 'status': 'inProgress'},
                      'delivery': {'status': 'ACCEPTED', 'method': 'steer', 'turnId': 'active-1'}}))
    sys.exit(0)
if send == 'hang':
    time.sleep(600)
if send == 'hang-delivered':
    land(argv)
    time.sleep(600)
if send == 'empty':
    sys.exit(0)
if send in {'busy', 'unknown-status', 'host-timeout'}:
    code = {'busy':'THREAD_BUSY', 'unknown-status':'UNKNOWN_STATUS', 'host-timeout':'TIMEOUT'}[send]
    print(json.dumps({'error':{'code':code, 'message':'fake'}, 'threadId':argv[2]}))
    sys.exit(1)
land(argv)
with (root/'rpc.log').open('a') as log:
    log.write('start\n')
result = {'threadId':argv[2], 'turn':{'id':'turn-1', 'status':'inProgress'}}
if reply:
    result['delivery'] = {'status': 'ACCEPTED', 'method': 'start', 'turnId': 'turn-1'}
print(json.dumps(result))

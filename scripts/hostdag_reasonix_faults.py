"""DeepSeek failure semantics and one non-secret local availability marker.

No key material, token/balance ledger, polling service or notification queue.
Notifications use the original watch alert and Telegram deferred queue.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

from hostdag_json import atomic_json

STATE_FILE = '.host-dag-deepseek-health.json'
FAULTS = {'balance', 'authentication'}
RECOVERY_PROMPT = 'Reply READY only. Do not use tools.'


def classify(event):
    """Inspect transient error data, return ONLY a fixed semantic label/code.

    Official error contract: 401 auth, 402 balance, 429 QPS/concurrency, 5xx
    service. No substring 'quota' inference and no raw request/error persistence.
    """
    if not isinstance(event, dict):
        return None
    kind = event.get('kind')
    failed = (kind in ('retrying', 'error', 'turn_failed') or event.get('err') or event.get('error') or
              event.get('is_error') or (event.get('type') == 'result' and event.get('subtype') != 'success'))
    if not failed:
        return None
    text = json.dumps(event, ensure_ascii=False)
    status = event.get('retryStatus') or event.get('httpStatus')
    if type(status) is not int:
        # Native error strings carry "status 402:" / "HTTP 401". Never infer
        # status from arbitrary numbers in a prompt, token count or key fragment.
        found = re.search(r'(?i)(?:status(?:[ _]?code)?|http(?:/\d(?:\.\d)?)?)["\\\s]*[:=]?\s*(4\d\d|5\d\d)\b', text)
        status = int(found[1]) if found else None
    if status == 429:
        label = 'rate_limit'
    elif status and status >= 500:
        label = 'service'
    elif status == 401 or (status == 403 and re.search(r'(?i)(?:api.?key|authentication).{0,40}(?:revoked|invalid|disabled|fail)', text)):
        label = 'authentication'
    elif status == 402 and re.search(r'(?i)insufficient[ _-](?:balance|quota)|balance.{0,12}(?:exhausted|insufficient)|quota[ _-]exhausted|余额不足|额度.{0,3}耗尽', text):
        label = 'balance'
    elif re.search(r'(?i)connection|network|timed? ?out|dns|socket|stream disconnected', text):
        label = 'network'
    else:
        label = 'request_failed'
    return {'kind': label, 'httpStatus': status}


def read(home):
    path = Path(home) / STATE_FILE
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {'generation': 0, 'revision': 0}


@contextmanager
def locked(home):
    home = Path(home)
    with (home / (STATE_FILE + '.lock')).open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = read(home)
        yield state
        atomic_json(home / STATE_FILE, state)


def blocked(home):
    return read(home).get('fault')


def record_failure(home, failure, ticket, notification_state=None, *, generation=None):
    if not failure or failure['kind'] not in FAULTS:
        return None
    with locked(home) as state:
        if generation is not None and generation != state.get('generation', 0):
            return None  # late response from a process using the old secret load
        state['revision'] = state.get('revision', 0) + 1
        if not state.get('fault'):
            scope = hashlib.sha256(str(Path(home).resolve()).encode()).hexdigest()[:20]
            state['fault'] = {'id': uuid.uuid4().hex, 'scope': scope, 'kind': failure['kind'],
                              'httpStatus': failure.get('httpStatus'), 'firstTicket': ticket,
                              'at': time.time(), 'generation': state.get('generation', 0),
                              'reasonixHome': str(Path(home).resolve()), 'notificationState': notification_state}
        return dict(state['fault'])


def begin_recovery(home):
    """Parent-authorized fresh CLI probe; marker/file changes alone never unblock."""
    with locked(home) as state:
        if not state.get('fault'):
            raise ValueError('No credential fault to recover; do not repeat a successful official probe')
        token = {'id': uuid.uuid4().hex, 'revision': state.get('revision', 0), 'startedAt': time.time()}
        state['recovery'] = token
        return dict(token)


def finish_recovery(home, token, result):
    if not token or result.get('status') != 'COMPLETED' or not result.get('sawResult') or result.get('credentialFailure'):
        return False
    with locked(home) as state:
        if state.get('recovery') != token or state.get('revision', 0) != token['revision']:
            return False  # a newer failure/probe wins, never replay old success
        state.pop('fault', None)
        state.pop('recovery', None)
        state['generation'] = state.get('generation', 0) + 1
        state['recoveredAt'] = time.time()
        state['recoverySessionId'] = result.get('sessionId')
        return True


def notify(state, ticket, fault, now):
    """Original watcher -> alert -> original durable Telegram queue. No transport here."""
    if not fault or fault.get('kind') not in FAULTS:
        return
    current = blocked(fault['reasonixHome'])
    if not current or current.get('id') != fault.get('id'):
        return  # stale receipt after a verified recovery
    from hostdag_self_heal import alert
    title = ('DeepSeek 官方 API 余额/额度不足' if fault['kind'] == 'balance' else 'DeepSeek 官方 API key认证失败')
    action = ('请更换或充值API key' if fault['kind'] == 'balance' else '请更新本机API key配置')
    body = f'{title}，Reasonix任务已停止重试，{action}。任务号：{fault["firstTicket"]}。'
    body += '恢复动作：由原协调者重载本机秘密配置并完成最小探针；成功后再决定原session/worktree续接，不自动重放。'
    target = dict(state, state_path=fault.get('notificationState') or state['state_path'],
                  reasonix_fault={'home': fault['reasonixHome'], 'id': fault['id']})
    alert(target, f'needs_user:reasonix:{fault["scope"]}:{fault["id"]}', title, body, now)

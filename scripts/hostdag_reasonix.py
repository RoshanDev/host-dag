"""Reasonix 2.56.0 CLI protocol. No API client, scheduler or automatic retries."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time
import tomllib
from urllib.parse import urlsplit
import hostdag_reasonix_faults as faults

MODEL = 'deepseek-flash'  # config provider name, NOT the API model
EFFORT = 'medium'
METRICS = 'reasonix-metrics.json'


def version():
    done = subprocess.run([shutil.which('reasonix') or 'reasonix', 'version', '--json'],
                          capture_output=True, text=True, stdin=subprocess.DEVNULL, timeout=10)
    if done.returncode:
        raise ValueError('Reasonix version could not be read')
    value = json.loads(done.stdout)
    if value.get('version') != 'v2.56.0':
        raise ValueError('Reasonix protocol is verified only for v2.56.0; review before using another version')
    return value


def configuration(home, model):
    """Only the explicitly named non-secret TOML; never inspect credential stores."""
    home = Path(home).expanduser().resolve(strict=True)
    config = tomllib.loads((home / 'config.toml').read_text())
    provider = next((p for p in config.get('providers', []) if p.get('name') == model), None)
    if not provider:
        raise ValueError('Reasonix --model must name an existing config provider')
    public = {k: provider.get(k) for k in ('name', 'kind', 'base_url', 'model', 'api_key_env')}
    url = urlsplit(public['base_url'] or '')
    fixture = url.scheme == 'http' and url.hostname == '127.0.0.1'
    if url.username or url.password or url.query or url.fragment:
        raise ValueError('Reasonix base URL must not contain credentials/query/fragment')
    if public['model'] != 'deepseek-v4-flash':
        raise ValueError('Reasonix pilot is Flash only; Pro is not authorized')
    official = {('openai', 'https://api.deepseek.com'), ('anthropic', 'https://api.deepseek.com/anthropic')}
    if not fixture and ((public['kind'], public['base_url']) not in official or
                        public['api_key_env'] != 'DEEPSEEK_API_KEY'):
        raise ValueError('Reasonix requires an official DeepSeek endpoint and DEEPSEEK_API_KEY env reference')
    agent = config.get('agent', {})
    delegated = [agent.get('planner_model'), agent.get('subagent_model'), *agent.get('subagent_models', {}).values()]
    if any(name and name != model for name in delegated):
        raise ValueError('Reasonix pilot cannot implicitly select another planner/subagent provider')
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', public['api_key_env'] or ''):
        raise ValueError('Reasonix api_key_env must be an environment variable name')
    return {'reasonixHome': str(home), 'configuredProvider': public, 'mockTransport': fixture,
            'credentialLoad': 'reasonix-home-dotenv',
            'configuredHostVerification': config.get('agent', {}).get('host_verification', 'unknown')}


def writable_dirs(cwd, out):
    """Directories outside --dir the executor must write: the worktree's shared
    git dir (commits/refs), the ticket's dispatch dir (contract evidence) and the
    shared pnpm store. The OS sandbox ro-binds / and only binds these back."""
    dirs = []
    probe = subprocess.run(['git', '-C', str(cwd), 'rev-parse', '--path-format=absolute', '--git-common-dir'],
                           capture_output=True, text=True)
    if probe.returncode == 0 and probe.stdout.strip():
        dirs.append(probe.stdout.strip())
    dirs.append(str(Path(out).resolve().parent))
    store = Path.home() / '.local/share/pnpm/store'
    if (Path(cwd) / 'package.json').exists() and store.is_dir():
        dirs.append(str(store))
    return [d for d in dict.fromkeys(dirs) if not Path(d).resolve().is_relative_to(Path(cwd).resolve())]


def argv(model, cwd, effort, metrics, resume=None, recovery=False, add_dirs=()):
    command = [shutil.which('reasonix') or 'reasonix', 'run', '--dir', cwd, '--model', model,
               '--effort', effort, '--output-format', 'stream-json', '--metrics', str(metrics),
               '--fail-on-unverified', '--permission-mode', 'read-only' if recovery else 'bypassPermissions']
    if recovery:
        command += ['--max-steps', '1']
    else:
        for directory in add_dirs:
            command += ['--add-dir', directory]
    if resume:
        command += ['--resume', resume]
    return command  # prompt travels on stdin; secrets never become arguments


def redact(text, secret=''):
    if secret:
        text = text.replace(secret, '[REDACTED]')
        text = text.replace(json.dumps(secret)[1:-1], '[REDACTED]')
    text = re.sub(r'(?i)(Bearer\s+)[^\s"\\]+', r'\1[REDACTED]', text)
    return re.sub(r'\bsk-[A-Za-z0-9_-]+', '[REDACTED]', text)


def log_line(line, secret=''):
    """Keep native control events + final result, not token-fragment payloads.

    An echoed key may span several text events. Per-line replacement alone is
    insufficient, so omit incremental content and redact the assembled result.
    This is a documented redaction projection, never invented CLI events.
    """
    try:
        event = json.loads(line)
    except ValueError:
        return ''  # launch diagnostics may include a request object or partial key
    if not isinstance(event, dict):
        return ''
    if event.get('type') == 'result' and (event.get('is_error') or event.get('subtype') != 'success'):
        # Classify the raw response in memory, never persist its message/object.
        event = {k: v for k, v in event.items() if k in ('type', 'subtype', 'is_error', 'session_id',
                 'usage', 'duration_ms', 'num_turns', 'permission_denials', 'completion', 'readiness')}
        line = json.dumps(event, ensure_ascii=False) + '\n'
    if isinstance(event, dict) and event.get('type') != 'result':
        fields = {'kind', 'sessionId', 'turnId', 'seq', 'status', 'modelRef', 'usage', 'completion',
                  'retryAttempt', 'retryMax', 'retryScope', 'retryCause', 'retryStatus', 'retryDelayMs',
                  'cancelled', 'level', 'streamAttempt'}
        event = {k: v for k, v in event.items() if k in fields}
        line = json.dumps(event, ensure_ascii=False) + '\n'
    return redact(line, secret)


def positive(value):
    return value if type(value) in (int, float) and value > 0 else None


def final_result(log, quota_pattern=None, metrics=None):
    result, session, model, errors, retries, failures = {}, None, None, [], [], []
    cancelled = False
    if Path(log).exists():
        for line in Path(log).read_text(errors='replace').splitlines():
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if not isinstance(e, dict):
                continue
            failure = faults.classify(e)
            if failure:
                failures.append(failure)
            session = e.get('session_id') or e.get('sessionId') or session
            if e.get('kind') == 'turn_started':
                model = e.get('modelRef') or model
            if e.get('type') == 'result':
                result = e
            if e.get('kind') == 'retrying':
                retries.append(e)
            if e.get('kind') in ('error', 'turn_failed') or e.get('error') or e.get('err') or e.get('is_error'):
                errors.append(e)
            if e.get('kind') == 'turn_done':
                cancelled |= e.get('cancelled') is True or e.get('status') == 'cancelled'
    m = {}
    if metrics and Path(metrics).exists():
        try:
            m = json.loads(Path(metrics).read_text())
        except (OSError, ValueError):
            pass
    text = str(result.get('result') or '')
    completion = result.get('completion') or {}
    verdict = completion.get('verdict') or m.get('completion_verdict', 'unknown')
    unverified = bool(result.get('readiness') or result.get('unverified_by') or result.get('permission_denials') or
                      (verdict != 'unknown' and verdict != 'complete') or
                      completion.get('review') in ('warned', 'failed', 'unavailable') or
                      completion.get('checks_failed') or completion.get('checks_suppressed') or
                      completion.get('gap_kinds') or completion.get('criteria_rewritten'))
    failed = bool(errors or retries or cancelled or unverified or result.get('is_error') or
                  result.get('subtype') != 'success')
    usage = result.get('usage') or {target: m.get(source) for target, source in
        [('input_tokens', 'prompt_tokens'), ('output_tokens', 'completion_tokens'),
         ('cache_read_input_tokens', 'cache_hit_tokens'), ('cache_creation_input_tokens', 'cache_miss_tokens')]}
    measured = any(positive(usage.get(k)) for k in ('input_tokens', 'output_tokens'))
    # cache_creation_input_tokens in this CLI means cache MISS, not cache writes.
    actual = {target: (usage[source] if measured and type(usage.get(source)) in (int, float) and usage[source] >= 0 else None) for target, source in
              [('inputTokens', 'input_tokens'), ('outputTokens', 'output_tokens'),
               ('cacheHitTokens', 'cache_read_input_tokens'), ('cacheMissTokens', 'cache_creation_input_tokens')]}
    cost = positive(m.get('cost')) if m.get('cost_complete') and m.get('currency') else None
    return {'sawResult': bool(result and text.strip()), 'isError': failed, 'resultText': text[:4000],
            'sessionId': session, 'reportedModel': model, 'actualApiModel': None, 'reportedEffort': None,
            'actualContextWindow': None, 'actualUsage': actual, 'reportedCost': cost,
            'costCurrency': m.get('currency') or None, 'costEstimated': m.get('estimated'),
            'usageStatus': 'reported' if any(v is not None for v in actual.values()) else 'unknown',
            'costStatus': 'unknown' if cost is None else 'cli-reported-not-invoice',
            'quotaExhausted': failed and any(f['kind'] == 'balance' for f in failures),
            'credentialFailure': next((f for f in failures if f['kind'] in faults.FAULTS), None),
            'requestFailure': failures[-1] if failures else None,
            'serviceError': any(f['kind'] == 'service' for f in failures),
            'hostVerification': {'unverified': unverified, 'readiness': result.get('readiness'),
                                 'unverifiedBy': result.get('unverified_by'),
                                 'completion': result.get('completion'),
                                 'verdict': verdict},
            'providerCancelled': cancelled, 'rawResult': result,
            'usageNote': 'Reported zero cache hits remain zero when usage is measured; missing/unmeasured and zero cost are unknown. CLI cost is not an API invoice; no Qoder credit conversion.'}


def supervise(record, out, write_json, quota_pattern, now_iso):
    """Drain/redact before persistence; stop on retry intent and reap the process group."""
    out = Path(out)
    log, errlog, metrics = out / 'cli.ndjson', out / 'cli.ndjson.stderr', out / METRICS
    command = argv(record['model'], record['cwd'], record['requestedEffort'], metrics, record.get('resumeSessionId'),
                   recovery=bool(record.get('recoveryProbe')), add_dirs=writable_dirs(record['cwd'], out))
    # Native CLI loads <REASONIX_HOME>/.env. The wrapper never reads that file
    # or the real credential value. Env injection remains fixture-only.
    key_name = record['configuredProvider']['api_key_env']
    env = {k: os.environ[k] for k in ('HOME', 'PATH', 'LANG', 'TMPDIR', 'XDG_RUNTIME_DIR') if k in os.environ}
    secret = os.environ.get(key_name, '') if record.get('mockTransport') else ''
    env.update(REASONIX_HOME=record['reasonixHome'], DO_NOT_TRACK='1')
    if secret:
        env[key_name] = secret
    if record.get('mockTransport'):
        env.update(HTTP_PROXY='http://127.0.0.1:1', HTTPS_PROXY='http://127.0.0.1:1',
                   ALL_PROXY='http://127.0.0.1:1', NO_PROXY='127.0.0.1,localhost')
    cancelled, retry_stopped = threading.Event(), threading.Event()
    credential_stopped = threading.Event()
    observed_failures, credential_fault = [], faults.blocked(record['reasonixHome'])
    credential_generation = faults.read(record['reasonixHome']).get('generation', 0)
    signals_sent, drain_errors = [], []
    child = None

    def kill_group(sig):
        if child:
            try:
                os.killpg(child.pid, sig)
                signals_sent.append(sig.name)
            except ProcessLookupError:
                pass

    def on_signal(*_):
        cancelled.set()
        kill_group(signal.SIGINT)

    def drain(pipe, path, inspect=False):
        nonlocal credential_fault
        try:
            with path.open('w') as handle:
                for line in pipe:
                    try:
                        raw = json.loads(line) if inspect else {'kind': 'error', 'err': line}
                    except ValueError:
                        raw = {'kind': 'error', 'err': line}
                    failure = faults.classify(raw)
                    if failure and (inspect or failure['kind'] != 'request_failed'):
                        observed_failures.append(failure)
                        if failure['kind'] in faults.FAULTS:
                            credential_fault = faults.record_failure(record['reasonixHome'], failure,
                                record['ticket'], record.get('notificationState'), generation=credential_generation)
                            credential_stopped.set()
                            kill_group(signal.SIGINT)
                    clean = log_line(line, secret) if inspect else '[Reasonix diagnostic omitted; see requestFailure]\n'
                    handle.write(clean); handle.flush()
                    if inspect:
                        try:
                            event = json.loads(clean)
                        except ValueError:
                            continue
                        if isinstance(event, dict) and event.get('kind') == 'retrying':
                            retry_stopped.set()
                            kill_group(signal.SIGINT)
        except Exception as exc:
            drain_errors.append(type(exc).__name__)
            kill_group(signal.SIGTERM)
        finally:
            pipe.close()

    previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
    code = 127
    try:
        if credential_fault and not record.get('recoveryProbe'):
            raise OSError('Reasonix credential fault remains blocked; parent recovery probe required')
        child = subprocess.Popen(command, cwd=record['cwd'], env=env, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                 errors='replace', start_new_session=True)
        for sig in previous:
            signal.signal(sig, on_signal)
        readers = [threading.Thread(target=drain, args=(child.stdout, log, True)),
                   threading.Thread(target=drain, args=(child.stderr, errlog))]
        for reader in readers:
            reader.start()
        try:
            child.stdin.write(faults.RECOVERY_PROMPT if record.get('recoveryProbe') else Path(record['promptFile']).read_text())
            child.stdin.close()
        except BrokenPipeError:
            pass
        stop_at = None
        checked_at = 0
        while True:
            try:
                code = child.wait(timeout=0.2)
                break
            except subprocess.TimeoutExpired:
                if not record.get('recoveryProbe') and time.monotonic() - checked_at > 1:
                    checked_at = time.monotonic()
                    credential_fault = faults.blocked(record['reasonixHome']) or credential_fault
                    if credential_fault:
                        credential_stopped.set()
                        kill_group(signal.SIGINT)
                if cancelled.is_set() or retry_stopped.is_set() or credential_stopped.is_set() or drain_errors:
                    stop_at = stop_at or time.monotonic()
                    if time.monotonic() - stop_at > 5:
                        kill_group(signal.SIGKILL)
        # The npm launcher can exit before the Go child/tool processes. Reap its
        # entire group even on normal exit; systemd also owns the cgroup.
        for reader in readers:
            reader.join(timeout=2)
        kill_group(signal.SIGTERM)
        kill_group(signal.SIGKILL)
        for reader in readers:
            reader.join(timeout=2)
        if any(reader.is_alive() for reader in readers):
            drain_errors.append('ReaderIncomplete')
    except OSError as exc:
        errlog.write_text('Reasonix launch blocked/failed; no raw exception persisted\n')
    finally:
        kill_group(signal.SIGKILL)
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    summary = final_result(log, quota_pattern, metrics)
    failure = next((f for f in observed_failures if f['kind'] in faults.FAULTS), None)
    # A fresh recovery may ignore the pre-existing incident, but never its own
    # cancellation, collection errors or newly observed provider failure.
    recovery = bool(record.get('recoveryProbe'))
    if credential_fault and not failure and not recovery:
        failure = {k: credential_fault[k] for k in ('kind', 'httpStatus')}
    summary.update(credentialFailure=failure, requestFailure=failure or (observed_failures[-1] if observed_failures else summary['requestFailure']),
                   quotaExhausted=bool(failure and failure['kind'] == 'balance'),
                   serviceError=summary['serviceError'] or any(f['kind'] == 'service' for f in observed_failures),
                   credentialFault=credential_fault)
    if observed_failures or (credential_fault and not recovery) or cancelled.is_set() or drain_errors or retry_stopped.is_set():
        summary['isError'] = True
    stderr = errlog.read_text(errors='replace')[-4000:] if errlog.exists() else ''
    status = ('CANCELLED' if cancelled.is_set() or (summary['providerCancelled'] and not retry_stopped.is_set() and not credential_fault)
              else 'QUOTA_EXHAUSTED' if summary['quotaExhausted']
              else 'AUTH_FAILED' if failure and failure['kind'] == 'authentication'
              else 'COMPLETED' if code == 0 and summary['sawResult'] and not summary['isError'] and not drain_errors
              else 'FAILED')
    metadata = {k: record.get(k) for k in ('ticket', 'provider', 'model', 'cliVersion', 'configuredProvider',
                'requestedContextWindow', 'requestedEffort', 'reasonixHome', 'mockTransport', 'cwd',
                'resumeSessionId', 'resumeFrom', 'startedAt', 'configuredHostVerification', 'credentialLoad', 'recoveryProbe')}
    result = {**metadata, **summary, 'status': status, 'exitCode': code,
               'credentialGeneration': credential_generation,
               'stderrTail': stderr[-800:], 'finishedAt': now_iso(), 'retryPolicy': 'return-to-parent',
               'retryStopped': retry_stopped.is_set(), 'processCleanup': {'pid': child.pid if child else None,
                'signals': signals_sent, 'drainErrors': drain_errors}, 'automaticAdoption': False}
    if record.get('recoveryProbe'):
        # Use the single supervisor verdict above. Rebuilding it from a native
        # success event here would erase CANCELLED/FAILED collection outcomes.
        result['credentialRecovered'] = faults.finish_recovery(record['reasonixHome'], record.get('recoveryProbe'), result)
        if result['credentialRecovered']:
            result['credentialFault'] = None
        elif result['status'] == 'COMPLETED':
            result.update(status='FAILED', isError=True, recoverySuperseded=True)
    write_json(out / 'cli-result.json', result)
    return 0

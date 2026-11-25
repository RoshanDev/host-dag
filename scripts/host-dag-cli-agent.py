#!/usr/bin/env python3
"""Headless Cursor / Claude / Qoder / Reasonix CLI dispatch for the Host DAG.

`run` writes <out>/cli-run.json and starts `supervise` in its own transient
user systemd unit, so the dispatching coordinator can end its turn. The
supervisor streams the agent's stream-json output to <out>/cli.ndjson and
writes <out>/cli-result.json when the agent exits. host-dag-host-watch reads
those files (provider cursor|claude|qoder|reasonix); it never talks to the agent itself.
"""
from __future__ import annotations

from hostdag_json import public_path

import argparse
import datetime as dt
import json
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

import hostdag_review_required_checks as review_checks
import hostdag_token_guard as token_guard
import hostdag_dispatch_guard as review_budget
import hostdag_qoder as qoder
import hostdag_reasonix as reasonix
import hostdag_claude_tmux as claude_tmux
import hostdag_review_required_checks as review_checks
from hostdag_cli_watch import CLI_PROVIDERS

PROVIDERS = CLI_PROVIDERS
RUN_FILE = "cli-run.json"
RESULT_FILE = "cli-result.json"
LOG_FILE = "cli.ndjson"
QUOTA_PATTERN = re.compile(
    r"usage limit|rate[ -]?limit|quota|out of credits|insufficient credits|limit (?:has been )?reached|"
    r"exceeded your|upgrade your plan|spend limit",
    re.IGNORECASE,
)


def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path: Path, value: dict[str, Any]) -> None:
    temp = path.with_suffix(path.suffix + ".pending")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=1) + "\n")
    os.replace(temp, path)


def agent_argv(provider: str, model: str, cwd: str, prompt: str,
               context_window: int = qoder.CONTEXT_WINDOW, effort: str = qoder.EFFORT) -> tuple[list[str], str | None]:
    """Return argv and the stdin payload. Each provider streams one JSON event per line."""
    if provider == "cursor":
        binary = shutil.which("cursor-agent") or str(Path.home() / ".local/bin/cursor-agent")
        return [binary, "-p", "--output-format", "stream-json", "--force", "--trust",
                "--workspace", cwd, "--model", model, prompt], None
    if provider == "qoder":
        return qoder.argv(model, cwd, prompt, context_window, effort), None
    if provider != "claude":
        raise ValueError(f"Unknown CLI provider: {provider}")
    binary = shutil.which("claude") or "claude"
    return [binary, "-p", "--model", model, "--permission-mode", "bypassPermissions",
            "--output-format", "stream-json", "--verbose"], prompt


def final_result(log: Path, provider: str = "claude") -> dict[str, Any]:
    """The last `result` event the CLIs emit, plus a quota-exhaustion scan of the tail."""
    if provider == "qoder":
        return qoder.final_result(log, QUOTA_PATTERN)
    if provider == "reasonix":
        return reasonix.final_result(log, QUOTA_PATTERN, log.with_name(reasonix.METRICS))
    result: dict[str, Any] = {}
    tail: list[str] = []
    if log.exists():
        with log.open("r", errors="replace") as handle:
            for line in handle:
                tail = (tail + [line])[-40:]
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict) and event.get("type") == "result":
                    result = event
    text = str(result.get("result") or "")
    scan = text if result else "".join(tail)
    return {
        "resultText": text[:4000],
        "isError": bool(result.get("is_error")) or result.get("subtype") not in (None, "success"),
        "sessionId": result.get("session_id") or result.get("chatId"),
        "sawResult": bool(result),
        "quotaExhausted": (bool(result.get('quota_exhausted')) if result.get('transport') == 'tmux' else
                           bool(QUOTA_PATTERN.search(scan)) and (not result or bool(result.get("is_error")))),
        **({'transportReason': result.get('reason'), 'processExitCode': result.get('process_exit_code')}
           if result.get('transport') == 'tmux' else {}),
    }


def cmd_run(args: argparse.Namespace) -> int:
    reasonix_record = {}
    if args.provider == 'reasonix':
        if not args.reasonix_pilot or args.role != 'executor':
            raise ValueError('Reasonix requires --reasonix-pilot and --role executor; parent authorizes each task')
        if args.replace or args.token_guard == 'enforce' or args.guard_retry_of or args.guard_handoff:
            raise ValueError('Reasonix requires fresh evidence; token enforcement/handoff is unsupported')
        if not args.reasonix_home:
            raise ValueError('Reasonix requires an explicit --reasonix-home config/state directory')
        if (Path(args.cwd) / 'reasonix.toml').exists():
            raise ValueError('Reasonix pilot needs parent review of project reasonix.toml overrides before dispatch')
        reasonix_record = reasonix.configuration(args.reasonix_home, args.model)
        if not reasonix_record['mockTransport'] and args.reasonix_env_file:
            raise ValueError('Official Reasonix loads its own local .env; do not override it with an EnvironmentFile')
        if not reasonix_record['mockTransport'] and not args.run:
            raise ValueError('Reasonix official dispatch requires the original --run for credential-fault notification')
        if args.run:
            # Resolve the original run through the existing host view, never a
            # new watcher/notification queue. No live state is written here.
            import importlib.util
            spec = importlib.util.spec_from_file_location('reasonix_run_view', HOST_VIEW)
            hv = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(hv)
            reasonix_record['notificationState'] = str(hv.find_run(args.run) / 'watch.json')
        if reasonix.faults.blocked(reasonix_record['reasonixHome']) and not args.reasonix_recovery_probe:
            raise ValueError('Reasonix credential is blocked across tickets; parent must reload local secrets and validate a recovery probe')
        if args.reasonix_recovery_probe and args.resume_from:
            raise ValueError('Credential recovery is a fresh minimal probe, never a resumed business task')
        reasonix_record.update(cliVersion=reasonix.version(), requestedEffort=args.effort or reasonix.EFFORT,
                               requestedContextWindow=args.context_window, contextRequestApplied=False,
                               sessionPersistence=True, tokenGuard='unsupported', role=args.role,
                               logFormat='Reasonix stream-json redaction projection; incremental payloads omitted')
        if args.reasonix_env_file:
            reasonix_record['credentialEnvFile'] = str(public_path(args.reasonix_env_file))
        if args.resume_from:
            prior_dir = public_path(args.resume_from)
            prior = json.loads((prior_dir / RUN_FILE).read_text())
            result = json.loads((prior_dir / RESULT_FILE).read_text())
            if (prior.get('provider') != 'reasonix' or unit_active(prior.get('unit', '')) or
                    result.get('status') not in ('COMPLETED', 'FAILED', 'CANCELLED', 'QUOTA_EXHAUSTED', 'AUTH_FAILED') or
                    not result.get('sessionId') or prior.get('model') != args.model or
                    prior.get('configuredProvider') != reasonix_record['configuredProvider'] or
                    prior.get('reasonixHome') != reasonix_record['reasonixHome'] or
                    Path(prior['cwd']).resolve() != Path(args.cwd).resolve()):
                raise ValueError('Resume requires the same Reasonix model/config/cwd, terminal receipt and inactive prior unit')
            reasonix_record.update(resumeFrom=str(prior_dir), resumeSessionId=result['sessionId'])
    if args.provider == 'qoder':
        args.context_window = args.context_window or qoder.CONTEXT_WINDOW
        args.effort = args.effort or qoder.EFFORT
    if args.provider == "qoder":
        if not args.qoder_simple or args.role != "executor":
            raise ValueError("Qoder requires --qoder-simple and --role executor: bounded T0/T1 work only")
        if args.token_guard == "enforce" or args.guard_retry_of or args.guard_handoff:
            raise ValueError("Qoder token guard enforcement/handoff is unsupported; actual token usage is unknown")
        if args.replace:
            raise ValueError("Qoder attempts require fresh evidence and parent review; --replace is disabled")
    out = public_path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    existing=json.loads((out/RUN_FILE).read_text()) if (out/RUN_FILE).exists() else None
    if existing and not args.replace and not (args.role=='reviewer' and existing.get('reviewBudget')):
        print(json.dumps({"ok": False, "error": f"{out / RUN_FILE} exists; use a new --out or --replace"}))
        return 2
    if existing and existing.get('guardMode') == 'enforce' and args.role!='reviewer':
        raise ValueError('Enforced retries require a new --out and --guard-retry-of / --guard-handoff')
    cwd = str(Path(args.cwd).resolve(strict=True))
    prompt_file = str(public_path(args.prompt_file))
    prompt = Path(prompt_file).read_text()
    review_selection = None
    # Hard hook (TOOL-KK-PY37-GUARD-HOOK): every review dispatch, recognised by role, an explicit
    # --review-axis or a review-* evidence path, gets the guards its covered file patterns require.
    review_reasons = review_checks.review_reasons(args.role, args.review_axis, (args.out, prompt_file))
    args.review_axis = args.review_axis or 'spec'
    if review_reasons:
        # Fail closed (TOOL-GUARD-FAIL-CLOSED): killed/timed-out/garbage guard = INFRA_ERROR, no budget reserved.
        prompt, review_selection = review_budget.review_required_checks(prompt, cwd, args.review_base, args.ticket, review_reasons)
    requested_prompt=prompt
    guard_config = None
    if args.provider not in ("qoder", "reasonix"):
        guard_config, prompt = token_guard.prepare(args, prompt, run=args.run, ticket=args.ticket)
    review_grant=None
    review_root=args.run or os.environ.get('HOST_DAG_RUN')
    if args.role=='reviewer':
        if not review_root:raise ValueError('reviewer requires --run for budget identity')
        import hashlib
        fingerprint=hashlib.sha256(json.dumps([requested_prompt,cwd,args.provider,args.model,args.effort,args.context_window,args.review_axis]).encode()).hexdigest()
        token=args.review_token or hashlib.sha256((str(out)+fingerprint).encode()).hexdigest()
        if existing and existing.get('reviewBudget',{}).get('token')!=token:
            raise ValueError('review retry needs the original token or a new --out')
        review_grant=review_budget.reserve_review(review_root,ticket=args.ticket,batch=args.batch,axis=args.review_axis,token=token,fingerprint=fingerprint)
        if review_grant and review_grant.get('replay'):
            print(json.dumps(review_grant['receipt']));return 0
    cli_prefix = os.environ.get("HOST_DAG_CLI_UNIT_PREFIX") or "host-dag-cli"
    unit = f"{cli_prefix}-{args.provider}-{re.sub(r'[^A-Za-z0-9]+', '-', args.ticket)}-{dt.datetime.now():%m%d%H%M%S}"
    if review_grant:
        unit += '-' + hashlib.sha256(token.encode()).hexdigest()[:12]
    if args.provider == 'reasonix':
        import uuid
        unit += '-' + uuid.uuid4().hex[:8]
    (out / RESULT_FILE).unlink(missing_ok=True)
    record = {
        "ticket": args.ticket, "provider": args.provider, "model": args.model, "cwd": cwd,
        "promptFile": prompt_file, "unit": unit, "log": str(out / LOG_FILE),
        "result": str(out / RESULT_FILE), "startedAt": now_iso(), "status": "STARTING",
    }
    if args.provider == "claude":
        # User 2026-11-11: 「claude -p 改 tmux」. Token-guard enforce needs stream-json steering -> stays -p.
        record["transport"] = "print" if args.token_guard == "enforce" else args.claude_transport
    if review_grant:
        record['reviewBudget']={k:review_grant[k] for k in ('token','round','axis','group')}
    if review_selection is not None:
        record['reviewRequiredChecks'] = review_selection
        if review_selection['required']:
            review_prompt = out / 'review-prompt.md'
            review_prompt.write_text(prompt)
            record['promptFile'] = str(review_prompt)
    record.update(reasonix_record)
    if args.provider == 'reasonix' and args.reasonix_recovery_probe:
        record['recoveryProbe'] = reasonix.faults.begin_recovery(record['reasonixHome'])
    if args.provider == "qoder":
        record.update(requestedContextWindow=args.context_window, requestedEffort=args.effort,
                      role=args.role, qoderSimple=True, cliVersion=qoder.version(),
                      sessionPersistence=True, tokenGuard="unsupported-usage-unknown")
    if guard_config:
        # Keep a retry's minimal handoff in the actual submitted prompt, without
        # modifying the caller's contract file.
        guarded_prompt = out / 'guard-prompt.md'
        guarded_prompt.write_text(prompt)
        record['promptFile'] = str(guarded_prompt)
        record['guardIdentity'] = unit
        record['guardMode'] = guard_config['mode']
        token_guard.Guard.create(identity=unit, cwd=cwd, contract=prompt, publish=True,
                                 exclude=[out / name for name in
                                          (RUN_FILE, RESULT_FILE, LOG_FILE, LOG_FILE + '.stderr', 'guard-prompt.md', 'review-prompt.md')],
                                 **guard_config)
    if args.provider == 'reasonix':
        # Exclusive first publication: concurrent dispatches cannot share an out.
        with (out / RUN_FILE).open('x') as handle:
            json.dump(record, handle, ensure_ascii=False, indent=1)
            handle.write('\n')
    else:
        write_json(out / RUN_FILE, record)
    argv = ["systemd-run", "--user", "--collect", f"--unit={unit}", f"--property=WorkingDirectory={cwd}",
            sys.executable, str(Path(__file__).resolve()), "supervise", "--out", str(out)]
    if args.provider == 'reasonix':
        properties = ['--property=MemoryMax=8G', '--property=KillMode=control-group', '--property=TimeoutStopSec=15']
        if args.reasonix_env_file:
            properties.append('--property=EnvironmentFile=' + str(public_path(args.reasonix_env_file)))
        argv[4:4] = properties
    try:
        if review_grant:review_budget.claim_review(review_root,review_grant['token'],fingerprint)
        started = subprocess.run(argv, capture_output=True, text=True)
    except review_budget.DispatchError:
        review_budget.settle_review(review_root,review_grant,'retryable');raise
    except BaseException:
        review_budget.settle_review(review_root,review_grant,'uncertain');raise
    if started.returncode != 0:
        review_budget.settle_review(review_root,review_grant,'retryable')
        record.update({"status": "START_FAILED", "error": started.stderr.strip()[:400]})
        write_json(out / RUN_FILE, record)
        print(json.dumps({"ok": False, **record}, ensure_ascii=False))
        return 1
    review_budget.settle_review(review_root,review_grant,'delivered',{'ok':True,'unit':unit,'runFile':str(out/RUN_FILE),'reviewBudget':record.get('reviewBudget')})
    record["status"] = "RUNNING"
    write_json(out / RUN_FILE, record)
    registered = register_executor(args, unit, out / RUN_FILE) if args.register else {"registered": False,
                                                                                         "reason": "--no-register",
                                                                                         "sideRun": True}
    if registered.get("sideRun"):  # the board and the patrol leave a declared side run alone
        record["sideRun"] = True
        write_json(out / RUN_FILE, record)
    print(json.dumps({"ok": True, "runFile": str(out / RUN_FILE), **record, "registration": registered},
                     ensure_ascii=False))
    return 0


HOST_VIEW = Path(__file__).resolve().parent / "host-dag-view.py"
SKIP_STATUSES = {"ACCEPTED", "SUPERSEDED", "CANCELLED"}


def ticket_batch(run_dir: Path, ticket: str) -> str | None:
    """The batch holding the ticket's live copy: the last batch that has it open, else the last that has it."""
    try:
        run = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    hits: list[tuple[bool, str]] = []
    for name, body in (run.get("batches") or {}).items():
        tickets = (body or {}).get("tickets") or {}
        if isinstance(tickets, list):
            tickets = {t.get("id"): t for t in tickets if isinstance(t, dict)}
        rec = tickets.get(ticket)
        if isinstance(rec, dict):
            hits.append((str(rec.get("status") or "").upper() not in SKIP_STATUSES, name))
    open_hits = [name for live, name in hits if live]
    return (open_hits or [name for _, name in hits] or [None])[-1]


def unit_active(unit: str) -> bool:
    cli_prefix = os.environ.get("HOST_DAG_CLI_UNIT_PREFIX") or "host-dag-"
    if not (unit.startswith("host-dag-") or unit.startswith(cli_prefix)):
        return False
    try:
        done = subprocess.run(["systemctl", "--user", "is-active", unit + ".service"],
                              capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return False
    return done.stdout.strip() in ("active", "activating", "reloading")


def side_run_reason(run_dir: Path, batch: str, ticket: str) -> str | None:
    """Why this dispatch would displace a main line still running on the ticket, or None.

    A declared side run will not displace an active main execution."""
    try:
        run = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
        rec = ((run.get("batches") or {}).get(batch) or {}).get("tickets", {}).get(ticket) or {}
    except (OSError, ValueError, AttributeError):
        return None
    current = str(rec.get("executor") or "")
    if current and unit_active(current):
        return f"current executor {current} is still running"
    if str(rec.get("state") or "").upper() == "WAITING_RESOURCE" and rec.get("wait_holder"):
        return f"ticket is WAITING_RESOURCE held by {rec['wait_holder']}"
    return None


def register_executor(args: argparse.Namespace, unit: str, run_file: Path) -> dict[str, Any]:
    """Write executor/evidence/model onto the ticket at dispatch, so the watcher tracks the new round.

    2026-10-22: five rounds dispatched at 09:50 updated nothing on their tickets; the watcher tracked none of
    them and two finished unnoticed. The user asked for registration to happen in the dispatch itself.
    A failure here never fails the dispatch: the unit is already running; the reason is reported instead."""
    try:
        sys.path.insert(0, str(HOST_VIEW.parent))
        import importlib.util
        spec = importlib.util.spec_from_file_location("hostdag_host_view_for_dispatch", HOST_VIEW)
        hv = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hv)
        run_dir = hv.find_run(args.run)
    except Exception as exc:  # noqa: BLE001
        return {"registered": False, "reason": f"run dir: {exc}"[:300]}
    batch = args.batch or ticket_batch(run_dir, args.ticket)
    if batch is None:
        return {"registered": False, "reason": f"{args.ticket} is not a ticket in {run_dir}/run.json"}
    if not args.force_register:
        side = side_run_reason(run_dir, batch, args.ticket)
        if side:
            return {"registered": False, "batch": batch, "sideRun": True,
                    "reason": side + "; a side run is not registered "
                    "(open its own ticket, or pass --force-register if this run replaces the main line)"}
    argv = [sys.executable, str(HOST_VIEW), "--run", str(run_dir), "ticket", "set", "--batch", batch,
            "--ticket", args.ticket, "--executor", unit, "--evidence", str(run_file), "--model", args.model]
    if args.provider in ("qoder", "reasonix"):
        argv += ["--effort", args.effort or reasonix.EFFORT]
    done = subprocess.run(argv, capture_output=True, text=True, timeout=60)
    if done.returncode != 0:
        return {"registered": False, "batch": batch, "reason": (done.stderr or done.stdout).strip()[-300:]}
    return {"registered": True, "batch": batch, "executor": unit, "evidence": str(run_file)}


def cmd_supervise(args: argparse.Namespace) -> int:
    out = Path(args.out)
    record = json.loads((out / RUN_FILE).read_text())
    if record['provider'] == 'reasonix':
        # The provider owns its native protocol, environment and process tree.
        # Original watch/registration/B5 still consume the same run/result files.
        import fcntl
        with (out / 'supervise.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if (out / RESULT_FILE).exists():
                raise ValueError('Reasonix receipt already exists; never replay this attempt')
            return reasonix.supervise(record, out, write_json, QUOTA_PATTERN, now_iso)
    prompt = Path(record["promptFile"]).read_text()
    options = {}
    if record["provider"] == "qoder":
        options = {"context_window": record["requestedContextWindow"], "effort": record["requestedEffort"]}
    argv, stdin = agent_argv(record["provider"], record["model"], record["cwd"], prompt, **options)
    log = out / LOG_FILE
    cancelled = False

    if record["provider"] == "claude" and record.get("transport") == "tmux":
        code, cancelled = claude_tmux.supervise(argv, record["cwd"], prompt, log, out / (LOG_FILE + ".stderr"),
                                                record["unit"], QUOTA_PATTERN)
        summary = final_result(log, "claude")
        if not summary["sawResult"] and (out / (LOG_FILE + ".stderr")).exists():
            summary["stderrTail"] = (out / (LOG_FILE + ".stderr")).read_text(errors="replace")[-800:]
        if record.get("guardIdentity"):
            with token_guard.locked(record["guardIdentity"]) as guard:
                if guard is not None:
                    guard.state.update(token_guard.read_usage(log, guard.state, "claude"))
                    if cancelled or code != 0 or not summary["sawResult"] or summary["isError"]:
                        guard.failed()
        status = ("CANCELLED" if cancelled else "QUOTA_EXHAUSTED" if summary["quotaExhausted"] else
                  "COMPLETED" if code == 0 and summary["sawResult"] and not summary["isError"] else "FAILED")
        write_json(out / RESULT_FILE, {"status": status, "exitCode": code, "finishedAt": now_iso(),
                                       "supervisorExitCode": code, "transport": "tmux", **summary})
        return 0

    if record.get('guardIdentity'):
        stream = record['provider'] == 'claude' and record['guardMode'] == 'enforce'
        if stream:
            argv += ['--input-format', 'stream-json']
        if record['provider'] != 'claude' and record['guardMode'] == 'enforce':
            raise ValueError('Enforcement is supported for Claude CLI executors only')
        try:
            code = token_guard.supervise(argv, record['cwd'], stdin or '', log,
                                        out / (LOG_FILE + '.stderr'), record['guardIdentity'], stream_input=stream)
        except InterruptedError:
            code, cancelled = 130, True
        summary = final_result(log, record["provider"])
        if not summary['sawResult'] and (out / (LOG_FILE + '.stderr')).exists():
            err = (out / (LOG_FILE + '.stderr')).read_text(errors='replace')[-4000:]
            summary['quotaExhausted'] = summary['quotaExhausted'] or bool(QUOTA_PATTERN.search(err))
            summary['stderrTail'] = err[-800:]
        with token_guard.locked(record['guardIdentity']) as guard:
            if cancelled or code != 0 or not summary['sawResult'] or summary['isError']:
                guard.failed()
            if guard.blocked:
                summary.update(resultText='TOKEN_GUARD_NEW_SESSION_REQUIRED: ' + guard.instruction(),
                               handoff=str(guard.handoff), retry_count=guard.state['retry_count'])
                # Existing CLI watcher reports FAILED; it never resumes CLI
                # sessions, so the coordinator receives the fresh-session request.
                status = 'FAILED'
            elif cancelled:
                status = 'CANCELLED'
            elif summary['quotaExhausted']:
                status = 'QUOTA_EXHAUSTED'
            else:
                status = 'COMPLETED' if code == 0 and summary['sawResult'] and not summary['isError'] else 'FAILED'
        write_json(out / RESULT_FILE, dict(status=status, exitCode=code, finishedAt=now_iso(), **summary))
        return 0

    def on_signal(_signum: int, _frame: Any) -> None:
        nonlocal cancelled
        cancelled = True
        if child.poll() is None:
            child.send_signal(signal.SIGINT)

    with log.open("ab") as stdout, (out / (LOG_FILE + ".stderr")).open("ab") as stderr:
        try:
            child = subprocess.Popen(argv, cwd=record["cwd"], stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr)
        except OSError as exc:
            if record["provider"] != "qoder":
                raise
            write_json(out / RESULT_FILE, {**record, **qoder.final_result(log, QUOTA_PATTERN),
                       "status": "FAILED", "exitCode": 127, "finishedAt": now_iso(),
                       "stderrTail": str(exc), "retryPolicy": "return-to-parent"})
            return 0
        previous = {sig: signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)}
        try:
            for sig in previous:
                signal.signal(sig, on_signal)
            if stdin and child.stdin:
                child.stdin.write(stdin.encode())
                child.stdin.close()
            code = child.wait()
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    summary = final_result(log, record["provider"])
    if (not summary["sawResult"] or (record["provider"] == "qoder" and
            (code != 0 or summary["isError"]))) and (out / (LOG_FILE + ".stderr")).exists():
        err = (out / (LOG_FILE + ".stderr")).read_text(errors="replace")[-4000:]
        summary["quotaExhausted"] = summary["quotaExhausted"] or bool(QUOTA_PATTERN.search(err))
        summary["stderrTail"] = err[-800:]
    if cancelled:
        status = "CANCELLED"
    elif summary["quotaExhausted"]:
        status = "QUOTA_EXHAUSTED"
    elif code == 0 and summary["sawResult"] and not summary["isError"]:
        status = "COMPLETED"
    else:
        status = "FAILED"
    metadata = {}
    if record["provider"] == "qoder":
        metadata = {key: record.get(key) for key in ("ticket", "provider", "model", "cliVersion",
                    "requestedContextWindow", "requestedEffort", "startedAt", "sessionPersistence")}
        metadata["retryPolicy"] = "return-to-parent"
    write_json(out / RESULT_FILE, {**metadata, "status": status, "exitCode": code,
                                   "finishedAt": now_iso(), **summary})
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    record = json.loads((Path(args.out) / RUN_FILE).read_text())
    done = subprocess.run(["systemctl", "--user", "kill", "--signal=SIGTERM", "--kill-whom=main", record["unit"]],
                          capture_output=True, text=True)
    print(json.dumps({"ok": done.returncode == 0, "unit": record["unit"], "stderr": done.stderr.strip()[:200]}))
    return 0 if done.returncode == 0 else 1


def cmd_phase(args: argparse.Namespace) -> int:
    record = json.loads((Path(args.out) / RUN_FILE).read_text())
    result = token_guard.set_phase(record['guardIdentity'], args.phase, args.context_fraction,
                                   terminal=(Path(args.out) / RESULT_FILE).exists())
    print(json.dumps({'ok': True, 'phase': args.phase, 'identity': record['guardIdentity'], **result}))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="start a headless agent in its own systemd user unit")
    run.add_argument("--provider", choices=PROVIDERS, required=True)
    run.add_argument("--qoder-simple", action="store_true",
                     help="explicit opt-in: bounded T0/T1 executor, no review/advisor/coordination/deployment")
    run.add_argument('--reasonix-pilot', action='store_true', help='explicit parent-authorized Reasonix executor pilot')
    run.add_argument('--reasonix-home', help='explicit Reasonix config/state directory; uses REASONIX_HOME, never HOME')
    run.add_argument('--reasonix-env-file', help='local systemd EnvironmentFile path only; never put a key on argv')
    run.add_argument('--reasonix-recovery-probe', action='store_true',
                     help='parent-authorized fresh minimal probe after native secret reload; does not replay business work')
    run.add_argument('--resume-from', help='previous terminal Reasonix evidence directory; fresh --out required')
    run.add_argument("--role", choices=["executor", "critical", "reviewer", "advisor", "coordinator"], default="executor")
    run.add_argument("--review-token")
    run.add_argument("--review-axis",choices=["spec","standards"],help="review axis (default spec); passing it marks the dispatch as a review")
    run.add_argument("--review-base", help="reviewer: base commit of the reviewed diff; required checks follow the guard file patterns it touches (unknown base = all guards)")
    run.add_argument("--context-window", type=qoder.positive_integer,
                     help="requested context: Qoder default 400000; Reasonix records only, has no context CLI flag")
    run.add_argument("--effort", help="requested effort: default medium; not effective readback")
    run.add_argument("--ticket", required=True)
    run.add_argument("--cwd", required=True)
    run.add_argument("--model", required=True, help="cursor: 'claude-opus-5-5[context=1m,effort=medium,fast=false]' (a bare id uses the context saved in ~/.cursor/cli-config.json); claude: opus")
    run.add_argument("--prompt-file", required=True)
    run.add_argument("--out", required=True, help="evidence directory for cli-run/cli-result/cli.ndjson")
    run.add_argument("--replace", action="store_true", help="reuse --out for a new round")
    run.add_argument("--claude-transport", choices=["tmux", "print"],
                     default=os.environ.get("HOST_DAG_CLAUDE_TRANSPORT", "tmux"),
                     help="claude only: tmux = real interactive TUI in a private tmux server (user 11-11); print = claude -p")
    run.add_argument("--run", help="run directory for registration (default $HOST_DAG_RUN, then the watcher's run)")
    run.add_argument("--batch", help="batch of --ticket (default: the batch holding its open copy)")
    token_guard.add_arguments(run)
    run.add_argument("--force-register", action="store_true",
                     help="register even when a main line still runs on the ticket (this run replaces it)")
    run.add_argument("--no-register", dest="register", action="store_false",
                     help="side run (e.g. offline prep beside a live soak): do not make it the ticket's executor")
    run.set_defaults(func=cmd_run)
    sup = sub.add_parser("supervise", help=argparse.SUPPRESS)
    sup.add_argument("--out", required=True)
    sup.set_defaults(func=cmd_supervise)
    cancel = sub.add_parser("cancel", help="SIGTERM the supervisor; it interrupts the agent and records CANCELLED")
    cancel.add_argument("--out", required=True)
    cancel.set_defaults(func=cmd_cancel)
    phase = sub.add_parser('phase', help='declare an executor stage boundary to the supervisor')
    phase.add_argument('--out', required=True)
    phase.add_argument('--phase', choices=['implement', 'deploy', 'acceptance'], required=True)
    phase.add_argument('--context-fraction', type=float, required=True)
    phase.set_defaults(func=cmd_phase)
    args = p.parse_args(argv)
    try:return args.func(args)
    except review_budget.DispatchError as exc:
        print(json.dumps({'ok':False,'error':{'code':exc.code,'message':exc.message}}));return 1


if __name__ == "__main__":
    sys.exit(main())

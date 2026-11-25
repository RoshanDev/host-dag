#!/usr/bin/env python3
"""Codex App Server adapter for the existing Host DAG watcher contract.

The native daemon owns execution. This client owns no queue or DAG state and
never starts a daemon, retries a mutation, or answers an approval request.
"""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path

from websockets.exceptions import WebSocketException
from websockets.sync.client import unix_connect

from hostdag_dispatch_guard import DispatchError, check as check_dispatch, read_object
import hostdag_token_guard as token_guard
import hostdag_dispatch_guard as review_budget
import hostdag_reply as reply_message
import hostdag_review_required_checks as review_checks


class HostError(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


# Summary item views drop steered user messages, so receipts page full turns.
TURN_PAGE_LIMIT = 5
READ_ATTEMPTS = 3


class NativeHost:
    def __init__(self, socket_path, timeout):
        self.timeout = timeout
        self.socket_path = socket_path
        self.sequence = 0
        self.ws = unix_connect(socket_path, uri="ws://localhost", compression=None,
                               open_timeout=timeout, close_timeout=1,
                               # thread/resume returns full history; a long coordinator passed 20 MiB on 10-03.
                               max_size=256 * 1024 * 1024)
        try:
            self.rpc("initialize", {"clientInfo": {"name": "hostdag_host_dag", "version": "1.0.0"},
                                    "capabilities": {"experimentalApi": True}})
            self.ws.send(json.dumps({"method": "initialized"}))
        except BaseException:
            self.close()
            raise

    def close(self):
        self.ws.close()

    def rpc(self, method, params):
        self.sequence += 1
        request_id = self.sequence
        self.ws.send(json.dumps({"id": request_id, "method": method, "params": params}))
        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(method)
            message = json.loads(self.ws.recv(timeout=remaining))
            if "method" in message:
                if "id" in message:
                    raise HostError("ATTENTION_REQUIRED", "Native host requested client interaction: " + message["method"])
                continue
            if message.get("id") != request_id:
                continue
            if "error" in message:
                error = message["error"]
                # Fixed native protocol's NotSubmittedReason refusals. Only
                # these complete invalid-request responses prove steer did not
                # land; do not infer retryability from general RPC failures.
                if (method == 'turn/steer' and isinstance(error, dict)
                        and error.get('code') == -32600
                        and (error.get('message') == 'no active turn to steer'
                             or re.fullmatch(r'expected active turn id `[^`\r\n]+` but found `[^`\r\n]+`',
                                             str(error.get('message', ''))))):
                    raise HostError('TURN_CHANGED', json.dumps(error, ensure_ascii=False))
                raise HostError("NATIVE_RPC_ERROR", json.dumps(error, ensure_ascii=False))
            result = message.get("result")
            if not isinstance(result, dict):
                raise HostError("PROTOCOL_ERROR", "Native response has no object result")
            return result

    def read(self, thread_id):
        # Legacy history is explicit at creation. Never repair or migrate another
        # client's thread in a read path; unsupported history stays an error.
        # Full-history hydration outgrows the frame limit on long parent threads,
        # and only the latest turn decides status, so hydrate just that turn.
        for _ in range(READ_ATTEMPTS):
            turns = self.turn_page(thread_id, 1)["data"]
            thread = self.rpc("thread/read", {"threadId": thread_id, "includeTurns": False})["thread"]
            thread["turns"] = turns
            # Fence the status read with the latest turn on both sides. A's late
            # page must not borrow B's active/terminal status (or drive a retry).
            after = self.turn_page(thread_id, 1)["data"]
            before_ids = [(t.get("id"), t.get("status")) for t in turns]
            after_ids = [(t.get("id"), t.get("status")) for t in after]
            if before_ids != after_ids:
                continue
            live = thread.get("status", {}).get("type")
            progressing = bool(turns) and turns[0].get("status") == "inProgress"
            if (live == "active" and not progressing) or (live in {"idle", "notLoaded"} and progressing):
                continue
            return thread
        if thread.get("status", {}).get("type") == "active":
            raise HostError("THREAD_BUSY", "Thread is active; latest turn has not stabilized")
        raise HostError("TURN_CHANGED", "Latest turn changed during status read; no consistent snapshot")

    def turn_page(self, thread_id, limit, cursor=None):
        params = {"threadId": thread_id, "limit": limit, "itemsView": "full", "sortDirection": "desc"}
        if cursor:
            params["cursor"] = cursor
        page = self.rpc("thread/turns/list", params)
        if not isinstance(page.get("data"), list):
            raise HostError("PROTOCOL_ERROR", "Native turn page has no data list")
        return page

    def turns_newest_first(self, thread_id):
        cursor = None
        while True:
            page = self.turn_page(thread_id, TURN_PAGE_LIMIT, cursor)
            yield from page["data"]
            cursor = page.get("nextCursor")
            if not cursor:
                return

    @contextlib.contextmanager
    def writer(self, thread_id):
        # One adapter writer per thread, across client processes. This doesn't
        # fence another TUI/client: the DAG still requires a single owner.
        root = Path(os.environ.get("HOST_DAG_CODEX_LOCK_DIR", Path.home() / ".local/state/host-dag-codex-host"))
        root.mkdir(parents=True, exist_ok=True)
        key = hashlib.sha256((self.socket_path + "\0" + thread_id).encode()).hexdigest()
        with (root / (key + ".lock")).open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise HostError("THREAD_BUSY", "Another adapter writer owns this thread") from exc
            yield

    def send(self, thread_id, message, effort=None, sandbox_policy=None, dispatch_context=None, service_tier=None, review_before_submit=None, steer_if_busy=False):
        with self.writer(thread_id):
            if dispatch_context is not None:
                check_dispatch(dispatch_context, thread_id)
            initial = self.read(thread_id)
            def require_or_reply(thread):
                try:
                    require_sendable(thread)
                except HostError as exc:
                    # Only this predicate's explicit active-turn refusal may
                    # steer. A read failure or a competing writer never does.
                    if exc.code != 'THREAD_BUSY' or not steer_if_busy:
                        raise
                    result = steer_current(self, thread_id, normalize(thread)['result']['turnId'],
                                           message, before_submit=review_before_submit, writer_held=True,
                                           dispatch_context=dispatch_context)
                    if result is None:
                        raise HostError('TURN_CHANGED', 'Observed turn ended; reread before another send')
                    turn_id = result.get('turnId')
                    return {'threadId': thread_id, **result,
                            'turn': {'id': turn_id, 'status': 'inProgress'},
                            'delivery': reply_message.accepted('steer', turn_id)}
            steered = require_or_reply(initial)
            if steered:
                return steered
            message = token_guard.before_send(thread_id, initial.get('path'), 'codex',
                                               normalize(initial)['status'], message)
            # Rejoin the SAME stored thread with no cwd/model/permission overrides.
            self.rpc("thread/resume", {"threadId": thread_id})
            # Resume can race another client; recheck before submitting.
            current = self.read(thread_id)
            steered = require_or_reply(current)
            if steered:
                return steered
            if dispatch_context is not None:
                expected_turn = dispatch_context.get("turn_id")
                if expected_turn and normalize(current)["result"]["turnId"] != expected_turn:
                    raise HostError("TURN_CHANGED", "Queued dispatch belongs to an earlier turn")
                check_dispatch(dispatch_context, thread_id)
            params = {"threadId": thread_id, "input": [{"type": "text", "text": message}]}
            if effort:
                # Native turn/start keeps this effort for the thread's later turns too.
                params["effort"] = effort
            if sandbox_policy:
                # Native turn/start keeps this policy for the thread's later turns too.
                params["sandboxPolicy"] = sandbox_policy
                if sandbox_policy["type"] == "dangerFullAccess":
                    params["approvalPolicy"] = "never"
            if service_tier is not None:
                params["serviceTier"] = service_tier
            if review_before_submit:
                review_before_submit()
            result = self.rpc("turn/start", params)
            if steer_if_busy:
                result['delivery'] = reply_message.accepted('start', result.get('turn', {}).get('id'))
            return {"threadId": thread_id, **result, **tier_receipt(service_tier)}

    def goal(self, thread_id):
        return self.rpc("thread/goal/get", {"threadId": thread_id}).get("goal")

    def set_goal_status(self, thread_id, source, target):
        # Only the source -> target edge is changed; complete/blocked/limited goals
        # and a goal the user already moved elsewhere are reported, never rewritten.
        with self.writer(thread_id):
            goal = self.goal(thread_id)
            status = goal.get("status") if isinstance(goal, dict) else None
            if status != source:
                return {"threadId": thread_id, "changed": False, "goal": goal}
            result = self.rpc("thread/goal/set", {"threadId": thread_id, "status": target})
            return {"threadId": thread_id, "changed": True, "goal": result.get("goal")}

    def cancel(self, thread_id, turn_id):
        with self.writer(thread_id):
            current = normalize(self.read(thread_id))
            if current["result"]["turnId"] != turn_id:
                raise HostError("TURN_CHANGED", "Latest turn differs from the requested cancellation")
            if current["status"] in {"completed", "cancelled", "failed"}:
                return current
            self.rpc("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                current = normalize(self.read(thread_id))
                if current["result"]["turnId"] != turn_id:
                    raise HostError("TURN_CHANGED", "Another turn appeared before stop confirmation")
                if current["status"] in {"completed", "cancelled", "failed"}:
                    return current
                time.sleep(0.1)
            raise HostError("STOP_UNCONFIRMED", "Interrupt acknowledged but terminal state not observed")


def normalize(thread):
    turns = thread.get("turns") or []
    last = turns[-1] if turns else {}
    live = thread.get("status", {}).get("type")
    flags = thread.get("status", {}).get("activeFlags") or []
    status = "unknown"
    if live == "active":
        status = "attention" if flags else "running"
    elif live == "systemError":
        status = "error"
    elif live in {"idle", "notLoaded"}:
        status = {"completed": "completed", "failed": "failed", "interrupted": "cancelled",
                  "inProgress": "unknown"}.get(last.get("status"), "idle" if not turns else "unknown")
    final = [item.get("text", "") for item in last.get("items", [])
             if item.get("type") == "agentMessage" and item.get("phase") in {None, "final_answer"}]
    native_error = last.get("error")
    text = "\n".join(final)
    if isinstance(native_error, dict) and native_error.get("message"):
        text = "\n".join(part for part in [text, str(native_error["message"])] if part)
    return {"threadId": thread["id"], "status": status,
            "result": {"turnId": last.get("id"), "text": text,
                       "availability": "available" if last else "unavailable"},
            "nativeStatus": thread.get("status"), "nativeError": native_error,
            "cwd": thread.get("cwd"), "model": thread.get("model"),
            "reasoningEffort": thread.get("reasoningEffort"),
            # The watcher reads the rollout tail to tell a hung model request from a long tool.
            "path": thread.get("path"), "updatedAt": thread.get("updatedAt")}


def require_sendable(thread):
    if thread.get("status", {}).get("type") == "active":
        raise HostError("THREAD_BUSY", "Thread has an active turn; message was not submitted")
    if normalize(thread)["status"] not in {"idle", "completed", "cancelled", "failed"}:
        raise HostError("UNKNOWN_STATUS", "Refusing to submit to an unrecognized thread state")


def messages(thread_id, turns, cursor, limit):
    # Only real user messages are send receipts. Assistant quotations aren't ACKs.
    # `turns` is newest first and lazily paged; stop once this page is known.
    offset = int(cursor or 0)
    if offset < 0 or not 1 <= limit <= 100:
        raise ValueError("Invalid message page")
    end = offset + limit
    rows = []
    for turn in turns:
        for item in reversed(turn.get("items") or []):
            if item.get("type") == "userMessage":
                rows.append({"id": item.get("id"), "role": "user", "text": "\n".join(
                    c.get("text", "") for c in item.get("content", []) if c.get("type") == "text")})
        if len(rows) > end:
            break
    return {"threadId": thread_id, "messages": rows[offset:end],
            "nextCursor": str(end) if end < len(rows) else None}


def message_args(p):
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--message")
    group.add_argument("--message-file", type=Path)


def read_message(args):
    value = args.message_file.read_text(encoding="utf-8") if args.message_file else args.message
    if not value or not value.strip():
        raise ValueError("Message must not be empty")
    return value


def writable_args(cmd):
    # Extra directories (git common dirs, ticket worktrees, evidence roots) the
    # executor may write without asking for an escalation nobody can approve.
    cmd.add_argument("--writable-root", action="append", type=Path, default=[])


def workspace_policy(roots):
    if not roots:
        return None
    return {"type": "workspaceWrite", "writableRoots": [str(r.resolve(strict=True)) for r in roots]}


def steer_current(host, thread_id, expected_turn_id, message, *, before_submit=None,
                  writer_held=False, dispatch_context=None):
    """Fence both guard injections and replies with the same writer/read/RPC path."""
    with contextlib.nullcontext() if writer_held else host.writer(thread_id):
        latest = normalize(host.read(thread_id))
        if latest['result']['turnId'] != expected_turn_id:
            raise HostError('TURN_CHANGED', 'Observation belongs to an earlier turn: ' + json.dumps(
                {'status': latest['status'], 'turnId': latest['result']['turnId']}))
        if latest['status'] != 'running':
            if writer_held:
                raise HostError('TURN_CHANGED', 'Observed turn is no longer running: ' + json.dumps(
                    {'status': latest['status'], 'turnId': latest['result']['turnId']}))
            return None
        if not expected_turn_id:
            raise HostError('UNKNOWN_STATUS', 'Active turn has no id')
        if dispatch_context is not None:
            if dispatch_context.get('turn_id') and dispatch_context['turn_id'] != expected_turn_id:
                raise HostError('TURN_CHANGED', 'Queued dispatch belongs to an earlier turn')
            check_dispatch(dispatch_context, thread_id)
        if before_submit:
            before_submit()
        return host.rpc('turn/steer', {'threadId': thread_id, 'expectedTurnId': expected_turn_id,
                                      'input': [{'type': 'text', 'text': message}]})


def guarded_read(host, args):
    thread = host.read(args.thread_id)
    current = normalize(thread)
    with token_guard.locked(args.thread_id) as guard:
        if guard is None:
            return current
        if current['status'] in {'failed', 'cancelled'}:
            guard.failed()

        def inject(message):
            if steer_current(host, args.thread_id, current['result']['turnId'], message) is None:
                # Do not wake a finished executor merely to write a checkpoint.
                guard.state['pending_instruction'] = message

        def stop():
            if current['status'] in {'running', 'attention'}:
                host.cancel(args.thread_id, current['result']['turnId'])

        sample = guard.sample_log(current.get('path'), 'codex', active=current['status'] == 'running',
                                  phase=getattr(args, 'guard_phase', None))
        sample['waiting'] = token_guard.wait_reason(guard.state, thread) if current['status'] == 'running' else None
        fraction = getattr(args, 'guard_context_fraction', None)
        if fraction is not None:
            sample['context_fraction'] = fraction
        guard.check(inject=inject, stop=stop, **sample)
        # A previous interrupt whose response was lost must be confirmed before
        # permitting a fresh executor; never silently resume the old one.
        if guard.blocked and not guard.state.get('stop_confirmed'):
            stop()
            guard.state['stop_confirmed'] = True
        if guard.blocked:
            current['status'] = 'attention'
            current['result']['text'] = ('TOKEN_GUARD_NEW_SESSION_REQUIRED: ' + guard.instruction()
                                         + '\n' + guard.state.get('handoff_error', ''))
        current['tokenGuard'] = {k: guard.state.get(k) for k in
                                 ('mode', 'tier', 'cap', 'tokens', 'rounds', 'stopped', 'retry_count')}
    return current


def tier_receipt(service_tier):
    # Requested per-dispatch tier, not proof of the backend's billed tier.
    return {"serviceTier": service_tier} if service_tier is not None else {}


def dispatch_args(cmd):
    cmd.add_argument("--service-tier", choices=["priority", "default"],
                     help="per-dispatch service tier; omitted leaves native defaults unchanged")
    cmd.add_argument("--run-file", type=Path,
                     help="also save the dispatch receipt as cli-run.json at this path")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--socket", default=os.environ.get("HOST_DAG_CODEX_SOCKET", str(
        Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "app-server-control/app-server-control.sock")))
    p.add_argument("--timeout", type=float, default=20)
    sub = p.add_subparsers(dest="group", required=True)
    thread = sub.add_parser("thread").add_subparsers(dest="action", required=True)
    read = thread.add_parser("read")
    read.add_argument("thread_id")
    read.add_argument("--view", choices=["result", "messages"], default="result")
    read.add_argument("--limit", type=int, default=25)
    read.add_argument("--cursor")
    read.add_argument('--guard-phase', choices=['implement', 'deploy', 'acceptance'])
    read.add_argument('--guard-context-fraction', type=float)
    send = thread.add_parser("send")
    send.add_argument("thread_id")
    reply_message.add_arguments(send)
    send.add_argument("--effort", help="persistent reasoning effort override for this and later turns")
    send.add_argument("--dispatch-context", type=Path,
                      help="saved run/batch/ticket/updated_at/thread context; revalidate before sending")
    send.add_argument('--guard-run',type=Path)
    send.add_argument('--guard-ticket')
    send.add_argument('--review-reservation')
    send.add_argument('--review-token')
    send.add_argument('--review-axis',choices=['spec','standards'],default='spec')
    writable_args(send)
    dispatch_args(send)
    send.add_argument("--sandbox", choices=["danger-full-access"],
                      help="lift this and later turns to full access with approval policy never")
    message_args(send)
    resume = thread.add_parser("resume")
    resume.add_argument("thread_id")
    cancel = thread.add_parser("cancel")
    cancel.add_argument("thread_id")
    cancel.add_argument("--turn-id", required=True)
    start = thread.add_parser("start")
    start.add_argument("--cwd", type=Path, required=True)
    start.add_argument("--model", required=True)
    start.add_argument("--effort", required=True)
    start.add_argument("--sandbox", choices=["read-only", "workspace-write", "danger-full-access"], required=True)
    start.add_argument("--approval-policy", choices=["never", "on-request", "untrusted"], required=True)
    start.add_argument("--approvals-reviewer", choices=["user", "auto_review"], default="user")
    writable_args(start)
    dispatch_args(start)
    start.add_argument('--dispatch-role',choices=['executor','reviewer'])
    start.add_argument('--review-token')
    start.add_argument('--review-axis',choices=['spec','standards'],help='review axis (default spec); passing it marks the start as a review')
    start.add_argument('--review-batch')
    start.add_argument('--review-base',help='reviewer: base commit of the reviewed diff; unknown base = all guards')
    token_guard.add_arguments(start)
    message_args(start)
    goal = sub.add_parser("goal").add_subparsers(dest="action", required=True)
    for name in ("read", "pause", "resume"):
        goal.add_parser(name).add_argument("thread_id")
    skills = sub.add_parser("skills")
    skills.add_argument("action", choices=["list"])
    skills.add_argument("--cwd", type=Path, required=True)
    model = sub.add_parser("model")
    model.add_argument("action", choices=["list"])
    account = sub.add_parser("account")
    account.add_argument("action", choices=["rate-limits"])
    return p


def execute(host, args):
    if args.group == "skills":
        return host.rpc("skills/list", {"cwds": [str(args.cwd.resolve(strict=True))], "forceReload": True})
    if args.group == "model":
        return host.rpc("model/list", {})
    if args.group == "account":
        # Read-only weekly/primary window usage; the executor switch reads it before dispatch.
        return host.rpc("account/rateLimits/read", {})
    if args.group == "goal":
        # A paused goal makes Codex skip idle auto-continuation; the watcher's
        # turn/start stays the only wake path while the DAG waits on events.
        if args.action == "read":
            return {"threadId": args.thread_id, "goal": host.goal(args.thread_id)}
        if args.action == "pause":
            return host.set_goal_status(args.thread_id, "active", "paused")
        return host.set_goal_status(args.thread_id, "paused", "active")
    if args.action == "read":
        if args.view == "messages":
            return messages(args.thread_id, host.turns_newest_first(args.thread_id), args.cursor, args.limit)
        return guarded_read(host, args)
    if args.action == "resume":
        token_guard.refuse_resume(args.thread_id, retry=normalize(host.read(args.thread_id))['status'] in {'failed', 'cancelled'})
        with host.writer(args.thread_id):
            return host.rpc("thread/resume", {"threadId": args.thread_id})
    if args.action == "send":
        if args.sandbox and args.writable_root:
            raise ValueError("--sandbox danger-full-access and --writable-root are exclusive")
        policy = {"type": "dangerFullAccess"} if args.sandbox else workspace_policy(args.writable_root)
        context = read_object(args.dispatch_context) if args.dispatch_context else None
        reply = reply_message.identity(args)
        message=reply_message.mark(read_message(args), reply)
        root=args.guard_run or os.environ.get('HOST_DAG_RUN')
        grant=None
        fingerprint=review_budget.review_fingerprint(args.thread_id,message,{'effort':args.effort,'policy':policy,'tier':args.service_tier} if args.effort or policy or args.service_tier else None)
        if root:
            if args.review_reservation:
                grant={'token':args.review_reservation}
            else:
                token=args.review_token or hashlib.sha256((fingerprint+str(args.message_file)).encode()).hexdigest()
                grant=review_budget.reserve_review(root,ticket=args.guard_ticket,thread=args.thread_id,
                    axis=args.review_axis,token=token,fingerprint=fingerprint,required=False)
            if grant and grant.get('replay'):return grant['receipt']
        def submit():
            if grant:
                claim=review_budget.claim_review(root,grant['token'],fingerprint)
                if claim.get('replay'):raise review_budget.ReviewReplay(claim['receipt'])
                grant.update(claim)
        try:
            result=host.send(args.thread_id,message,args.effort,policy,context,args.service_tier,
                             **({'review_before_submit':submit} if grant else {}),
                             **({'steer_if_busy':True} if reply else {}))
        except review_budget.ReviewReplay as replay:
            return replay.receipt
        except HostError as exc:
            review_budget.settle_review(root,grant,'retryable' if exc.code in ('THREAD_BUSY','TURN_CHANGED') else 'uncertain')
            raise
        except BaseException:
            review_budget.settle_review(root,grant,'uncertain');raise
        if grant:result['reviewBudget']={k:grant[k] for k in ('token','round','axis','group')}
        review_budget.settle_review(root,grant,'delivered',result)
        return result
    if args.action == "cancel":
        return host.cancel(args.thread_id, args.turn_id)
    message = read_message(args)
    review_selection=None
    # Hard hook (TOOL-KK-PY37-GUARD-HOOK): any review start (role, guard ticket, explicit axis or a
    # review-* message file such as parent-review-start.sh's <evidence>/review-*/contract.md).
    review_reasons=review_checks.review_reasons(args.dispatch_role or ('reviewer' if args.guard_ticket else None),
                                                args.review_axis, (getattr(args,'message_file',None),))
    args.review_axis=args.review_axis or 'spec'
    if review_reasons:
        # Fail closed (TOOL-GUARD-FAIL-CLOSED): killed/timed-out/garbage guard = INFRA_ERROR, no budget reserved.
        message, review_selection = review_budget.review_required_checks(message, args.cwd, args.review_base, args.guard_ticket, review_reasons)
    requested_message=message
    guard_config, message = token_guard.prepare(args, message)
    if args.writable_root and args.sandbox != "workspace-write":
        raise ValueError("--writable-root needs --sandbox workspace-write")
    policy = workspace_policy(args.writable_root)
    cwd = str(args.cwd.resolve(strict=True))
    start_params = {
        "cwd": cwd, "model": args.model, "config": {"model_reasoning_effort": args.effort, **EXECUTOR_CONFIG},
        "sandbox": args.sandbox, "approvalPolicy": args.approval_policy,
        "approvalsReviewer": args.approvals_reviewer, "allowProviderModelFallback": False,
        "historyMode": "legacy",
    }
    start_params.update(tier_receipt(args.service_tier))
    root=args.guard_run or os.environ.get('HOST_DAG_RUN')
    role=args.dispatch_role or ('reviewer' if args.guard_ticket else None)
    grant=None
    if role=='reviewer':
        if not root or not args.guard_ticket:
            raise DispatchError('REVIEW_IDENTITY_REQUIRED','reviewer start needs guard-run/ticket')
        fingerprint=hashlib.sha256(json.dumps([requested_message,start_params,policy,args.review_axis],sort_keys=True).encode()).hexdigest()
        token=args.review_token or hashlib.sha256((str(args.message_file)+fingerprint).encode()).hexdigest()
        grant=review_budget.reserve_review(root,ticket=args.guard_ticket,batch=args.review_batch,axis=args.review_axis,token=token,fingerprint=fingerprint)
        if grant and grant.get('replay'):return grant['receipt']
    try:
        result = host.rpc("thread/start", start_params)
        args.thread_id = result["thread"]["id"]
        actual = (result.get("cwd"), result.get("model"), result.get("reasoningEffort"))
        if actual != (cwd, args.model, args.effort) or result["thread"].get("historyMode") != "legacy":
            raise HostError("BINDING_MISMATCH", "Native cwd/model/effort/history differs; no turn submitted")
        expected_sandbox = {"read-only": "readOnly", "workspace-write": "workspaceWrite", "danger-full-access": "dangerFullAccess"}[args.sandbox]
        if (result.get("sandbox", {}).get("type") != expected_sandbox
                or result.get("approvalPolicy") != args.approval_policy
                or result.get("approvalsReviewer") != args.approvals_reviewer):
            raise HostError("BINDING_MISMATCH", "Native permissions differ; no turn submitted")
        if guard_config:
            token_guard.Guard.create(identity=args.thread_id, cwd=cwd, contract=message,
                                     publish=True, **guard_config)
        params = {"threadId": args.thread_id, "input": [{"type": "text", "text": message}]}
        if policy:
            params["sandboxPolicy"] = policy
        params.update(tier_receipt(args.service_tier))
        if grant:
            claim=review_budget.claim_review(root,grant["token"],fingerprint)
            if claim.get("replay"):raise review_budget.ReviewReplay(claim["receipt"])
        turn = host.rpc("turn/start", params)
        answer={**result, **turn, "threadId": args.thread_id, **tier_receipt(args.service_tier)}
        if review_selection is not None:
            answer["reviewRequiredChecks"]=review_selection
        if grant:
            answer["reviewBudget"]={k:grant[k] for k in ("token","round","axis","group")}
            review_budget.settle_review(root,grant,"delivered",answer)
        return answer
    except review_budget.ReviewReplay as replay:
        return replay.receipt
    except DispatchError:
        review_budget.settle_review(root,grant,'retryable');raise
    except BaseException:
        review_budget.settle_review(root,grant,'uncertain')
        raise



# Executors must not reach the user's own Chrome: its 1Password menu blocks automation and
# stalled acceptance three times (2026-09-29, 10-01, 10-09) although AGENTS.md forbade it.
# Browser acceptance goes through scripts/hostdag_clean_browser.py instead.
# Executors may drive the user's own Chrome (user decision 2026-10-28: shared
# sign-ins are wanted); acceptance evidence still uses the real login form.
EXECUTOR_CONFIG = {}


def main():
    args = parser().parse_args()
    host = None
    try:
        if args.timeout <= 0:
            raise ValueError("timeout must be positive")
        if args.group == 'thread' and args.action == 'send':
            reply_message.identity(args)
        host = NativeHost(args.socket, args.timeout)
        result = execute(host, args)
        receipt = json.dumps(result, ensure_ascii=False)
        # Dispatch already succeeded. Publish its identity before attempting the
        # optional copy so callers never retry a successful turn on file errors.
        print(receipt, flush=True)
        if getattr(args, "run_file", None):
            try:
                args.run_file.write_text(receipt + "\n", encoding="utf-8")
            except OSError as exc:
                print(json.dumps({"warning": {"code": "RUN_FILE_WRITE_FAILED",
                                             "message": str(exc), "path": str(args.run_file)},
                                  "threadId": result["threadId"],
                                  "turnId": result["turn"]["id"],
                                  "dispatchSucceeded": True, "retryDispatch": False,
                                  **tier_receipt(args.service_tier)}, ensure_ascii=False), file=sys.stderr)
        return 0
    except (HostError, DispatchError) as exc:
        error = {"code": exc.code, "message": exc.message}
    except TimeoutError:
        error = {"code": "TIMEOUT", "message": "Native request timed out; mutation outcome may be unknown"}
    except (OSError, WebSocketException) as exc:
        error = {"code": "RUNTIME_UNREACHABLE", "message": str(exc)}
    except (ValueError, KeyError, TypeError) as exc:
        error = {"code": "PROTOCOL_ERROR", "message": str(exc)}
    finally:
        if host:
            host.close()
    print(json.dumps({"error": error, "threadId": getattr(args, "thread_id", None)}, ensure_ascii=False))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

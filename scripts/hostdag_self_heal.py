"""Stall detection, transient-failure retry and human alerts for host-dag-host-watch.

User decision 2026-09-30: after a coordinator turn hung on one model request
for three hours while the watcher held a worker failure behind "parent busy",
the watcher may interrupt a turn that is waiting on the MODEL with no rollout
activity, retry a worker whose failure is an infrastructure blip, and alert the
user on the desktop when it cannot heal. A turn waiting on a TOOL (a long build)
is never interrupted; it only raises an alert after a long silence.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import fcntl
from contextlib import contextmanager
from pathlib import Path
from hostdag_json import atomic_json, atomic_text, read_json
from typing import Any

try:
    import hostdag_ticket_state as ts
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import hostdag_ticket_state as ts

from hostdag_display import memory_text

MODEL_STALL_SECONDS = 20 * 60
TOOL_STALL_ALERT_SECONDS = 3 * 60 * 60
MAX_AUTO_RETRIES = 2
RETRY_BACKOFF_SECONDS = (120, 600)
ALERT_REPEAT_SECONDS = 60 * 60
TAIL_BYTES = 512 * 1024

TOOL_CALL_TYPES = {"function_call", "custom_tool_call", "local_shell_call", "web_search_call"}
TOOL_OUTPUT_TYPES = {"function_call_output", "custom_tool_call_output", "local_shell_call_output"}
TRANSIENT = re.compile(
    r"connection (?:failed|reset|refused|closed)|error sending request|stream disconnected|"
    r"remote compact task|timed? ?out|\b50[234]\b|overloaded|internal server error|broken pipe|"
    r"unexpected end of (?:stream|file)|temporarily unavailable",
    re.IGNORECASE,
)

RETRY_MESSAGE = (
    "[host-dag-host-watch 自动续跑] 上一轮因基础设施问题中断（{reason}），不是产品或审查结论。"
    "请沿原票合同从中断点继续：先读回工作树 HEAD/clean、证据目录与上一轮已完成的步骤，"
    "不要重复已完成的破坏性操作，不要新建会话或队列。这是第 {n} 次自动续跑，最多 {max} 次。"
)


def turn_activity(path: str | None) -> dict[str, Any] | None:
    """Last rollout write time and whether the open turn waits on the model or on a tool."""
    if not path:
        return None
    rollout = Path(path)
    try:
        size = rollout.stat().st_size
        mtime = rollout.stat().st_mtime
        with rollout.open("rb") as handle:
            handle.seek(max(0, size - TAIL_BYTES))
            tail = handle.read().decode("utf-8", errors="replace").splitlines()
    except OSError:
        return None
    waiting = "unknown"
    open_calls: set[str] = set()
    for line in tail:
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        payload = record.get("payload") if isinstance(record, dict) else None
        if not isinstance(payload, dict):
            continue
        kind = payload.get("type")
        if record.get("type") == "event_msg" and kind in {"task_complete", "turn_aborted"}:
            waiting, open_calls = "idle", set()
        elif record.get("type") == "event_msg" and kind in {"task_started", "user_message"}:
            waiting = "model"
        elif record.get("type") != "response_item":
            continue
        elif kind in TOOL_CALL_TYPES:
            open_calls.add(str(payload.get("call_id") or payload.get("id")))
            waiting = "tool"
        elif kind in TOOL_OUTPUT_TYPES:
            open_calls.discard(str(payload.get("call_id")))
            waiting = "tool" if open_calls else "model"
        elif kind == "message" and payload.get("role") == "user":
            waiting = "model"
    return {"mtime": mtime, "waiting": waiting}


def stall(activity: dict[str, Any] | None, now: float) -> str | None:
    """'model' when a model request is overdue (safe to interrupt), 'tool' for a long silent tool."""
    if not activity:
        return None
    idle = now - float(activity["mtime"])
    if activity["waiting"] == "model" and idle >= MODEL_STALL_SECONDS:
        return "model"
    if activity["waiting"] == "tool" and idle >= TOOL_STALL_ALERT_SECONDS:
        return "tool"
    return None


def server_identity() -> str | None:
    """Identity of the running Codex app-server, from the daemon's pid file; changes on every restart.

    2026-10-08: systemd-oomd killed the cgroup holding the app-server, and the turns it was
    running came back "cancelled", indistinguishable from a user cancel without this.
    """
    default = Path(os.environ.get("CODEX_HOME") or Path.home() / ".codex") / "app-server-daemon/app-server.pid"
    path = Path(os.environ.get("HOST_DAG_CODEX_SERVER_PID_FILE") or default)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not data.get("pid"):
        return None
    ident = data.get("processIdentity") if isinstance(data.get("processIdentity"), dict) else {}
    return f"{data['pid']}:{ident.get('bootId', '')}:{ident.get('startTicks', '')}"


def is_transient(text: str | None) -> bool:
    return bool(text) and bool(TRANSIENT.search(text or ""))


def retry_due(rec: dict[str, Any], now: float) -> bool | None:
    """None when retries are used up; False while backing off; True when a retry may go now."""
    count = int(rec.get("auto_retries") or 0)
    if count >= MAX_AUTO_RETRIES:
        return None
    last = rec.get("last_failure_at")
    wait = RETRY_BACKOFF_SECONDS[min(count, len(RETRY_BACKOFF_SECONDS) - 1)]
    return last is None or now - float(last) >= wait


def retry_message_file(state: dict[str, Any], ticket: str, reason: str, n: int) -> str:
    folder = Path(state["state_path"]).parent / "auto-retry"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{re.sub(r'[^A-Za-z0-9._-]+', '-', ticket)}-{n}.md"
    target.write_text(RETRY_MESSAGE.format(reason=reason[:200], n=n, max=MAX_AUTO_RETRIES), encoding="utf-8")
    return str(target)


def alert(state: dict[str, Any], key: str, title: str, body: str, now: float, desktop: bool = True,
          *, desktop_body_limit: int = 500, urgency: str = "critical") -> bool:
    """Log every alert; show it on the desktop at most once an hour per key."""
    sent = state.setdefault("alerts_sent", {})
    record = {"at": now, "key": key, "title": title, "body": body, "desktop": desktop}
    log = Path(state["state_path"]).parent / "alerts.jsonl"
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    reasonix_fault = telegram_key(key).startswith('needs_user:reasonix:')
    if reasonix_fault:
        # Durable admission, NOT a sent receipt. The existing bridge flush owns
        # transport/retry; desktop status cannot imply Telegram delivery.
        defer_telegram(log.parent, title, body, now, key=key, credential_fault=state.get('reasonix_fault'))
    last = sent.get(key)
    if last is not None and now - float(last) < ALERT_REPEAT_SECONDS:
        return False
    sent[key] = now
    if desktop and os.environ.get("HOST_DAG_WATCH_NOTIFY", "1") != "0":
        title = memory_text(title)
        body = for_people(body)  # alerts.jsonl above keeps the paths; people never read them (2026-10-15)
        try:
            subprocess.run(["notify-send", "-a", "Host DAG", "-u", urgency, title, body[:desktop_body_limit]],
                           capture_output=True, timeout=10, check=False)
        except (OSError, subprocess.TimeoutExpired):
            pass
        run_dir = Path(state["state_path"]).parent
        if not reasonix_fault and telegram_worthy(key, title, stated_tickets(state), states=ts.snapshot(load_run(state))) and first_telegram(run_dir, key, title):
            if is_quiet_hours(now) and not telegram_key(key).startswith("urgent:"):
                defer_telegram(run_dir, title, body, now, key=key)
            elif not send_telegram(title, body):
                defer_telegram(run_dir, title, body, now, key=key)  # retried by the bridge's flush, never dropped
    return True



PATH_RE = re.compile(r"(?:\$RUN|/run/media|/home|/tmp|/var|~|\.{0,2}/?evidence)/[^\s，。；、）)\]]*")
PATH_LEAD_RE = re.compile(r"[（(]?\s*(?:详情|详见|报告|证据|日志|路径|请求|见|位于|在)\s*[:：]?\s*$")


def for_people(text: str) -> str:
    """Drop file paths (and the "详情/报告/证据 …" lead-ins pointing at them) from a desktop/Telegram body.

    User, 2026-10-15: "tg 推送和桌面通知不需要证据或者报告位置，我又不看".
    """
    text = memory_text(text)
    out, pos = [], 0
    for m in PATH_RE.finditer(text):
        head = PATH_LEAD_RE.sub("", text[pos:m.start()])
        out.append(head)
        pos = m.end()
    out.append(text[pos:])
    cleaned = "".join(out)
    cleaned = re.sub(r"[（(]\s*[）)]", "", cleaned)
    cleaned = re.sub(r"[ \t]+([，。；、])", r"\1", cleaned)
    cleaned = re.sub(r"([，；、])\s*([。；])", r"\2", cleaned)
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    return "\n".join(line.rstrip(" ，；、:：") for line in cleaned.splitlines()).strip()


def telegram_key(key: str) -> str:
    """host-dag-alert.py prefixes coordinator keys; classify on what the coordinator wrote."""
    return key.removeprefix("coordinator:")


TICKET = re.compile(r"[A-Z][A-Z0-9]*-\d+")


def telegram_worthy(key: str, title: str, stated: set[str] | frozenset[str] = frozenset(),
                    *, states: dict | None = None) -> bool:
    """Only actual ticket acceptance and requests for human action reach Telegram.

    A result/round/batch report is never a Telegram event. Acceptance wording or
    an accepted: prefix alone cannot substitute for the run receipt's state.
    """
    plain = telegram_key(key)
    if plain.startswith(("result:", "stage:", "advisor-fyi:")):
        return False
    if plain.startswith("urgent:") and key.startswith("coordinator:"):
        # The coordinator tagged harness recoveries "urgent" (10-18 20:26/21:15 "S50 recovery required");
        # those are its own or the advisor's to handle. Only one that asks the user for action is pushed.
        return bool(ASKS_USER.search(title))
    if plain.startswith(("needs_user:", "advisor:", "urgent:")):
        return True  # explicit human-decision/action routes; urgent: here is the host guard's
    ticket = TICKET.search(title)
    if "阶段" not in title and ticket and (
            plain.startswith("accepted:") or
            (key.startswith("coordinator:") and (ACCEPTED_TITLE.search(title) or "-accepted" in key.lower()))):
        matches = [r for k, r in (states or {}).items() if k.partition("/")[2] == ticket.group(0)]
        return bool(matches) and all(r.get("state") in ts.DONE for r in matches)
    if not key.startswith("coordinator:") or any(name in title for name in stated):
        return False
    return bool(ASKS_USER.search(title))


def run_receipt(state: dict[str, Any]) -> Path:
    return Path(state.get("run_receipt") or Path(state["state_path"]).parent / "run.json")


def load_run(state: dict[str, Any]) -> dict[str, Any]:
    try:
        data = read_json(run_receipt(state))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def stated_tickets(state: dict[str, Any]) -> frozenset[str]:
    """Tickets the state machine already reports: an explicit `state`, or a derived NEEDS_USER/accepted."""
    snap = state.get("ticket_states")
    if not isinstance(snap, dict):
        snap = ts.snapshot(load_run(state))
    covered = ts.DONE | {ts.NEEDS_USER}
    return frozenset(k.partition("/")[2] for k, rec in snap.items()
                     if (rec or {}).get("explicit") or (rec or {}).get("state") in covered)


ACCEPTED_TITLE = re.compile(r"已(?:限定|缩减|部分)?验收|验收(?:完成|通过)")
ASKS_USER = re.compile(r"需你|待你|需要你")


def first_telegram(run_dir: Path, key: str, title: str) -> bool:
    """One push per accepted ticket even when the watcher and the coordinator both report it."""
    ticket = TICKET.search(title)
    plain = telegram_key(key)
    if plain.startswith("urgent:"):
        return True  # emergencies repeat; alert() already limits each key to once an hour
    asks = plain.startswith(("advisor:", "result:", "needs_user:")) or bool(ASKS_USER.search(title))
    ident = f"accepted:{ticket.group(0)}" if ticket and not asks else plain
    return remember_telegram(run_dir, ident, aliases=(f"coordinator:{ident}",))


def remember_telegram(run_dir: Path, ident: str, *, aliases=()) -> bool:
    """Serialize every sent-file read/modify/write operation.

    first_telegram retains its existing admission semantics. The bridge calls
    this only after an actual ack. Never hold this lock during transport or
    while acquiring deferred.lock (the bridge already owns deferred.lock).
    """
    path = run_dir / "telegram-sent.json"
    with (run_dir / 'telegram-sent.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            sent = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            sent = []
        if ident in sent or any(alias in sent for alias in aliases):
            return False
        atomic_json(path, sent + [ident])
        return True


def state_alerts(state: dict[str, Any], run: dict[str, Any], now: float,
                 notify=None) -> list[dict[str, Any]]:
    """Alert on ticket state transitions since the last poll; returns what was raised.

    Pushed (via alert -> Telegram): a ticket entering ACCEPTED/INTEGRATED (once per ticket),
    entering NEEDS_USER (once per entry). Batch completion is desktop/log only.
    The first call only records the snapshot, so a watcher restart never replays history.
    """
    notify = notify or alert
    after = ts.snapshot(run)
    before = state.get("ticket_states")
    state["ticket_states"] = after
    if not isinstance(before, dict):
        return []
    raised = []
    entries = state.setdefault("needs_user_entries", {})
    for ch in ts.transitions(before, after):
        name, where = ch["ticket"], f"{ch['batch']}/{ch['ticket']}"
        head = f"{ch['batch']} · {ch['title']}" if ch["title"] else ch["batch"]
        if ch["to"] in ts.DONE and ch["from"] not in ts.DONE and ch["from"] is not None:
            label = ts.SCOPE_LABEL.get(ch["scope"] or "")
            title = f"{name} 已验收" + (f"（{label}）" if label and ch["scope"] != "FULL" else "")
            body = f"{head}\n第 {ch['attempt']} 次执行，状态 {ch['from']} → {ch['to']}" + (
                f"\n证据 {ch['evidence']}" if ch["evidence"] else "")
            key = f"accepted:{name}"
        elif ch["to"] == ts.NEEDS_USER:
            entries[where] = int(entries.get(where) or 0) + 1
            title = f"{name} 需要你决定"
            body = f"{head}\n{ch['blocker'] or '协调者未写明问题，请看看板或 alerts.jsonl'}"
            key = f"needs_user:{name}:{entries[where]}"
        else:
            continue
        notify(state, key, title, body, now)
        raised.append({"key": key, "title": title, "body": body})
    titles = {b: (body or {}).get("title") for b, body in (run.get("batches") or {}).items() if isinstance(body, dict)}
    known = {k.partition("/")[0] for k in before}
    for batch in ts.finished_batches(before, after):
        if batch not in known:
            continue  # a batch recorded already finished is a backfill, not news
        names = sorted(k.partition("/")[2] for k, rec in after.items()
                       if k.startswith(batch + "/") and rec["state"] in ts.DONE)
        key, title = f"result:{batch}", f"批次 {batch} 全部验收（{len(names)} 票）"
        body = (titles.get(batch) or "") + "\n" + "、".join(names)
        notify(state, key, title, body.strip(), now)
        raised.append({"key": key, "title": title, "body": body.strip()})
    return raised


QUIET_START_HOUR = 22
QUIET_END_HOUR = 8


def is_quiet_hours(now: float) -> bool:
    """22:00–08:00 local time: Telegram is held for the morning (user decision 2026-10-14)."""
    hour = time.localtime(now).tm_hour
    return hour >= QUIET_START_HOUR or hour < QUIET_END_HOUR


@contextmanager
def deferred_lock(run_dir: Path):
    folder = run_dir / "advisor"
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / 'deferred.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield


def defer_telegram(run_dir: Path, title: str, body: str, now: float, *, key: str = "", credential_fault=None) -> None:
    with deferred_lock(run_dir):
        path = run_dir / 'advisor' / 'deferred.jsonl'
        item = {'at': now, 'title': title, 'body': body, 'key': key}
        if telegram_key(key).startswith('needs_user:reasonix:'):
            key = telegram_key(key)
            sent = read_json(run_dir / 'telegram-sent.json') if (run_dir / 'telegram-sent.json').exists() else []
            if key in sent:
                return
            if path.exists():
                for line in path.read_text().splitlines():
                    try:
                        previous = json.loads(line)
                    except ValueError:
                        continue
                    if telegram_key(previous.get('key', '')) == key:
                        return
            item.update(key=key, status='queued')
            if credential_fault:
                item['credential_fault'] = credential_fault
        with path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(item, ensure_ascii=False) + '\n')


def flush_deferred(run_dir: Path, now: float) -> int:
    with deferred_lock(run_dir):
        return _flush_deferred(run_dir, now)


def _flush_deferred(run_dir: Path, now: float) -> int:
    """Outside quiet hours, send everything held overnight as one digest; returns items sent."""
    path = run_dir / "advisor" / "deferred.jsonl"
    if is_quiet_hours(now) or not path.exists():
        return 0
    items = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            items.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not items:
        path.unlink()
        return 0
    states = ts.snapshot(load_run({"state_path": str(run_dir / "watch.json")}))
    allowed = []
    for item in items:
        if telegram_worthy(item.get("key", ""), item.get("title", ""), states=states):
            allowed.append(item)
        else:
            with (path.parent / "suppressed.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    items = allowed
    atomic_text(path, "".join(json.dumps(i, ensure_ascii=False) + "\n" for i in items))
    if not items:
        path.unlink()
        return 0
    # Reasonix credential incidents stay separate so each exact key has an
    # acknowledged success before it enters telegram-sent. Failed sends remain
    # in this same queue; never claim delivery merely after enqueue/HTTP 200.
    credential_items = [i for i in items if telegram_key(i.get('key', '')).startswith('needs_user:reasonix:')]
    ordinary = [i for i in items if i not in credential_items]
    pending, delivered = [], 0
    sent_path = run_dir / 'telegram-sent.json'
    sent = read_json(sent_path) if sent_path.exists() else []
    for item in credential_items:
        key = telegram_key(item['key'])
        if key in sent:
            continue
        if item.get('credential_fault'):
            from hostdag_reasonix_faults import blocked
            incident = item['credential_fault']
            current = blocked(incident['home'])
            if not current or current.get('id') != incident['id']:
                with (path.parent / 'suppressed.jsonl').open('a') as handle:
                    handle.write(json.dumps({**item, 'reason': 'credential-recovered'}, ensure_ascii=False) + '\n')
                continue
        if send_telegram(item['title'], item['body']):
            remember_telegram(run_dir, key)
            sent.append(key)
            delivered += 1
        else:
            pending.append({**item, 'status': 'undelivered', 'last_attempt': now})
    atomic_text(path, ''.join(json.dumps(i, ensure_ascii=False) + '\n' for i in pending + ordinary))
    if not ordinary:
        if not pending:
            path.unlink()
        return delivered
    items = ordinary
    parts = [f"{time.strftime('%H:%M', time.localtime(i['at']))} {i['title']}\n{i['body'][:600]}" for i in items]
    text = "\n\n".join(parts)
    chunks = [text[i:i + 3500] for i in range(0, len(text), 3500)]
    for n, chunk in enumerate(chunks, 1):
        if not send_telegram(f"补发的通知（{len(items)} 条，{n}/{len(chunks)}）", chunk):
            return delivered  # keep the file; retry on the next poll
    if pending:
        atomic_text(path, ''.join(json.dumps(i, ensure_ascii=False) + '\n' for i in pending))
    else:
        path.unlink()
    return delivered + len(items)


def telegram_config() -> dict[str, str]:
    """Read KEY=VALUE lines from the Telegram config; empty when it is absent.

    Telegram routing is enforced by telegram_worthy; desktop events are broader.
    """
    path = Path(os.environ.get("HOST_DAG_TELEGRAM_CONFIG",
                               Path.home() / ".config" / "host-dag" / "telegram.env"))
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    config = {}
    for line in lines:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip().strip('"').strip("'")
    return config


def send_telegram(title: str, body: str) -> bool:
    """Push one message to the configured chat; never raises."""
    import urllib.request
    title, body = memory_text(title), memory_text(body)
    config = telegram_config()
    token, chat = config.get("TELEGRAM_BOT_TOKEN"), config.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return False
    api = config.get("TELEGRAM_API", "https://api.telegram.org").rstrip("/")
    proxy = config.get("TELEGRAM_PROXY", "")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(
        {"http": proxy, "https": proxy} if proxy else {}))
    payload = json.dumps({"chat_id": chat, "text": f"{title}\n\n{body}"[:4000],
                          "disable_web_page_preview": True}).encode()
    request = urllib.request.Request(f"{api}/bot{token}/sendMessage", data=payload,
                                     headers={"Content-Type": "application/json"})
    for attempt in range(3):  # the proxy path drops TLS now and then (SSL EOF)
        try:
            with opener.open(request, timeout=10) as response:
                if response.status == 200 and json.load(response).get('ok') is True:
                    return True
        except (OSError, ValueError):
            pass
        time.sleep(2 * (attempt + 1))
    return False

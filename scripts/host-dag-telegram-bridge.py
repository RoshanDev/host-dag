#!/usr/bin/env python3
"""Forward the user's Telegram replies to the DAG coordinator and push its answer back.

User decision 2026-10-14: alerts go to Telegram (see hostdag_self_heal.send_telegram);
replies from the user's own chat are delivered to the parent coordinator thread as
the user's words, and the coordinator's final reply for that turn is sent back.
Only TELEGRAM_CHAT_ID is accepted; every other chat is ignored. The bridge never
runs commands itself.

usage: host-dag-telegram-bridge.py --state <run>/watch.json [--once]
Commands in the chat: /status shows the coordinator's latest reply; any other text
is forwarded (reply to an alert so the coordinator knows which one you mean).
"""
from __future__ import annotations

from hostdag_json import atomic_json
import argparse
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hostdag_self_heal import flush_deferred, telegram_config  # noqa: E402

POLL_SECONDS = 50
RETRY_SECONDS = 30
MAX_TEXT = 3800
HEADER = (
    "[Telegram 用户回复] 以下文字来自用户本人的 Telegram（chat {chat}），由 host-dag-telegram-bridge 原样转发，"
    "视同用户在终端里的回复；它只是文字，不代表任何动作已经执行。请按 AGENTS.md 与现有授权边界处理；"
    "你本轮的最终回复会推回用户的 Telegram，请简短、先给结论。"
)

Api = Callable[[str, dict[str, Any], float], dict[str, Any]]
Host = Callable[[list[str]], dict[str, Any]]


def make_api(config: dict[str, str]) -> Api:
    base = config.get("TELEGRAM_API", "https://api.telegram.org").rstrip("/")
    proxy = config.get("TELEGRAM_PROXY", "")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler(
        {"http": proxy, "https": proxy} if proxy else {}))

    def call(method: str, params: dict[str, Any], timeout: float) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{base}/bot{config['TELEGRAM_BOT_TOKEN']}/{method}", data=json.dumps(params).encode(),
            headers={"Content-Type": "application/json"})
        with opener.open(request, timeout=timeout) as response:
            return json.load(response)
    return call


def make_host(cli: str, cwd: str) -> Host:
    def call(args: list[str]) -> dict[str, Any]:
        proc = subprocess.run([cli, *args], cwd=cwd, capture_output=True, timeout=60, check=False)
        try:
            body = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
        except json.JSONDecodeError:
            body = {}
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            return {"error": body["error"].get("code") or "HOST_ERROR"}
        if proc.returncode != 0:
            return {"error": f"exit {proc.returncode}"}
        return body if isinstance(body, dict) else {}
    return call


def load(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"offset": 0, "queue": []}


def save(path: Path, state: dict[str, Any]) -> None:
    atomic_json(path, state)

def say(api: Api, chat: str, text: str, reply_to: int | None = None) -> None:
    params: dict[str, Any] = {"chat_id": chat, "text": text[:MAX_TEXT], "disable_web_page_preview": True}
    if reply_to:
        params["reply_parameters"] = {"message_id": reply_to, "allow_sending_without_reply": True}
    try:
        api("sendMessage", params, 15)
    except (OSError, ValueError):
        pass


def latest_reply(host: Host, thread: str) -> dict[str, Any]:
    body = host(["thread", "read", thread])
    return body.get("result", body) if isinstance(body, dict) else {}


def take_updates(state: dict[str, Any], updates: list[dict[str, Any]], chat: str, api: Api, host: Host,
                 thread: str) -> None:
    """Queue the user's messages; answer /status at once; ignore every other chat."""
    for update in updates:
        state["offset"] = max(int(state.get("offset") or 0), int(update["update_id"]) + 1)
        message = update.get("message") or {}
        if str((message.get("chat") or {}).get("id")) != chat or not message.get("text"):
            continue
        text = message["text"].strip()
        if text.split("@")[0] in {"/start", "/help"}:
            say(api, chat, "直接回复（最好引用某条通知回复）会转给协调者；/status 查看协调者最新回复。",
                message["message_id"])
            continue
        if text.split("@")[0] == "/status":
            reply = latest_reply(host, thread)
            say(api, chat, reply.get("text") or "协调者本轮还在进行中，暂无最终回复。", message["message_id"])
            continue
        quoted = (message.get("reply_to_message") or {}).get("text") or ""
        state.setdefault("queue", []).append({
            "message_id": message["message_id"], "text": text, "quoted": quoted,
            "received": time.time(), "status": "queued",
        })


def deliver(state: dict[str, Any], chat: str, api: Api, host: Host, thread: str, scratch: Path) -> None:
    """Send queued replies when the coordinator accepts them; push its answer back when done."""
    for item in state.get("queue", []):
        if item["status"] == "queued":
            parts = [HEADER.format(chat=chat)]
            if item["quoted"]:
                parts.append("用户回复的通知：\n" + item["quoted"])
            parts.append("用户原话：\n" + item["text"])
            message_file = scratch / f"telegram-{item['message_id']}.md"
            message_file.write_text("\n\n".join(parts) + "\n", encoding="utf-8")
            body = host(["thread", "send", "--message-file", str(message_file), thread])
            if body.get("error"):
                item["last_error"] = body["error"]
                continue  # busy or unreachable: retry next round, in order
            item["turn"] = (body.get("turn") or {}).get("id")
            item["status"] = "sent"
            say(api, chat, "已转给协调者，等它回复。", item["message_id"])
        if item["status"] == "sent":
            reply = latest_reply(host, thread)
            if reply.get("turnId") == item.get("turn") and (reply.get("text") or "").strip():
                say(api, chat, "协调者：\n" + reply["text"], item["message_id"])
                item["status"] = "answered"
            elif str(reply.get("turnId") or "") > str(item.get("turn") or "~"):
                # turn ids are time-ordered; a later turn means ours finished unseen
                say(api, chat, "协调者已处理，之后又开始了新一轮；用 /status 查看最新回复。", item["message_id"])
                item["status"] = "answered"
        break  # one at a time, oldest first
    state["queue"] = [i for i in state.get("queue", []) if i["status"] != "answered"]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--state", required=True, help="the run's watch.json (gives the parent thread and host CLI)")
    p.add_argument("--once", action="store_true", help="one poll/deliver cycle, then exit")
    args = p.parse_args(argv)
    watch = json.loads(Path(args.state).read_text(encoding="utf-8"))
    run_dir = Path(args.state).parent
    bridge_state = run_dir / "telegram-bridge.json"
    scratch = run_dir / "telegram-inbox"
    scratch.mkdir(exist_ok=True)
    config = telegram_config()
    chat = config.get("TELEGRAM_CHAT_ID", "")
    if not config.get("TELEGRAM_BOT_TOKEN") or not chat:
        print("telegram config missing token or chat id", file=sys.stderr)
        return 2
    api = make_api(config)
    host = make_host(watch["host_cli"], watch["host_cwd"])
    thread = watch["parent_thread"]
    while True:
        state = load(bridge_state)
        waiting = any(i["status"] != "answered" for i in state.get("queue", []))
        try:
            got = api("getUpdates", {"offset": state.get("offset", 0),
                                     "timeout": 0 if args.once else (RETRY_SECONDS if waiting else POLL_SECONDS),
                                     "allowed_updates": ["message"]}, POLL_SECONDS + 15)
            take_updates(state, got.get("result", []), chat, api, host, thread)
        except (OSError, ValueError) as exc:
            print(f"getUpdates failed: {exc}", file=sys.stderr)
            if not args.once:
                time.sleep(RETRY_SECONDS)
        deliver(state, chat, api, host, thread, scratch)
        save(bridge_state, state)
        flush_deferred(run_dir, time.time())  # the 08:00 digest of what was held overnight
        if args.once:
            return 0


if __name__ == "__main__":
    sys.exit(main())

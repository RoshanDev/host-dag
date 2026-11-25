"""Qoder 1.1.81 wire protocol; no scheduler, quota polling or session retries."""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path

MODEL = "DeepSeek-Flash"
CONTEXT_WINDOW = 400000
EFFORT = "medium"


def positive_integer(value):
    try:
        number = int(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError("context window must be a positive integer") from None
    if number <= 0:
        raise argparse.ArgumentTypeError("context window must be a positive integer")
    return number


def argv(model, cwd, prompt, context_window, effort):
    # Positional prompt + DEVNULL in the supervisor: Qoder otherwise concatenates
    # inherited stdin with the prompt. Keep default tools and session persistence.
    return [shutil.which("qodercli") or "qodercli", "-p", "--cwd", cwd,
            "--model", model, "--context-window", str(positive_integer(context_window)),
            "--reasoning-effort", effort, "--permission-mode", "bypass_permissions",
            "--max-model-request-retries", "0", "--output-format", "stream-json", "--", prompt]


def version():
    try:
        result = subprocess.run([shutil.which("qodercli") or "qodercli", "--version"],
                                stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def positive_or_unknown(value):
    return value if isinstance(value, (int, float)) and value > 0 else None


def final_result(log: Path, quota_pattern):
    result, init, session = {}, {}, None
    errors, tail, assistant_usage = [], [], []
    if log.exists():
        with log.open(errors="replace") as handle:
            for line in handle:
                tail = (tail + [line])[-40:]
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(event, dict):
                    continue
                session = event.get("session_id") or session
                if event.get("type") == "system" and event.get("subtype") == "init":
                    init = event
                if event.get("type") == "result":
                    result = event
                if event.get("type") == "error":
                    errors.append(event)
                message = event.get("message") or {}
                if event.get("type") == "assistant" and isinstance(message, dict) and message.get("usage"):
                    assistant_usage.append({"messageId": message.get("id"), "usage": message["usage"]})
    text = str(result.get("result") or "")
    if not text and errors:
        error = errors[-1]
        text = str(error.get("message") or error.get("error") or error)
    failed = bool(errors or result.get("is_error") or result.get("subtype") not in (None, "success"))
    usage = result.get("usage") or {}
    models = result.get("modelUsage") or {}
    # Do not estimate tokens from credits, context ratios, or the requested window.
    actual = {name: {"contextWindow": positive_or_unknown(model.get("contextWindow")),
                     "inputTokens": positive_or_unknown(model.get("inputTokens")),
                     "outputTokens": positive_or_unknown(model.get("outputTokens"))}
              for name, model in models.items() if isinstance(model, dict)}
    scan = json.dumps([result, errors], ensure_ascii=False) if result else "".join(tail)
    return {"sawResult": bool(result), "isError": failed, "resultText": text[:4000],
            "sessionId": result.get("session_id") or session,
            "quotaExhausted": bool(quota_pattern.search(scan)) and (failed or not result),
            "reportedModel": init.get("model"), "reportedCliVersion": init.get("qodercli_version"),
            "reportedEffort": None, "actualContextWindow": next(iter(actual.values()))["contextWindow"]
            if len(actual) == 1 else None,
            "actualUsage": {key: positive_or_unknown(usage.get(key)) for key in
                            ("input_tokens", "output_tokens", "cache_read_input_tokens", "cache_creation_input_tokens")},
            "actualModelUsage": actual, "rawUsage": usage, "rawModelUsage": models,
            "rawAssistantUsage": assistant_usage, "rawErrors": errors, "rawResult": result,
            "totalCredits": result.get("total_credits"), "durationMs": result.get("duration_ms"),
            "durationApiMs": result.get("duration_api_ms"), "tools": init.get("tools"),
            "usageNote": "Zero/missing token and context counters mean unknown; effort is requested, not attested."}

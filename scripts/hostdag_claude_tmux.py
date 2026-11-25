"""Run one Claude Code executor turn in the real interactive TUI inside a private tmux server.

User 2026-11-11 09:4x: 「claude -p 改 tmux」 (A/B 11-11: claude -p used about 1.5x the 5-hour window
of the interactive CLI for the same work). Binary, login and entrypoint label are unchanged; the
prompt is pasted into the normal `claude` UI and the session transcript is mirrored into cli.ndjson,
followed by one synthesized `result` row so host-dag-host-watch / final_result() read it like -p output.

Each executor gets its own tmux server (-L <unit>) inside its systemd unit, so stopping the unit
tears the session down without touching any other tmux session on the workstation.

Completion requires a fresh [DAG_TASK_COMPLETE] answer after task notifications, no pending
tool/background IDs and no active descendant shells. Idle time only debounces that gate.
HOST_DAG_CLAUDE_EXEC_TIMEOUT (seconds, default 7200) bounds the attempt;
HOST_DAG_CLAUDE_UNKNOWN_TIMEOUT (default 180) bounds idle unconfirmed terminal states.
Both must be finite and positive. Continuations are limited to two. Each subprocess has a
10-second bound. Failure diagnostics go to tmux-last-pane.txt and cli.ndjson; process_exit_code
is the actual pane status when available, separate from the supervisor's returned exit code.
"""
from __future__ import annotations

import json
import math
import os
import re
import signal
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

IDLE_SECONDS = 60          # debounce only; task/process completion is checked separately
READY_TIMEOUT = 90
SUBMIT_TIMEOUT = 90
BUSY_MARKERS = ("esc to interrupt",)
COMPLETE_MARKER = "[DAG_TASK_COMPLETE]"
OWN_LINE = ("This pasted text is my ticket contract for you (from the DAG coordinator); carry it out fully. "
            "Wait for all background tasks/shells, collect their exit codes and deliver the final ticket result. "
            "Only then put [DAG_TASK_COMPLETE] on its own line in your final answer.")
DIALOGS = (("trust this folder", ["Down", "Enter"]), ("yes, i accept", ["Down", "Enter"]))


def stop_task_id(value: dict) -> str | None:
    """Claude 2.1.322 TaskStop schema: task_id ?? shell_id (including its aliases)."""
    return value.get('task_id') if value.get('task_id') is not None else value.get('shell_id')


class Tmux:
    def __init__(self, socket: str, session: str = "exec"):
        self.socket, self.session = socket, session

    def run(self, *args: str, check: bool = False) -> subprocess.CompletedProcess:
        env = dict(os.environ, TERM=os.environ.get("TERM") or "xterm-256color")
        return subprocess.run(["tmux", "-L", self.socket, *args], capture_output=True, text=True,
                              env=env, check=check, timeout=10)

    def pane(self) -> str:
        return self.run("capture-pane", "-p", "-t", self.session).stdout

    def diagnostic_pane(self) -> str:
        # Scrollback is useful evidence, but must never be fed to quota classification.
        return self.run("capture-pane", "-p", "-t", self.session, "-S", "-200").stdout

    def keys(self, *keys: str) -> None:
        self.run("send-keys", "-t", self.session, *keys)

    def alive(self) -> bool:
        return self.run("has-session", "-t", self.session).returncode == 0

    def process_state(self) -> tuple[bool, int | None]:
        state = self.run("display-message", "-p", "-t", self.session,
                         "#{pane_dead}:#{pane_dead_status}", check=True).stdout.strip().split(':', 1)
        if len(state) != 2 or state[0] not in ('0', '1'):
            raise ValueError(f'Unknown pane state: {state!r}')
        return state[0] == '1', int(state[1]) if len(state) == 2 and state[1].isdigit() else None

    def kill(self) -> None:
        self.run("kill-server")

    def active_shells(self) -> list[int]:
        """Only descendants of this private pane; never inspect another executor's tasks."""
        root = int(self.run("display-message", "-p", "-t", self.session, "#{pane_pid}", check=True).stdout)
        listing = subprocess.run(["ps", "-eo", "pid=,ppid=,stat=,comm="], capture_output=True,
                                 text=True, check=True, timeout=10)
        processes = [line.split(None, 3) for line in listing.stdout.splitlines()]
        descendants = {root}
        while True:
            children = {int(pid) for pid, parent, _, _ in processes if int(parent) in descendants}
            if children <= descendants:
                break
            descendants |= children
        return [int(pid) for pid, _, state, name in processes if int(pid) in descendants - {root}
                and not state.startswith('Z') and name in ('sh', 'bash', 'zsh', 'dash', 'fish', 'ksh')]


class Tasks:
    """Track outstanding tool calls and CLI background IDs, including auto-backgrounded Bash."""
    def __init__(self):
        self.tools: set[str] = set()
        self.background: set[str] = set()
        self.background_launches: set[str] = set()
        self.calls: dict[str, tuple[str, dict]] = {}
        self.delivered = False

    def accept(self, row: dict) -> None:
        message = row.get('message') or {}
        content = message.get('content') or []
        blocks = content if isinstance(content, list) else []
        if row.get('type') == 'assistant':
            for block in blocks:
                if block.get('type') == 'tool_use' and block.get('id'):
                    self.calls[block['id']] = (block.get('name', ''), block.get('input') or {})
            self.tools.update(b['id'] for b in blocks if b.get('type') == 'tool_use' and b.get('id'))
            self.background_launches.update(b['id'] for b in blocks if b.get('type') == 'tool_use'
                and b.get('id') and (b.get('input') or {}).get('run_in_background'))
            stop, text = last_assistant([row])
            self.delivered = (stop == 'end_turn' and COMPLETE_MARKER in text.splitlines() and not self.pending())
        if row.get('type') != 'user':
            return
        result = row.get('toolUseResult') or {}
        background_id = (result.get('backgroundTaskId') or
                         (result.get('agentId') if result.get('status') == 'async_launched' else None)) if isinstance(result, dict) else None
        for block in blocks:
            if block.get('type') == 'tool_result':
                self.delivered = False
                tool, inputs = self.calls.pop(block.get('tool_use_id'), ('', {}))
                succeeded = (not block.get('is_error') and isinstance(result, dict)
                             and not result.get('error') and not result.get('is_error')
                             and result.get('success') is not False)
                if succeeded and tool in ('TaskStop', 'KillShell', 'KillBash'):
                    target = stop_task_id(inputs)
                    returned = stop_task_id(result)
                    # All current aliases return TaskStop's task_id/message. Keep old shell receipts readable.
                    prefix = ('Successfully stopped task: ' if result.get('task_id') is not None
                              else 'Successfully killed shell: ')
                    # A stop request alone is not evidence. Require the matching successful CLI receipt.
                    if (target and returned == target
                            and re.match(re.escape(prefix + target) + r'(?:\s|\(|$)', result.get('message', ''))):
                        self.background.discard(target)
                # A TaskOutput terminal status also proves a task exited without a notification.
                if succeeded and tool == 'TaskOutput':
                    task = result.get('task') or {}
                    if (isinstance(task, dict) and task.get('status') in ('completed', 'failed', 'killed')
                            and (task.get('task_id') or task.get('id')) == inputs.get('task_id')):
                        self.background.discard(inputs.get('task_id'))
                text = block.get('content')
                task_id = background_id
                if isinstance(text, str):
                    match = re.search(r'Command running in background with ID: ([\w-]+)', text)
                    if match:
                        task_id = match[1]
                if task_id:
                    self.background.add(task_id)
                # Unknown background launch formats fail closed until the execution deadline.
                if task_id or block.get('is_error') or block.get('tool_use_id') not in self.background_launches:
                    self.tools.discard(block.get('tool_use_id'))
        if isinstance(result, dict):
            if background_id:
                self.background.add(background_id)
        # CLI notifications are user events, never assistant prose or echoed tool output.
        texts = [content] if isinstance(content, str) else [b.get('text', '') for b in blocks if b.get('type') == 'text']
        for text in texts:
            for body in re.findall(r'<task-notification>(.*?)</task-notification>', text, re.S):
                self.delivered = False
                task_id = re.search(r'<task-id>([\w-]+)</task-id>', body)
                if task_id and re.search(r'<status>(completed|failed|killed)</status>', body):
                    self.background.discard(task_id[1])

    def pending(self) -> bool:
        return bool(self.tools or self.background)


def transcript_path(session_id: str) -> Path | None:
    hits = list((Path.home() / ".claude" / "projects").glob(f"*/{session_id}.jsonl"))
    return hits[0] if hits else None


def last_assistant(rows: list[dict[str, Any]]) -> tuple[str | None, str]:
    """stop_reason and text of the last assistant message in the transcript."""
    for row in reversed(rows):
        message = row.get("message") if isinstance(row, dict) else None
        if row.get("type") == "assistant" and isinstance(message, dict):
            text = "".join(b.get("text", "") for b in message.get("content") or [] if isinstance(b, dict) and b.get("type") == "text")
            return message.get("stop_reason"), text
    return None, ""


def quota_error(rows: list[dict], pane: str, prompt: str) -> str:
    """CLI error events first; UI fallback only accepts complete known error lines.

    Normal transcript messages, tool output, prompt echo and scrollback are not errors.
    """
    for row in reversed(rows):
        if (row.get('isApiErrorMessage') or row.get('type') == 'error' or
                (row.get('type') == 'result' and row.get('is_error'))):
            diagnostic = json.dumps(row, ensure_ascii=False)
            if re.search(r'rate_limit|rate limit|usage limit|quota|insufficient credits|hit your limit', diagnostic, re.I):
                return diagnostic
        # Do not reuse an old recovered API error after a later assistant/tool turn.
        if row.get('type') == 'assistant':
            break
    prose = prompt
    for row in rows:
        if row.get('isApiErrorMessage'):
            continue
        content = (row.get('message') or {}).get('content') or []
        if isinstance(content, str):
            prose += '\n' + content
        else:
            for block in content:
                text = block.get('text') if block.get('type') == 'text' else block.get('content')
                if isinstance(text, str):
                    prose += '\n' + text
    for line in pane.splitlines():
        line = line.strip().removeprefix('⎿').strip()
        if re.fullmatch(r"(?:You've hit your limit|You have hit your usage limit)(?:[ .·—-]+resets? .+)?[.!]?|"
                        r'API Error: 429(?: .+)?', line, re.I) and line not in prose:
            return line
    return ''


def timeout_setting(name: str, default: int) -> float:
    value = float(os.environ.get(name, default))
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')
    return value


class StopSupervision(Exception):
    def __init__(self, reason: str, detail: str = ""):
        self.reason, self.detail = reason, detail


def supervise(argv: list[str], cwd: str, prompt: str, log: Path, stderr: Path, socket: str,
              quota_pattern: re.Pattern) -> tuple[int, bool]:
    """Return supervisor status, not a fabricated Claude process exit code."""
    session_id = str(uuid.uuid4())
    cmd = [a for a in argv if a not in ("-p", "--verbose")]
    if "--output-format" in cmd:
        i = cmd.index("--output-format"); del cmd[i:i + 2]
    cmd += ["--session-id", session_id, "--disallowedTools", "AskUserQuestion"]
    tmux = Tmux(socket)
    cancelled, reason, detail, process_exit_code = False, 'unknown', '', None
    rows: list[dict[str, Any]] = []
    tasks = Tasks()
    shells: list[int] = []
    offset = 0
    err = stderr.open("a")

    def note(msg: str) -> None:
        err.write(time.strftime("%FT%T ") + msg + "\n"); err.flush()

    def interrupted(_s, _f):
        nonlocal cancelled
        cancelled = True

    def check_cancel():
        if cancelled:
            raise StopSupervision('cancelled')

    def keys(*values):
        check_cancel()
        tmux.keys(*values)

    def run(*values, **kwargs):
        check_cancel()
        return tmux.run(*values, **kwargs)

    def check_alive():
        nonlocal process_exit_code
        if not tmux.alive():
            raise StopSupervision('tui_exited', 'Claude TUI session disappeared; process status unknown')
        dead, process_exit_code = tmux.process_state()
        if dead:
            raise StopSupervision('tui_exited', f'Claude TUI unexpectedly exited: {process_exit_code}')

    prior = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        execution_deadline = time.monotonic() + timeout_setting('HOST_DAG_CLAUDE_EXEC_TIMEOUT', 7200)
        unknown_timeout = timeout_setting('HOST_DAG_CLAUDE_UNKNOWN_TIMEOUT', 180)
        # Set remain-on-exit before launching Claude, including an immediately crashing CLI.
        run("new-session", "-d", "-s", tmux.session, "-x", "220", "-y", "60", "-c", cwd, check=True)
        run("set-option", "-w", "-t", tmux.session, "remain-on-exit", "on", check=True)
        run("respawn-pane", "-k", "-t", tmux.session, " ".join(map(shlex_quote, cmd)), check=True)
        note(f"tmux -L {socket} session {session_id} started")
        deadline = time.monotonic() + READY_TIMEOUT
        while True:
            check_cancel()
            if time.monotonic() >= deadline:
                raise StopSupervision('ready_timeout')
            time.sleep(2)
            check_cancel(); check_alive()
            pane = tmux.pane().lower()
            dialog = next((k for t, k in DIALOGS if t in pane), None)
            if dialog:
                for key in dialog:
                    keys(key); time.sleep(1)
                continue
            if "❯" in pane:
                break
        check_cancel()
        buf = log.with_name("tmux-prompt.txt"); buf.write_text(prompt)
        run("load-buffer", "-b", "p", str(buf), check=True)
        run("paste-buffer", "-p", "-d", "-b", "p", "-t", tmux.session, check=True)
        time.sleep(2)
        run("send-keys", "-t", tmux.session, "-l", OWN_LINE, check=True)
        time.sleep(1); keys("Enter")
        path, deadline = None, time.monotonic() + SUBMIT_TIMEOUT
        while path is None:
            check_cancel()
            if time.monotonic() >= deadline:
                raise StopSupervision('submit_timeout')
            time.sleep(3)
            check_cancel(); check_alive()
            path = transcript_path(session_id)
            if path is None and time.monotonic() > deadline - SUBMIT_TIMEOUT / 2:
                keys("Enter")
        quiet_since = time.monotonic()
        continuation_at, continuations = quiet_since, 0
        unknown_since = None
        with log.open("ab") as out:
            while True:
                check_cancel()
                time.sleep(5)
                check_cancel()
                if time.monotonic() >= execution_deadline:
                    raise StopSupervision('execution_timeout', 'Execution deadline exceeded; task not confirmed complete')
                grew = False
                if path.exists():
                    with path.open("rb") as fh:
                        fh.seek(offset)
                        while (raw := fh.readline()).endswith(b"\n"):
                            offset = fh.tell(); out.write(raw); grew = True
                            try:
                                row = json.loads(raw)
                                if isinstance(row, dict):
                                    rows.append(row); tasks.accept(row)
                            except ValueError:
                                pass
                    out.flush()
                if grew:
                    quiet_since = time.monotonic()
                check_alive()
                pane = tmux.pane()
                busy = any(m in pane.lower() for m in BUSY_MARKERS)
                stop, text = last_assistant(rows)
                shells = tmux.active_shells()
                if shells:
                    tasks.delivered = False
                pending = tasks.pending() or bool(shells)
                quota = quota_error(rows, pane, prompt)
                if not busy and quota:
                    raise StopSupervision('quota_exhausted', quota)
                if (tasks.delivered and stop == "end_turn" and not pending
                        and not busy and time.monotonic() - quiet_since > IDLE_SECONDS):
                    check_cancel()
                    reason = 'completed'
                    break
                # Known outstanding work waits for notifications (or the total deadline).
                # An idle, incomplete turn may be continued at most twice; never rerun a command here.
                if not busy and not pending:
                    if unknown_since is None or grew:
                        unknown_since = time.monotonic()
                    if time.monotonic() - unknown_since >= unknown_timeout:
                        raise StopSupervision('unknown_terminal', f'Idle without confirmed delivery; stop_reason={stop}')
                else:
                    unknown_since = None
                if (not busy and stop in ('end_turn', 'max_tokens') and continuations < 2
                        and time.monotonic() - max(quiet_since, continuation_at) > IDLE_SECONDS):
                    run('send-keys', '-t', tmux.session, '-l',
                        'Continue the ticket: wait for outstanding tasks, collect exit codes and deliver the final result. '
                        'Do not restart running commands. Only after all work is finished emit [DAG_TASK_COMPLETE] '
                        'on its own line.', check=True)
                    keys('Enter')
                    continuations += 1
                    continuation_at = time.monotonic()
    except StopSupervision as exc:
        reason, detail = exc.reason, exc.detail
    except (subprocess.SubprocessError, OSError, ValueError) as exc:
        reason, detail = 'transport_error', str(exc)
    finally:
        # One cancellation/failure path: never submit a contract or continuation after cancellation.
        if cancelled:
            reason = 'cancelled'
        try:
            pane = tmux.diagnostic_pane()
            log.with_name('tmux-last-pane.txt').write_text(pane)
            if reason == 'completed' and not cancelled:
                tmux.keys('/exit', 'Enter')
                time.sleep(3)
                if tmux.alive():
                    _, process_exit_code = tmux.process_state()
                    if process_exit_code not in (None, 0):
                        reason, detail = 'tui_shutdown_error', f'Claude shutdown exit: {process_exit_code}'
            elif cancelled and tmux.alive():
                tmux.keys('Escape')
        except (subprocess.SubprocessError, OSError, ValueError) as exc:
            note('final diagnostic: ' + str(exc))
        if cancelled:
            reason = 'cancelled'
        ok = reason == 'completed'
        _, text = last_assistant(rows)
        result = {'type': 'result', 'subtype': 'success' if ok else 'error', 'is_error': not ok,
                  'result': text if ok else (detail or reason), 'session_id': session_id,
                  'transport': 'tmux', 'reason': reason, 'process_exit_code': process_exit_code,
                  'quota_exhausted': reason == 'quota_exhausted',
                  'pending_tools': sorted(tasks.tools), 'pending_tasks': sorted(tasks.background),
                  'active_shells': shells}
        with log.open('ab') as out:
            out.write((json.dumps(result, ensure_ascii=False) + '\n').encode())
        note(f'{reason}: {detail}')
        try:
            tmux.kill()
        except (subprocess.SubprocessError, OSError) as exc:
            note('server cleanup: ' + str(exc))
        finally:
            for sig, handler in prior.items():
                signal.signal(sig, handler)
            err.close()
    return (0 if ok else 1), cancelled


def shlex_quote(s: str) -> str:
    import shlex
    return shlex.quote(s)

"""Small shared JSON file I/O for live DAG metadata.

Readers reopen after each transient failure (initial read + three retries).
Atomic publication prevents partial JSON visibility; it does not serialize
read/modify/write transactions, whose existing locks remain required.
"""
import json
import os
from pathlib import Path
import tempfile
import time


def public_path(path):
    """Stable logical identity for run metadata and its sibling locks/control.

    Normalize spelling only. Resolving a metadata symlink pins its current SSD
    copy and loses both the public lock directory and later rollback updates.
    Only publication or explicit physical-boundary checks may resolve targets.
    """
    return Path(os.path.abspath(Path(path).expanduser()))


def read_json(path):
    for attempt in range(4):
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except FileNotFoundError:
            # Atomic writers never leave the target briefly absent; absence is
            # a normal state for optional files (board-freeze.json) and is hot.
            raise
        except (json.JSONDecodeError, OSError):
            if attempt == 3:
                raise
            time.sleep(0.5)


def atomic_text(path, text, *, durable=False, before_replace=None):
    # Keep the public symlink and publish on the target filesystem. Transaction
    # locks must still use the public path (run.json.lock never moves).
    path = Path(path).resolve()
    # Callers own directory creation and transaction locks.
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            if path.exists():
                os.fchmod(stream.fileno(), path.stat().st_mode & 0o777)
            stream.write(text)
            if durable:
                stream.flush()
                os.fsync(stream.fileno())
        if before_replace is not None:
            before_replace()
        os.replace(tmp, path)
        if durable:
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        tmp.unlink(missing_ok=True)


def atomic_json(path, value, *, durable=False):
    atomic_text(path, json.dumps(value, ensure_ascii=False, indent=2) + "\n", durable=durable)


def log_desktop_notice(run_dir, key, title, body, at=None):
    """Append a directly shown desktop notice to <run>/alerts.jsonl; never raise.

    Fable L2174 §1a: quota/rotation reminders keep their direct notify-send (no
    hostdag_self_heal.alert hourly dedupe) and add the full text here in the same
    record shape as alert() ({at,key,title,body,desktop}); desktop=true means
    "meant for the desktop", not a delivery receipt. Only a run root (run.json
    present) gets a log; append mode follows a segment-one symlink. Callers must
    pop up first: a failing or stalled log write never blocks the popup.
    Never use this for the host-dag OTP (hostdag_dag_cli.notify_desktop).
    """
    try:
        if run_dir is None:
            return False
        run_dir = Path(run_dir)
        if not (run_dir / "run.json").exists():
            return False
        record = {"at": time.time() if at is None else at, "key": str(key), "title": str(title),
                  "body": str(body), "desktop": True}
        with (run_dir / "alerts.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        return True
    except Exception:  # noqa: BLE001 - logging is best effort by contract
        return False

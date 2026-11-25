"""Host DAG stage B4: the core side of ``host-dag`` (scripts/host-dag-view.py).

Design: docs/local-dev/dag-state-machine-design-20261019.md §8 (shadow mirror, old CLI translation),
§10 B4, §13.4 (pinned interpreter, ``sm quiesce``), §14.1 (one connection per operation), §15 (status).

Two things live here:

1. ``mirror_ticket_set`` / ``mirror_batch_set``: after a legacy ``ticket set`` / ``batch set`` has
   written run.json (the authority while ``meta.mode`` is shadow), and before it prints, the same
   invocation is translated (hostdag_dag_translate) and submitted to ``<run>/sm/state.db`` as ONE
   compound transaction. These functions run synchronously inside host-dag's bounded daemon worker
   (``core_mirror`` in host-dag-view.py: budget default 2 s), never print, and never raise:
   - no ``sm/state.db``: nothing happens at all (the run is not seeded);
   - disabled (``HOST_DAG_DAG_MIRROR=0``), quiesced, not the pinned interpreter, SQLite too old, disk
     low, busy, core mode, or any exception: the result carries the text of host-dag's single stderr
     line and ``lost=True`` (host-dag spools the write, ``spool_lost``); an alert row goes to
     ``sm/alerts.jsonl`` when ``sm/`` is writable (none when quiesced: the disk may be on its way out);
   - a core refusal is a shadow *result*, not a failure: it goes to ``sm/mirror.jsonl`` silently.
   The connection is opened for this one operation and closed (quiesce relies on it, §14.1).

2. The core-only commands (``exec request``, ``question ask/link``, ``decide``, ``decisions
   --pending``, ``wait add/update/waive``, ``pagewindow enqueue/policy``, ``authority switch``,
   ``sm quiesce/resume/status``). They need ``state.db`` and the pinned interpreter and refuse with a
   clear message otherwise. In shadow mode they write the core only; run.json is unchanged.
"""

from __future__ import annotations

from hostdag_json import public_path

import hashlib
import secrets
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping, Sequence, TextIO

from hostdag_advisor_log import is_final_answer
import hostdag_dag_controller as ctl
import hostdag_dag_core as c
import hostdag_dag_resolvers as r
import hostdag_dag_translate as tr
from hostdag_dag_core import Json, JsonObj

ENV_MIRROR = "HOST_DAG_DAG_MIRROR"  # "0" turns the mirror off
ENV_ALLOW_UNPINNED = "HOST_DAG_DAG_ALLOW_UNPINNED"  # "1": tests / development on another >= 3.51.3 interpreter
ENV_BUDGET = "HOST_DAG_DAG_MIRROR_BUDGET"  # seconds
ENV_RUN_ID = "HOST_DAG_DAG_RUN_ID"
ENV_CHECK_CMD = "HOST_DAG_PAGE_WINDOW_CHECK_CMD"  # tests: replaces `<browser python> page-window.py check`
DEFAULT_BUDGET = 2.0
MIRROR_BUSY_MS = 250
MIRROR_LOG = "mirror.jsonl"
PAGE_WINDOW_PYTHON = os.environ.get("HOST_DAG_PAGE_WINDOW_PYTHON") or sys.executable
PAGE_WINDOW_CLI = Path(__file__).resolve().parent / "local-dev/page-window.py"


class CliError(Exception):
    """The core command cannot run here (no database, wrong interpreter, quiesced, gate not met)."""


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def pinned(executable: str | None = None) -> bool:
    try:
        return Path(executable or sys.executable).resolve() == c.SM_PYTHON.resolve()
    except OSError:
        return False


def interpreter_problem() -> str | None:
    """Why this process must not open state.db (design §13.2), or None."""
    try:
        c.check_sqlite_version()
    except c.SqliteVersionError as exc:
        return str(exc)
    if not pinned() and os.environ.get(ENV_ALLOW_UNPINNED) != "1":
        return f"interpreter {sys.executable} is not the pinned {c.SM_PYTHON} (design §13.2)"
    return None


def run_id_for(run_dir: Path) -> str:
    return valid_run_id(os.environ.get(ENV_RUN_ID) or ctl.default_run_id(run_dir))


def sm_dir(run_dir: Path) -> Path:
    return Path(run_dir) / ctl.SM_DIR


def db_path(run_dir: Path) -> Path:
    return sm_dir(run_dir) / ctl.DB_NAME


def _budget() -> float:
    try:
        return max(0.1, float(os.environ.get(ENV_BUDGET) or DEFAULT_BUDGET))
    except ValueError:
        return DEFAULT_BUDGET


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def spec_digest(path: str) -> str | None:
    try:
        return hashlib.sha256(Path(path).expanduser().read_bytes()).hexdigest()
    except OSError:
        return None


# ---------------------------------------------------------------- the shadow mirror


@dataclass
class MirrorResult:
    """What one mirror attempt came to. ``warn`` is the text of the caller's single stderr line (None:
    stay silent); ``lost`` means the core did not see this legacy write (spool it, review P2-5)."""
    status: str  # absent | disabled | quiesced | skipped | disk_low | accepted | refused | busy | mode | error
    detail: str = ""
    warn: str | None = None
    lost: bool = False
    row: JsonObj = field(default_factory=dict)


SILENT = ("absent", "disabled", "accepted", "refused")


class _Mirror:
    """One mirror attempt, run synchronously INSIDE the caller's bounded worker thread (host-dag's
    ``core_mirror``): every file-system touch of the possibly stalled run disk (state.db stat, quiesce
    marker, statvfs, os.access, the spec hash, sm/ alerts and log) happens here, never on the thread that
    prints the legacy result (review P2-5). Never prints."""

    def __init__(self, run_dir: Path, command: str, ticket: str | None,
                 statvfs: Callable[[str], r._Statvfs] = os.statvfs) -> None:
        self.run_dir = Path(run_dir)
        self.sm = sm_dir(self.run_dir)
        self.command = command
        self.ticket = ticket
        self.statvfs = statvfs
        self.at = now_iso()

    def fail(self, status: str, detail: str, *, alert: bool = True) -> MirrorResult:
        if alert:
            self.append(ctl.ALERTS_LOG, {"at": self.at, "kind": f"mirror_{status}", "source": "host-dag",
                                         "command": self.command, "ticket": self.ticket, "detail": detail[:2000]})
        return MirrorResult(status, detail, f"{status}: {detail}", True)

    def append(self, name: str, row: JsonObj) -> None:
        try:
            if self.sm.is_dir() and os.access(self.sm, os.W_OK):
                ctl.SmFiles(self.sm).append(name, [row])
        except Exception:  # noqa: BLE001 - logging is best effort too
            pass

    def preflight(self) -> MirrorResult | None:
        if os.environ.get(ENV_MIRROR) == "0":
            return MirrorResult("disabled")
        if not db_path(self.run_dir).is_file():
            return MirrorResult("absent")
        if (self.sm / c.QUIESCE_MARKER).exists():
            return self.fail("quiesced", f"{self.sm / c.QUIESCE_MARKER} exists", alert=False)
        problem = interpreter_problem()
        if problem:
            return self.fail("skipped", problem)
        try:
            disk = r.disk_status(self.sm, self.statvfs)
        except OSError as exc:
            return self.fail("skipped", f"statvfs: {exc}")
        if not disk.ok:
            return self.fail("disk_low", disk.reason)
        return None

    def run(self, work: Callable[[c.SqliteStore], MirrorResult], *, checked: bool = False) -> MirrorResult:
        pre = None if checked else self.preflight()
        if pre is not None:
            return pre
        started = time.monotonic()
        try:
            conn = c.connect(db_path(self.run_dir), run_id_for(self.run_dir), busy_timeout_ms=MIRROR_BUSY_MS)
            no_checkpoint_on_close(conn)
        except c.QuiescedError as exc:
            return self.fail("quiesced", str(exc), alert=False)
        except Exception as exc:  # noqa: BLE001
            return self.fail("error", f"{type(exc).__name__}: {exc}")
        try:
            store = c.SqliteStore(conn, max_wait_seconds=min(0.5, _budget() / 2))
            result = work(store)
        except c.BusyError as exc:
            result = MirrorResult("busy", str(exc))
        except Exception as exc:  # noqa: BLE001
            result = MirrorResult("error", f"{type(exc).__name__}: {exc}")
        finally:
            conn.close()
        result.row = {"at": self.at, "command": self.command, "ticket": self.ticket, "status": result.status,
                      "detail": result.detail[:2000], **result.row, "seconds": round(time.monotonic() - started, 3)}
        self.append(MIRROR_LOG, result.row)
        if result.status in SILENT:
            return result
        failed = self.fail(result.status, result.detail)
        failed.row = result.row
        return failed


def no_checkpoint_on_close(conn: sqlite3.Connection) -> None:
    """core-mirror: the last connection to close checkpoints the WAL, and on the busy USB run
    disk that fsync alone took ~1.1 s of a 2 s budget (COMMIT's own fsync ~0.7 s). A mirror is one short
    write; the controller's cycles and ``sm quiesce`` (explicit TRUNCATE checkpoint) keep the WAL short."""
    flag = getattr(sqlite3, "SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE", None)
    if flag is not None and hasattr(conn, "setconfig"):
        conn.setconfig(flag, True)


def _receipts(receipts: Sequence[c.Receipt]) -> list[Json]:
    return [x.to_json() for x in receipts]


def mirror_ticket_set(run_dir: Path, req: tr.TicketSet | Callable[[], tr.TicketSet], before: object, after: object,
                      *, statvfs: Callable[[str], r._Statvfs] = os.statvfs, base: str | None = None) -> MirrorResult:
    """Translate and submit one successful legacy ``ticket set`` (see module doc). Never raises. ``req``
    may be a builder, so reading the spec file for its digest happens here, in the bounded worker."""
    m = _Mirror(run_dir, "ticket set", req.ticket if isinstance(req, tr.TicketSet) else None, statvfs)
    try:
        request = req if isinstance(req, tr.TicketSet) else req()
        m.ticket = request.ticket
        pre = m.preflight()
        if pre is not None:
            return pre
        legacy_before, legacy_after = tr.LegacyTicket.load(before), tr.LegacyTicket.load(after)
        tracked = tr.tracked_sessions(_read_json(Path(run_dir) / "watch.json"))
        op_base = base or tr.new_base()

        def work(store: c.SqliteStore) -> MirrorResult:
            holder: list[tr.Translation] = []

            def plan(world: c.World) -> list[c.Op]:
                if world.meta.mode != "shadow":
                    raise _ModeSkip(f"core authority mode is {world.meta.mode}; ticket set stays legacy-only "
                                    "until stage C")
                holder.clear()
                out = tr.plan_ticket_set(request, legacy_before, legacy_after, world, tracked, m.at, op_base)
                holder.append(out)
                return out.ops
            try:
                receipts = store.submit_batch(plan)
            except _ModeSkip as exc:
                return MirrorResult("mode", str(exc))
            except (c.Rejected, c.ValidationError, c.OpConflict, c.InvariantError) as exc:
                return MirrorResult("refused", f"{type(exc).__name__}: {exc}",
                                    row={"base": op_base, "shape": list(req_shape(request))})
            done = holder[0] if holder else tr.Translation()
            return MirrorResult("accepted", f"{len(receipts)} op(s)",
                                row={"base": op_base, "translation": done.to_json(), "receipts": _receipts(receipts)})
        return m.run(work, checked=True)
    except Exception as exc:  # noqa: BLE001 - the live command already succeeded
        return m.fail("error", f"{type(exc).__name__}: {exc}")


class _ModeSkip(c.CoreError):
    pass


def req_shape(req: tr.TicketSet) -> tuple[str, ...]:
    return tuple(sorted(req.given()))


def mirror_batch_set(run_dir: Path, batch: str, given: Sequence[str]) -> MirrorResult:
    """``batch set`` has no core equivalent (row B01): record that, touch nothing else."""
    m = _Mirror(run_dir, "batch set", None)
    try:
        if os.environ.get(ENV_MIRROR) == "0" or not db_path(Path(run_dir)).is_file():
            return MirrorResult("absent")
        if (m.sm / c.QUIESCE_MARKER).exists():
            return MirrorResult("quiesced")
        out = tr.plan_batch_set(given)
        row: JsonObj = {"at": m.at, "command": "batch set", "batch": batch, "status": "skipped",
                        "translation": out.to_json()}
        m.append(MIRROR_LOG, row)
        return MirrorResult("skipped", "batch metadata is not core state", None, False, row)
    except Exception as exc:  # noqa: BLE001
        return m.fail("error", f"{type(exc).__name__}: {exc}")


# ---- the spool of legacy writes the core missed (review P2-5)
#
# When a mirror fails or times out, the legacy write is in run.json but not in the core. It is appended
# to a small spool on the INTERNAL disk (default ~/.local/state/host-dag-dag/<run_id>/mirror-spool.jsonl,
# next to the B3 backup), never under the run directory: the run disk may be the thing that stalled.
# A row names the batch, the ticket and the flags that were given, and keeps only sha256 digests of the
# field values and of the ticket record before/after (re-review N2: no ticket bodies or free text);
# reconciliation compares them with run.json. The file rotates at 5 MiB, one previous file kept. B7 seed
# / a later reconciliation uses it; nothing reads it automatically yet.

ENV_SPOOL_ROOT = "HOST_DAG_DAG_SPOOL_ROOT"
SPOOL_NAME = "mirror-spool.jsonl"
SPOOL_MAX_BYTES = 5 * 1024 * 1024
DIGESTED = ("fields", "before", "after")


def valid_run_id(value: str) -> str:
    """Re-review N1: a run id becomes a path component; same rule as capacity pool names."""
    if not r.POOL_NAME.fullmatch(value):
        raise CliError(f"run id {value!r} is not a plain name ({r.POOL_NAME.pattern})")
    return value


def run_id_no_io(run_dir: Path) -> str:
    """``ctl.default_run_id`` without touching the file system (no resolve(): the disk may hang)."""
    if os.environ.get(ENV_RUN_ID):
        return valid_run_id(str(os.environ[ENV_RUN_ID]))
    path = Path(os.path.abspath(run_dir))
    return valid_run_id(path.parent.name if path.name == "local-dev-dag" else path.name)


def spool_path(run_dir: Path) -> Path:
    root = Path(os.environ.get(ENV_SPOOL_ROOT) or ctl.DEFAULT_BACKUP_ROOT)
    return root / run_id_no_io(run_dir) / SPOOL_NAME


def _sha(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def spool_record(record: Mapping[str, object]) -> JsonObj:
    """What the spool keeps of one legacy write: ids, flag names, digests."""
    fields = record.get("fields")
    flags = sorted(k for k, v in fields.items() if v is not None and v is not False) if isinstance(fields, dict) else []
    out: JsonObj = {"batch": str(record.get("batch") or ""), "flags": list(flags)}
    for key in ("after_flag", "by", "affects_downstream"):
        if record.get(key) is not None:
            flags.append(key)
            out["flags"] = sorted(flags)
    for key in DIGESTED:
        if key in record:
            out[f"{key}_sha256"] = _sha(record[key])
    return out


def spool_lost(run_dir: Path, *, command: str, ticket: str | None, status: str, detail: str,
               record: Mapping[str, object]) -> Path | None:
    """Append one lost legacy write. Never raises; returns where it went (None: could not write).
    The file is opened O_NOFOLLOW and its directory must not be a symlink (re-review N1)."""
    try:
        path = spool_path(run_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.parent.is_symlink():
            return None
        try:
            if path.stat().st_size >= SPOOL_MAX_BYTES and not path.is_symlink():
                os.replace(path, path.with_name(path.name + ".1"))  # keep one previous file
        except FileNotFoundError:
            pass
        row = {"at": now_iso(), "run_dir": os.path.abspath(run_dir), "run_id": run_id_no_io(run_dir),
               "command": command, "ticket": ticket, "status": status, "detail": detail[:500],
               "record": spool_record(record)}
        data = (json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
        fd = os.open(path, flags, 0o600)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        return path
    except Exception:  # noqa: BLE001
        return None


# ---- the retry queue of ticket-set mirrors whose outcome is unknown (core-mirror)
#
# The spool above only keeps digests, so nothing could replay a lost write: 347 of 535 spooled rows of
# run 01a0b493 were "timeout: outcome unknown", among them the acceptances the core then lacked. Now
# host-dag fixes the op base before the mirror starts, and a ticket set that did not finish (timeout,
# busy, error, interrupted) is also queued here with what is needed to replay it: the request, the
# ticket before/after and that base. One file per write under <spool dir>/mirror-retry/ (internal disk,
# 0600, O_NOFOLLOW; it holds the request text until drained, unlike the digest-only spool).
#
# Draining is idempotent: if the core already has ops <base>-NN (the slow original did commit), the
# entry is resolved as applied; otherwise the same base is replayed, so a racing original and a replay
# cannot both apply (op ids are unique; a compound is accepted whole). Entries drain oldest first, under
# a non-blocking flock, at the start of the next mirror worker and by `host-dag sm retry`; each attempt
# and outcome is logged to <spool dir>/mirror-retry.jsonl. RETRY_MAX failed attempts park an entry in
# mirror-retry/failed/ with an alert row; `host-dag sm reconcile` covers what is left behind.

RETRY_DIR = "mirror-retry"
RETRY_LOG = "mirror-retry.jsonl"
RETRY_MAX = 20
RETRY_DONE = ("accepted", "refused", "absent", "disabled", "mode", "applied")


def retry_dir(run_dir: Path) -> Path:
    return spool_path(run_dir).parent / RETRY_DIR


def _private_write(path: Path, data: Mapping[str, object], *, append: bool = False) -> None:
    raw = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str) + "\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    flags |= os.O_APPEND if append else os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        os.write(fd, raw.encode("utf-8"))
    finally:
        os.close(fd)


def _retry_log(run_dir: Path, row: Mapping[str, object]) -> None:
    try:
        _private_write(spool_path(run_dir).parent / RETRY_LOG, {"at": now_iso(), **row}, append=True)
    except Exception:  # noqa: BLE001
        pass


def queue_retry(run_dir: Path, *, base: str, status: str, detail: str, request: Mapping[str, object],
                before: object, after: object) -> Path | None:
    """Queue one ticket-set mirror for replay with the same op base. Never raises; None: not queued."""
    try:
        folder = retry_dir(run_dir)
        folder.mkdir(parents=True, exist_ok=True, mode=0o700)
        if folder.is_symlink() or folder.parent.is_symlink():
            return None
        path = folder / f"{time.time_ns():020d}-{base}.json"
        _private_write(path, {"base": base, "run_dir": os.path.abspath(run_dir), "queued_at": now_iso(),
                              "status": status, "detail": detail[:500], "attempts": 0, "request": dict(request),
                              "before": before, "after": after})
        _retry_log(run_dir, {"event": "queued", "base": base, "ticket": request.get("ticket"), "status": status})
        return path
    except Exception:  # noqa: BLE001
        return None


def request_json(req: tr.TicketSet) -> JsonObj:
    return {"batch": req.batch, "ticket": req.ticket, "fields": json.loads(json.dumps(dict(req.fields), default=str)),
            "after": req.after, "affects_downstream": list(req.affects_downstream)
            if req.affects_downstream is not None else None, "by": req.by, "spec_digest": req.spec_digest, "run_dir": req.run_dir}


def request_load(o: Mapping[str, object]) -> tr.TicketSet:
    """The queued request; a spec digest the timed-out command never got to is computed now."""
    affects, fields = o.get("affects_downstream"), dict(o.get("fields") or {})  # type: ignore[call-overload]
    digest = o.get("spec_digest")
    if digest is None and isinstance(fields.get("window_spec"), str):
        digest = spec_digest(fields["window_spec"])
    return tr.TicketSet(str(o["batch"]), str(o["ticket"]), fields, after=o.get("after"),  # type: ignore[arg-type]
                        by=o.get("by"), spec_digest=digest, run_dir=o.get("run_dir"),  # type: ignore[arg-type]
                        affects_downstream=tuple(affects) if isinstance(affects, list) else None)


def _applied(run_dir: Path, base: str) -> list[str]:
    """Op ids of this base the core already holds (read-only connection)."""
    conn = sqlite3.connect(f"file:{db_path(run_dir)}?mode=ro", uri=True, isolation_level=None, timeout=1)
    try:
        return [row[0] for row in conn.execute("SELECT op_id FROM ops WHERE op_id >= ? AND op_id < ? ORDER BY op_id",
                                               (base + "-", base + ".")).fetchall()]
    finally:
        conn.close()


def drain_retry_queue(run_dir: Path, *, dry_run: bool = False, deadline: float | None = None,
                      statvfs: Callable[[str], r._Statvfs] = os.statvfs) -> JsonObj:
    """Replay queued mirrors oldest first until the queue is empty, one fails, or ``deadline``
    (time.monotonic()) passes. Never raises for a single entry; returns what happened."""
    import fcntl  # noqa: PLC0415 - Linux only, like the rest of the host tooling

    folder = retry_dir(run_dir)
    out: JsonObj = {"queue": str(folder), "entries": [], "done": [], "kept": [], "locked": False}
    try:
        names = sorted(p.name for p in folder.glob("*.json"))
    except OSError:
        names = []
    out["entries"] = names
    if dry_run or not names:
        return out
    problem = interpreter_problem()
    if problem:  # a replay here would only fail again and burn an attempt
        out["skipped"] = problem
        return out
    lock = os.open(folder / ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0), 0o600)
    try:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            out["locked"] = True  # another host-dag is draining
            return out
        for name in names:
            if deadline is not None and time.monotonic() >= deadline:
                break
            path = folder / name
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                continue
            except (OSError, ValueError) as exc:
                entry = {"base": name, "request": {}, "attempts": RETRY_MAX, "detail": f"unreadable: {exc}"}
            base, ticket = str(entry.get("base")), (entry.get("request") or {}).get("ticket")
            try:
                applied = _applied(run_dir, base) if db_path(run_dir).is_file() else []
            except sqlite3.Error:
                applied = []
            if applied:
                status, detail = "applied", f"the original write committed {len(applied)} op(s)"
            elif entry.get("request"):
                res = mirror_ticket_set(run_dir, request_load(entry["request"]), entry.get("before"),
                                        entry.get("after"), statvfs=statvfs, base=base)
                status, detail = res.status, res.detail
            else:
                status, detail = "error", str(entry.get("detail") or "no request")
            row = {"base": base, "ticket": ticket, "status": status, "detail": detail[:500],
                   "attempt": int(entry.get("attempts") or 0) + 1, "queued_at": entry.get("queued_at")}
            if status in RETRY_DONE:
                path.unlink(missing_ok=True)
                _retry_log(run_dir, {"event": "resolved", **row})
                out["done"].append(row)
                continue
            entry["attempts"], entry["last"] = row["attempt"], {"at": now_iso(), "status": status, "detail": detail[:500]}
            if row["attempt"] >= RETRY_MAX:
                (folder / "failed").mkdir(exist_ok=True, mode=0o700)
                _private_write(folder / "failed" / name, entry)
                path.unlink(missing_ok=True)
                _retry_log(run_dir, {"event": "parked", **row})
                _Mirror(run_dir, "ticket set", ticket).append(ctl.ALERTS_LOG, {
                    "at": now_iso(), "kind": "mirror_retry_parked", "source": "host-dag", "ticket": ticket,
                    "detail": f"{base}: {status}: {detail}"[:2000]})
            else:
                tmp = path.with_name("." + name + ".tmp")
                tmp.unlink(missing_ok=True)
                _private_write(tmp, entry)
                os.replace(tmp, path)
                _retry_log(run_dir, {"event": "kept", **row})
            out["kept"].append(row)
            break  # keep the order: later entries wait for this one
        return out
    finally:
        os.close(lock)


# ---- the user's own actions (review P2-4)


NOTIFY_TITLE = "host-dag 用户确认"


# 有意例外（Fable L2174 §1a）：quota/轮换提醒会把全文追加进 alerts.jsonl（hostdag_json.log_desktop_notice），
# 这里的验证码弹窗保持 notify-send 直调：验证码与弹窗正文绝不落日志/Telegram/stdout；
# _log_confirm 只记录尝试的 action 与结果（不含验证码，见 confirm_as_user）。
def notify_desktop(title: str, body: str) -> str | None:
    """Show a critical desktop notification (notify-send). None on success, else why it failed. Never
    Telegram: the TG bridge forwards replies to the coordinator, which must not learn the code."""
    try:
        proc = subprocess.run(["notify-send", "-u", "critical", "-a", "host-dag", title, body],
                              capture_output=True, text=True, timeout=10, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return f"notify-send: {type(exc).__name__}: {exc}"
    if proc.returncode != 0:
        return f"notify-send exited {proc.returncode}: {(proc.stderr or proc.stdout).strip()[:300]}"
    return None


def _log_confirm(run_dir: Path | None, what: str, outcome: str) -> None:
    """Record the attempt (action, outcome; never the code) in sm/alerts.jsonl when sm/ is writable."""
    if run_dir is None:
        return
    try:
        sm = sm_dir(Path(run_dir))
        if sm.is_dir() and os.access(sm, os.W_OK):
            ctl.SmFiles(sm).append(ctl.ALERTS_LOG, [{"at": now_iso(), "kind": "user_confirm", "source": "host-dag",
                                                     "action": what, "outcome": outcome}])
    except Exception:  # noqa: BLE001
        pass


def confirm_as_user(what: str, *, run_dir: Path | None = None, stdin: TextIO | None = None,
                    stdout: TextIO | None = None, ask: Callable[[str], str] = input,
                    notify: Callable[[str, str], str | None] | None = None) -> None:
    """Acting as the user (``--as-user``, ``authority switch``) needs a person (review P2-4, re-review I3):
    1. stdin and stdout are TTYs (an LLM coordinator runs commands without one);
    2. the person types ``yes``;
    3. a one-time 6-digit code is shown ONLY as a critical desktop notification (notify-send, title
       ``host-dag 用户确认``, body: the code and the action) and the person types it back. No notification
       (notify-send missing or failing) -> refused. The code never goes to Telegram, stdout or a log.
    Every attempt is logged (action and outcome, without the code) in sm/alerts.jsonl.
    Residual risk (design §15.6): an agent that can read the desktop's notifications could still read it."""
    def refuse(why: str) -> CliError:
        _log_confirm(run_dir, what, f"refused: {why}")
        return CliError(f"{what}: {why}")

    sin, sout = stdin or sys.stdin, stdout or sys.stdout
    try:
        interactive = bool(sin.isatty() and sout.isatty())
    except (AttributeError, ValueError, OSError):
        interactive = False
    if not interactive:
        raise refuse("acts as the user: it needs an interactive terminal (stdin and stdout a TTY), a typed yes "
                     "and the code from the desktop notification; refusing")
    try:
        if ask(f"{what}: type yes to confirm you are the user: ").strip() != "yes":
            raise refuse("not confirmed")
        code = f"{secrets.randbelow(10**6):06d}"
        failed = (notify or notify_desktop)(NOTIFY_TITLE, f"确认码 {code}\n操作：{what}")
        if failed:
            raise refuse(f"no desktop notification, so no proof of a person at this desktop ({failed})")
        typed = ask(f"{what}: type the 6-digit code from the desktop notification \"{NOTIFY_TITLE}\": ")
    except EOFError as exc:
        raise refuse("no confirmation") from exc
    if not secrets.compare_digest(typed.strip(), code):
        raise refuse("wrong code")
    _log_confirm(run_dir, what, "confirmed")


# ---------------------------------------------------------------- core-only commands


def check_core(run_dir: Path, command: str) -> Path:
    """The core can serve this command here: state.db exists and this is the core interpreter."""
    db = db_path(run_dir)
    if not db.is_file():
        raise CliError(f"{command} needs the core, and {db} does not exist (the run is not seeded, stage B7)")
    problem = interpreter_problem()
    if problem:
        raise CliError(f"{command} needs the core interpreter: {problem}. Run it as `{c.SM_PYTHON} "
                       f"{shlex.quote(sys.argv[0])} ...`")
    return db


def open_store(run_dir: Path, command: str) -> c.SqliteStore:
    """One connection for one command (§14.1). Refuses with a clear message instead of guessing."""
    db = check_core(run_dir, command)
    try:
        return c.SqliteStore.open(db, run_id_for(run_dir))
    except c.QuiescedError as exc:
        raise CliError(f"{command}: the core is quiesced ({exc}); `host-dag sm resume` first") from exc


def submit(run_dir: Path, command: str, ops: Sequence[c.Op] | Callable[[c.World], Sequence[c.Op]]) -> JsonObj:
    store = open_store(run_dir, command)
    try:
        receipts = store.submit_batch(ops)
        mode = store.meta().get("mode", "")
    finally:
        store.close()
    out: JsonObj = {"command": command, "mode": mode, "receipts": _receipts(receipts)}
    if mode == "shadow":
        out["note"] = "shadow: written to the core only; run.json (the board's authority) is unchanged"
    return out


def op(kind: str, ticket: str | None, payload: JsonObj, *, actor: str = "coordinator",
       expected_revision: int | None = None, at: str | None = None) -> c.Op:
    body: JsonObj = {k: v for k, v in payload.items() if v is not None}
    return c.Op(tr.new_base(), kind, actor, at or now_iso(), ticket, expected_revision, None, body)


def quota_receipt(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    data = _read_json(Path(path).expanduser())
    if not isinstance(data, dict) or data.get("status") != "OK":
        raise CliError(f"--quota-receipt {path}: not an OK `host-dag-quota.py select` result")
    return {k: str(data[k]) for k in ("provider", "model", "effort") if isinstance(data.get(k), str)}


def exec_request(run_dir: Path, ticket: str, *, role: str, mode: str, provider: str | None, model: str | None,
                 effort: str | None, contract: str | None, session: str | None, resume_of: str | None,
                 axis: str | None, override_reason: str | None, receipt: str | None) -> JsonObj:
    picked = quota_receipt(receipt)
    payload: JsonObj = {"role": role, "mode": mode if role == "executor" else ("adopt" if mode == "adopt" else "review"),
                        "provider": provider or picked.get("provider"), "model": model or picked.get("model"),
                        "effort": effort or picked.get("effort"), "contract": contract, "session": session,
                        "resume_of": resume_of, "axis": axis, "override_reason": override_reason}
    return submit(run_dir, "exec request", [op("execution.request", ticket, payload)])


def question_ask(run_dir: Path, ticket: str, *, to: str, key: str, title: str, question_file: str | None) -> JsonObj:
    return submit(run_dir, "question ask", [op("question.ask", ticket, {"to": to, "key": key, "title": title,
                                                                         "question_file": question_file})])


def question_link(run_dir: Path, ticket: str, key: str, *, title: str | None,
                  table: r.AliasTable | None = None) -> JsonObj:
    """Register an advisor question that already exists in advisor/log.jsonl (§8: `--wait-on decision:K`
    is replaced by this). The key resolves exactly or through the alias table (never by suffix); the
    question keeps its original ask time so the resolver can bind the answer."""
    log = Path(run_dir) / r.ADVISOR_LOG_REL
    try:
        text = log.read_text(encoding="utf-8")
    except OSError as exc:
        raise CliError(f"question link: cannot read {log}: {exc}") from exc
    rows: list[JsonObj] = []
    for raw in text.splitlines():
        try:
            rec = json.loads(raw)
        except ValueError:
            continue
        if isinstance(rec, dict) and isinstance(rec.get("key"), str):
            rows.append(rec)
    keys, how = r.resolve_key(key, [str(x["key"]) for x in rows], table if table is not None else r.AliasTable.load())
    if how in ("none", "ambiguous") or len(keys) != 1:
        raise CliError(f"question link: advisor key {key!r} resolves {how} ({keys}); name the exact key")
    first = next(x for x in rows if x["key"] == keys[0])
    started = first.get("at")
    asked_at = datetime.fromtimestamp(float(started)).astimezone().isoformat(timespec="seconds") \
        if isinstance(started, (int, float)) else None
    qfile = first.get("question_file") if isinstance(first.get("question_file"), str) else None
    payload: JsonObj = {"to": "advisor", "key": keys[0], "title": title or str(first.get("title") or keys[0]),
                        "question_file": qfile, "asked_at": asked_at}
    out = submit(run_dir, "question link", [op("question.ask", ticket, payload)])
    latest = next(x for x in reversed(rows) if x["key"] == keys[0])
    out["resolved"] = {"key": keys[0], "how": how, "answered": is_final_answer(latest)}
    return out


def decide(run_dir: Path, ticket: str, *, response: str, expected_revision: int, action_id: str | None,
           question_id: str | None, reason: str | None, verdict: str | None, actor: str) -> JsonObj:
    if not action_id and not question_id:
        raise CliError("decide needs --action-id (or --question-id for a user question)")
    if actor == "user":
        check_core(run_dir, "decide")
        confirm_as_user("decide --as-user", run_dir=run_dir)
    payload: JsonObj = {"response": response, "action_id": action_id, "question_id": question_id,
                        "reason": reason, "verdict": verdict}
    return submit(run_dir, "decide", [op("decision.respond", ticket, payload, actor=actor,
                                         expected_revision=expected_revision)])


def decisions_pending(run_dir: Path, ticket: str | None = None) -> list[JsonObj]:
    store = open_store(run_dir, "decisions --pending")
    try:
        tickets = store.tickets()
    finally:
        store.close()
    out: list[JsonObj] = []
    for name in sorted(tickets):
        if ticket and name != ticket:
            continue
        t = tickets[name]
        for p in t.outstanding():
            out.append({"ticket": name, "batch": t.batch, "revision": t.revision, "label": c.label(t, tickets),
                        "action_id": p.action_id, "kind": p.kind, "status": p.status, "created_at": p.created_at,
                        "allowed_actions": list(p.allowed_actions), "bound": dict(p.bound),
                        "nudges": len(p.nudges), "detail": p.detail,
                        "payload": json.loads(json.dumps(p.payload))})
    out.sort(key=lambda x: (str(x["created_at"]), str(x["action_id"])))
    return out


def wait_add(run_dir: Path, ticket: str, *, kind: str, ref: str, note: str | None, wait_id: str | None) -> JsonObj:
    return submit(run_dir, "wait add", [op("wait.add", ticket, {"kind": kind, "ref": ref, "note": note,
                                                                 "wait_id": wait_id})])


def wait_update(run_dir: Path, ticket: str, *, wait_id: str, ref: str, reason: str, note: str | None) -> JsonObj:
    return submit(run_dir, "wait update", [op("wait.update", ticket, {"wait_id": wait_id, "ref": ref,
                                                                       "reason": reason, "note": note})])


def wait_waive(run_dir: Path, ticket: str, *, wait_id: str, reason: str, actor: str) -> JsonObj:
    if actor == "user":
        check_core(run_dir, "wait waive")
        confirm_as_user("wait waive --as-user", run_dir=run_dir)
    return submit(run_dir, "wait waive", [op("wait.waive", ticket, {"wait_id": wait_id, "reason": reason},
                                             actor=actor)])


def check_spec(spec: str, timeout: float = 300.0) -> tuple[bool, str]:
    """`page-window.py check` (offline: no locks, no cluster writes), as the stage A watcher runs it."""
    override = os.environ.get(ENV_CHECK_CMD)
    argv = [*shlex.split(override), spec] if override else [PAGE_WINDOW_PYTHON, str(PAGE_WINDOW_CLI), "check", spec]
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    return proc.returncode == 0, (proc.stdout or proc.stderr)[-1500:]


def pagewindow_enqueue(run_dir: Path, ticket: str, *, spec: str, priority: int | None, needs_guest: bool,
                       est_seconds: int | None, check: Callable[[str], tuple[bool, str]] = check_spec) -> JsonObj:
    """§5: only an approved spec enters the queue: grant written in the spec, offline check passed,
    and its content digest pinned (spec_digest, required). Anything missing becomes a prepare_window
    decision in the core instead."""
    path = public_path(spec)
    data = _read_json(path)
    if not isinstance(data, dict):
        raise CliError(f"pagewindow enqueue: cannot read spec {path}")
    digest = spec_digest(str(path))
    if digest is None:
        raise CliError(f"pagewindow enqueue: cannot hash {path}")
    grant = data.get("grant")
    granted = isinstance(grant, dict) and grant.get("allowRealWindow") is True
    ok, report = check(str(path)) if granted else (False, "not checked: no grant")
    payload: JsonObj = {"spec": str(path), "spec_digest": digest, "grant": granted, "checked": ok,
                        "priority": priority, "needs_guest": needs_guest or None, "est_seconds": est_seconds}
    out = submit(run_dir, "pagewindow enqueue", [op("pagewindow.enqueue", ticket, payload)])
    out["spec_digest"] = digest
    out["check"] = {"ok": ok, "report": report}
    return out


def pagewindow_policy(run_dir: Path, ticket: str, *, priority: int, reason: str, entry_id: str | None) -> JsonObj:
    return submit(run_dir, "pagewindow policy", [op("pagewindow.policy", ticket, {"priority": priority,
                                                                                   "reason": reason,
                                                                                   "entry_id": entry_id})])


def switch_gate(run_dir: Path, at: str, hours: float = 24.0) -> JsonObj:
    rows = ctl.SmFiles(sm_dir(run_dir)).jsonl(ctl.DIFF_LOG)
    return ctl.diff_report(rows, at, hours)


def authority_switch(run_dir: Path, *, mode: str, epoch: int | None, force: bool, yes: bool,
                     reason: str) -> JsonObj:
    """§8 stage C. To core only with --force AND a passing 24 h zero_diff gate from the B3 shadow-diff
    stream; any switch needs --yes. Actor user."""
    if mode not in c.MODES:
        raise CliError(f"mode must be one of {c.MODES}")
    at = now_iso()
    gate: JsonObj | None = None
    if mode == "core":
        gate = switch_gate(run_dir, at)
        if not force:
            raise CliError("authority switch to core needs --force (and a passing 24 h zero-diff gate): "
                           + json.dumps(gate, ensure_ascii=False))
        if gate.get("zero_diff") is not True:
            raise CliError("authority switch to core refused: the 24 h shadow-diff gate does not pass: "
                           + json.dumps(gate, ensure_ascii=False))
    if not yes:
        raise CliError(f"authority switch to {mode} changes who owns ticket state; pass --yes")
    check_core(run_dir, "authority switch")
    confirm_as_user(f"authority switch --mode {mode}", run_dir=run_dir)  # every switch is the user's (review P2-4)
    store = open_store(run_dir, "authority switch")
    try:
        current = int(store.meta().get("authority_epoch") or 0)
    finally:
        store.close()
    new_epoch = epoch if epoch is not None else (current + 1 if mode == "core" else current)
    out = submit(run_dir, "authority switch", [op("authority.switch", None, {"mode": mode, "epoch": new_epoch,
                                                                             "reason": reason}, actor="user")])
    if gate is not None:
        out["gate"] = gate
    return out


def sm_config(run_dir: Path) -> ctl.Config:
    return ctl.Config(Path(run_dir), run_id_for(Path(run_dir)))


def sm_quiesce(run_dir: Path, *, reason: str, timeout: float) -> JsonObj:
    problem = interpreter_problem()
    if problem:
        raise CliError(f"sm quiesce needs the core interpreter: {problem}")
    try:
        return ctl.quiesce(sm_config(run_dir), now_iso(), reason=reason, timeout=timeout)
    except ctl.ControllerError as exc:
        raise CliError(f"sm quiesce: {exc}") from exc


def sm_resume(run_dir: Path) -> JsonObj:
    problem = interpreter_problem()
    if problem:
        raise CliError(f"sm resume needs the core interpreter: {problem}")
    try:
        return ctl.resume(sm_config(run_dir), now_iso())
    except ctl.ControllerError as exc:
        raise CliError(f"sm resume: {exc}") from exc


def sm_status(run_dir: Path) -> JsonObj:
    """Read-only: no database connection (works on any interpreter)."""
    files = ctl.SmFiles(sm_dir(run_dir))
    heartbeat = files.read_json(ctl.HEARTBEAT)
    marker = sm_dir(run_dir) / c.QUIESCE_MARKER
    mirror = files.jsonl(MIRROR_LOG)
    counts: dict[str, int] = {}
    for row in mirror[-200:]:
        counts[str(row.get("status"))] = counts.get(str(row.get("status")), 0) + 1
    return {"db": str(db_path(run_dir)), "exists": db_path(run_dir).is_file(), "quiesced": marker.exists(),
            "quiesce": files.read_json(c.QUIESCE_MARKER), "heartbeat": heartbeat,
            "interpreter": {"executable": sys.executable, "pinned": pinned(), "problem": interpreter_problem()},
            "mirror_last_200": dict(sorted(counts.items()))}


# ---- one-shot reconciliation of closures run.json has and the core lost (core-mirror)
#
# In shadow mode run.json is the authority. A `ticket set --state ACCEPTED|INTEGRATED` whose mirror
# timed out, or was refused because the core never saw the candidate (T19), leaves the core ticket open,
# and every dependent is then refused by _ticket_close ("cannot be accepted before its prerequisites").
# `host-dag sm reconcile` lists those tickets (dry run, read-only connection) and with --apply backfills
# them in ONE transaction as ``ticket.close`` + ``legacy_reconcile`` by the ``seed`` actor. Only
# accepted/integrated is backfilled; a core closure of another kind is reported, never overwritten.
# Op ids derive from the run.json digest and the ticket, so re-applying the same run.json is a replay.

RECONCILE_LOG = "reconcile.jsonl"
RECONCILE_REASON = "mirror lost or refused the legacy acceptance"


def legacy_acceptance(rec: Mapping[str, object]) -> str | None:
    """"ACCEPTED" / "INTEGRATED" when run.json records this ticket as done, else None. A written
    ``state`` decides; without one, ``status`` ACCEPTED does (as hostdag_ticket_state.derive_state)."""
    state = str(rec.get("state") or "").strip().upper()
    if state:
        return state if state in ("ACCEPTED", "INTEGRATED") else None
    return "ACCEPTED" if str(rec.get("status") or "").strip().upper() in ("ACCEPTED", "DONE", "CLOSED") else None


def _read_world_ro(db: Path) -> c.World:
    """The core records over a read-only connection (dry run: nothing written, not even WAL setup)."""
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, isolation_level=None, timeout=5)
    try:
        store = c.SqliteStore(conn)
        return store.world()
    finally:
        conn.close()


def _unmet(world: c.World) -> dict[str, list[str]]:
    return {n: unmet for n, t in sorted(world.tickets.items()) if t.phase != c.CLOSED
            for unmet in [[d for d in t.deps if not c.accepted(world.tickets.get(d))]] if unmet}


def reconcile_closures(run_dir: Path, *, apply: bool = False, tickets: Sequence[str] = (),
                       reason: str = RECONCILE_REASON, at: str | None = None) -> JsonObj:
    """Compare run.json acceptances with the core and (``apply``) backfill the missing ones. Returns
    the report: what would change / changed, what is skipped and why, and the open tickets whose
    prerequisites are unmet in the core before and after (the check _ticket_close makes)."""
    run_dir = Path(run_dir)
    db = check_core(run_dir, "sm reconcile")
    raw = (run_dir / "run.json").read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    run = json.loads(raw)
    legacy: dict[str, JsonObj] = {}
    for body in (run.get("batches") or {}).values():
        for name, rec in ((body or {}).get("tickets") or {}).items():
            if isinstance(rec, dict) and (not tickets or name in tickets):
                legacy[str(name)] = rec
    stamp = at or now_iso()

    def classify(world: c.World) -> tuple[list[JsonObj], list[JsonObj]]:
        todo, skipped = [], []
        for name, rec in sorted(legacy.items()):
            kind = legacy_acceptance(rec)
            if kind is None:
                continue
            t = world.tickets.get(name)
            row: JsonObj = {"ticket": name, "legacy_state": rec.get("state"), "legacy_status": rec.get("status")}
            if t is None:
                skipped.append({**row, "why": "not in the core (never created there)"})
            elif t.closure is not None and t.closure.kind == "accepted":
                continue  # already consistent
            elif t.closure is not None:
                skipped.append({**row, "why": f"core closed it {t.closure.kind}; not overwritten"})
            else:
                todo.append({**row, "core_phase": t.phase, "core_revision": t.revision,
                             "integrated": kind == "INTEGRATED"})
        # prerequisites first, so the report reads in acceptance order
        names = {r["ticket"] for r in todo}
        order: list[JsonObj] = []
        pending = list(todo)
        while pending:
            ready = [r for r in pending if not any(d in names and d not in {o["ticket"] for o in order}
                                                   for d in world.tickets[str(r["ticket"])].deps)] or pending[:1]
            order.append(ready[0])
            pending.remove(ready[0])
        return order, skipped

    def ops_for(rows: Sequence[JsonObj]) -> list[c.Op]:
        out = []
        for r_ in rows:
            name = str(r_["ticket"])
            rec = legacy[name]
            body: JsonObj = {"state": legacy_acceptance(rec), "status": rec.get("status"), "run_json_sha256": digest,
                             "accept_scope": rec.get("accept_scope"), "accepted_tip": rec.get("accepted_tip")}
            payload: JsonObj = {"kind": "accepted", "reason": reason,
                                "legacy_reconcile": {k: v for k, v in body.items() if v is not None}}
            out.append(c.Op(f"reconcile-{digest[:16]}-{name}", "ticket.close", "seed", stamp, name, None, None,
                            payload))
        return out

    before = _read_world_ro(db)
    todo, skipped = classify(before)
    report: JsonObj = {"run_json": str(run_dir / "run.json"), "run_json_sha256": digest, "db": str(db),
                       "mode": "apply" if apply else "dry-run", "would_close": todo, "skipped": skipped,
                       "unmet_prerequisites_before": _unmet(before)}
    if before.meta.mode != "shadow":
        raise CliError(f"sm reconcile: core authority mode is {before.meta.mode}; run.json is not the authority")
    if not apply:
        closed = {str(r_["ticket"]) for r_ in todo}
        after_dry = {n: [d for d in deps if d not in closed] for n, deps in _unmet(before).items() if n not in closed}
        report["unmet_prerequisites_after"] = {n: deps for n, deps in after_dry.items() if deps}
        return report
    store = open_store(run_dir, "sm reconcile")
    try:
        receipts = store.submit_batch(lambda world: ops_for(classify(world)[0]))
        after = store.world()
    finally:
        store.close()
    report["receipts"] = _receipts(receipts)
    report["unmet_prerequisites_after"] = _unmet(after)
    _Mirror(run_dir, "sm reconcile", None).append(RECONCILE_LOG, {
        "at": stamp, "run_json_sha256": digest, "closed": [x.ticket for x in receipts if x.status == "accepted"],
        "receipts": report["receipts"], "skipped": skipped})
    return report


def dump(data: Json | Mapping[str, Json]) -> str:
    return json.dumps(data, ensure_ascii=False, indent=1)

"""Disposable read-only UI cache. Never used by writers or admission decisions."""
import copy
import hashlib
import json
import os
from pathlib import Path
import threading
import time

from hostdag_json import atomic_text

# A stale cache only earns the refresh-lag label after this many seconds without a successful
# read (user 11-10: the old label read "磁盘忙" even when the disk had merely paused).
FRESH_SECONDS = 3.0
MAX_SAVED_BYTES = 4 << 20  # the saved snapshot is a startup convenience, never a big payload


def cache_path(run: Path, view: str) -> Path:
    # No resolve/stat of a potentially stalled USB path on the input thread.
    key = hashlib.sha256(str(run.absolute()).encode()).hexdigest()[:24]
    return Path.home() / '.cache/host-dag/board' / key / (view + '.json')


def clock_text(stamp: float) -> str:
    return time.strftime('%H:%M:%S', time.localtime(stamp)) if stamp else '无'


class Snapshot:
    """Bounded read cache for the UI: one worker, never blocking the input thread.

    ``status`` names a delay only once the last successful read (a new version, or a read that
    confirmed the source was unchanged) is more than FRESH_SECONDS old; read/parse errors show
    at once. That check is what the board paints above its first line.
    """

    def __init__(self, path: Path, *, clock=time.time):
        self.path = path
        self.clock = clock
        self.value = None
        self.updated_at = 0.0
        self.fresh_at = 0.0  # last successful read in this session; 0 = never
        self.error = ''
        self.worker = None
        self.status = '正在读取'
        try:
            # This runs on the input thread: a huge saved payload is not worth the stall.
            if path.stat().st_size > MAX_SAVED_BYTES:
                raise OSError('saved snapshot too large')
            saved = json.loads(path.read_text())
            self.value, self.updated_at = saved['value'], saved['updated_at']
            self.status = '缓存 · ' + clock_text(self.updated_at)
        except (OSError, ValueError, KeyError):
            pass

    def get(self, loader, *, budget=.15, store=True):
        if self.worker is None or not self.worker.is_alive():
            def load():
                try:
                    value = loader()
                    stamp = self.clock()
                    if store:  # a huge payload could not be read back either: do not rewrite it every refresh
                        text = json.dumps({'updated_at': stamp, 'value': value}, ensure_ascii=False, indent=2) + '\n'
                        if len(text.encode('utf-8')) <= MAX_SAVED_BYTES:
                            self.path.parent.mkdir(parents=True, exist_ok=True)
                            atomic_text(self.path, text)
                    self.value, self.updated_at = value, stamp
                    self.fresh_at = stamp
                    self.error, self.status = '', ''
                except Exception as exc:
                    self.error = self.status = '读取失败：' + type(exc).__name__
            self.worker = threading.Thread(target=load, name='board-read', daemon=True)
            self.worker.start()
        self.worker.join(budget)
        if self.worker.is_alive():
            if self.error:
                self.status = self.error  # an error is shown as soon as it happens
            elif not self.fresh_at or self.clock() - self.fresh_at > FRESH_SECONDS:
                self.status = '刷新延迟 · 上次成功刷新 ' + clock_text(self.fresh_at)
            else:
                self.status = ''
        return self.value


class JsonFile:
    """JSON reads with per-stage timings; an unchanged identity, mtime_ns and size reuses the parse.

    Read-only and never shared with writers: the caller gets a deep copy, because the board
    marks the loaded structure (normalized executors, live/wait markers) in place.
    """

    def __init__(self):
        self._entries: dict[str, tuple[tuple[int, int, int, int], object]] = {}
        self.timings = {'stat': 0.0, 'read': 0.0, 'parse': 0.0}
        self.reused = 0

    def reset_timings(self) -> None:
        self.timings = {'stat': 0.0, 'read': 0.0, 'parse': 0.0}

    def _add(self, stages: dict[str, float]) -> None:
        """A refresh loads run.json and watch.json: the timing line shows their sum, not the last file."""
        for stage, seconds in stages.items():
            self.timings[stage] = self.timings.get(stage, 0.0) + seconds

    def load(self, path: Path):
        started = time.perf_counter()
        stat = os.stat(path)
        stat_done = time.perf_counter()
        # The identity catches an atomic replace that lands in the same ns tick at the same size.
        key = (stat.st_dev, stat.st_ino, stat.st_mtime_ns, stat.st_size)
        previous = self._entries.get(str(path))
        if previous is not None and previous[0] == key:
            # Reuse the parse; the copy that keeps the caller's marks off it is the remaining cost.
            value = copy.deepcopy(previous[1])
            self.reused += 1
            self._add({'stat': stat_done - started, 'read': 0.0,
                       'parse': time.perf_counter() - stat_done})
            return value
        raw = path.read_bytes()
        read_done = time.perf_counter()
        try:
            value = json.loads(raw)
        finally:
            self._add({'stat': stat_done - started, 'read': read_done - stat_done,
                       'parse': time.perf_counter() - read_done})
        if isinstance(value, dict):
            self._entries[str(path)] = (key, value)
        return copy.deepcopy(value)


def tail_lines(path: Path, *, limit=256 << 10):
    """Bounded recent-log read, including a partial final line for display only."""
    with path.open('rb') as stream:
        size = stream.seek(0, 2)
        start = max(0, size - limit)
        stream.seek(start)
        raw = stream.read(limit)
    if start:
        raw = raw.partition(b'\n')[2]
    return raw.decode('utf-8', 'replace').splitlines()


_jsonl_cache = {}
_jsonl_lock = threading.Lock()


def jsonl_rows(path: Path):
    """Incremental display index: keep history semantics, parse only appended bytes.

    Truncate/replace/same-size rewrite invalidates the index. An unfinished final
    line is held until its newline arrives. This is never a source for mutations.
    """
    with _jsonl_lock, path.open('rb') as stream:
        stat = os.fstat(stream.fileno())
        identity = (stat.st_dev, stat.st_ino)
        state = _jsonl_cache.get(str(path))
        # Growth is not proof of append. Validate EVERY previously consumed byte
        # [0, offset), including an unfinished final line, in bounded chunks.
        # Continue the validated hasher with new bytes so the saved digest always
        # describes exactly the bytes used to build rows/partial, without a
        # second read that could hash a concurrently rewritten version instead.
        digest = hashlib.sha256()
        unchanged = (state is not None and state['identity'] == identity and
                     stat.st_size >= state['offset'] and not
                     (stat.st_size == state['offset'] and stat.st_mtime_ns != state['mtime']))
        if unchanged:
            remaining = state['offset']
            while remaining:
                data = stream.read(min(256 << 10, remaining))
                if not data:
                    break
                digest.update(data)
                remaining -= len(data)
            unchanged = remaining == 0 and digest.digest() == state['prefix_hash']
        if not unchanged:
            state = {'identity': identity, 'offset': 0, 'partial': b'', 'rows': [], 'mtime': 0}
            digest = hashlib.sha256()
        stream.seek(state['offset'])
        while state['offset'] < stat.st_size:
            data = stream.read(min(256 << 10, stat.st_size - state['offset']))
            if not data:
                break
            digest.update(data)
            state['offset'] += len(data)
            lines = (state['partial'] + data).split(b'\n')
            state['partial'] = lines.pop()
            for raw in lines:
                try:
                    row = json.loads(raw.decode('utf-8', 'replace'))
                    if isinstance(row, dict):
                        state['rows'].append(row)
                except (ValueError, UnicodeDecodeError):
                    pass
        state['mtime'] = stat.st_mtime_ns
        state['prefix_hash'] = digest.digest()
        _jsonl_cache[str(path)] = state
        rows = list(state['rows'])
        # Legacy ledgers may end in a complete JSON object without a newline.
        try:
            row = json.loads(state['partial'].decode('utf-8', 'replace'))
            if isinstance(row, dict):
                rows.append(row)
        except (ValueError, UnicodeDecodeError):
            pass
        return rows

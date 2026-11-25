"""Per-job SSD scratch and Linux I/O priority for foreground worker commands."""
import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import re
import shutil
import tempfile


@contextmanager
def job_environment(job, base=None):
    env = dict(os.environ if base is None else base)
    cache = Path.home() / '.cache'
    job = re.sub(r'[^A-Za-z0-9_.-]', '-', str(job))[:100] or 'job'
    go_root, tmp_root = cache / 'go-tmp' / job, cache / 'host-dag/tmp'
    go_root.mkdir(parents=True, exist_ok=True)
    tmp_root.mkdir(parents=True, exist_ok=True)
    (cache / 'jest').mkdir(parents=True, exist_ok=True)
    go = Path(tempfile.mkdtemp(prefix='run-', dir=go_root))
    tmp = Path(tempfile.mkdtemp(prefix=job + '-', dir=tmp_root))
    try:
        env.update(GOTMPDIR=str(go), TMPDIR=str(tmp), JEST_CACHE_DIRECTORY=str(cache / 'jest'))
        yield env
    finally:
        shutil.rmtree(go)
        shutil.rmtree(tmp)
        try:
            go_root.rmdir()  # Only empty; concurrent jobs keep their own scratch.
        except OSError:
            pass


def jest_command(argv, env):
    argv = list(argv)
    direct = any(Path(x).name in {'jest', 'jest.js'} for x in argv[:3])
    if direct:
        # Reject a second cache location rather than letting /tmp override SSD.
        for i in range(len(argv) - 1, -1, -1):
            if argv[i].startswith('--cacheDirectory='):
                del argv[i]
            elif argv[i] == '--cacheDirectory':
                del argv[i:i + 2]
        argv += ['--cacheDirectory', env['JEST_CACHE_DIRECTORY']]
    return argv


def io_command(argv, *, heavy=False):
    return (['ionice', '-c2', '-n4'] if heavy else ['ionice', '-c3']) + ['nice', '-n', '19', *argv]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job', required=True)
    parser.add_argument('--io', action='store_true', help='idle priority for bypass I/O; heavy holders use resource-lock runner')
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command:
        parser.error('command required after --')
    # Reuse the lock runner process-group cleanup for SIGTERM/SIGINT as well.
    from hostdag_resource_locks import run_command, termination_cleanup, Terminated
    try:
        with termination_cleanup():
            return run_command(command, job=args.job, io=args.io)
    except Terminated as exc:
        return 128 + exc.signum


if __name__ == '__main__':
    raise SystemExit(main())

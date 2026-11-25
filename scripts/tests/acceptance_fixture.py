"""Fixture-run helper: a Fable candidate row that attributes a delivery acceptance (TOOL-LINT-POST-HOOK-ACCEPTANCES).

A new delivery acceptance must carry candidate_decision_ref to a final model=fable advisor row that names the
ticket, says candidate and lists every SHA (release_coverage.attribution_valid). Tests that accept a delivery
ticket append such a row to their temporary run, never to a real one.
"""
import json
from pathlib import Path
import subprocess


def local_candidate(run_dir):
    """A real unrelated source commit for tests of other acceptance guards."""
    root = Path(run_dir)
    repo = root / 'fixture-source'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args],
                                       stderr=subprocess.DEVNULL, text=True).strip()
    git('init', '-q')
    git('config', 'user.name', 'fixture')
    git('config', 'user.email', 'fixture@example.invalid')
    git('remote', 'add', 'origin', 'https://example.com/org/demo-repo.git')
    (repo / 'README.fixture').write_text('Unrelated source; no adjunct payload changes.\n')
    git('add', 'README.fixture')
    git('commit', '-qm', 'fixture source')
    sha = git('rev-parse', 'HEAD')
    (root / 'control').mkdir(exist_ok=True)
    (root / 'control/release-heads.json').write_text(json.dumps({'GC': dict(repo=str(repo), sha=sha)}))
    return 'GC:' + sha


def fable_candidate_ref(run_dir, tickets, candidate):
    """Append one decided Fable row to <run_dir>/advisor/log.jsonl; return its run-relative ref."""
    log = Path(run_dir) / "advisor/log.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    tickets = [tickets] if isinstance(tickets, str) else list(tickets)
    row = dict(verdict="decided", model="fable", provisional=False, tickets=tickets, candidate=candidate,
               decision="candidate attribution for " + ", ".join(tickets) + ": " + candidate)
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    return "advisor/log.jsonl#L%d" % len(log.read_text(encoding="utf-8").splitlines())

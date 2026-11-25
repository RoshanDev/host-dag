import os
"""Regression metadata policy. Acceptance guard is called only by board ticket set."""
import json
import re
from pathlib import Path

DEFECT_CLASSES = frozenset("oci-media-type image-missing-file sample-config cli-argv remote-stdout api-request resource-order image-owner env-residue precheck-threshold".split())
SLUG = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*\Z")
CRASH = re.compile(r"Traceback \(most recent call last\)|FileNotFoundError|No such file or directory|command not found|ModuleNotFoundError|Segmentation fault", re.I)


def all_rows(run):
    return {tid: row for batch in (run.get("batches") or {}).values()
            for tid, row in (batch.get("tickets") or {}).items()}


def is_release_fix(tid, row):
    chain = str(row.get("chain_id") or row.get("chain") or "")
    return bool(re.search(os.environ.get("HOST_DAG_RELEASE_FIX_CHAIN_REGEX") or r"(?!x)x", chain, re.I)
                or re.match(os.environ.get("HOST_DAG_RELEASE_FIX_TICKET_REGEX") or r"(?!x)x", tid))


def recurrence_findings(tickets):
    findings = []
    for tid, row in tickets.items():
        slug = row.get("defect_class")
        peers = sorted(k for k, t in tickets.items() if k != tid and slug and t.get("defect_class") == slug)
        regression = row.get("regression") or {}
        if peers and (not isinstance(regression, dict) or not str(regression.get("why_existing_check_missed") or "").strip()):
            findings.append(dict(ticket=tid, rule="fix-regression-recurrence", owner="parent",
                                 severity="info", blocking=False,
                                 evidence=dict(defect_class=slug, peers=peers,
                                               nudge="补 regression.why_existing_check_missed")))
    return findings


def validate_update(run, tid, before, fields, root):
    row = {**before, **{k: v for k, v in fields.items() if v is not None}}
    slug = row.get("defect_class")
    if fields.get("defect_class") is not None and slug:
        extra_path = Path(root) / "control/defect-classes.json"
        extra = json.loads(extra_path.read_text()) if extra_path.is_file() else {}
        registered = isinstance(extra, dict) and isinstance(extra.get(slug), dict) and str(extra[slug].get("decision_ref") or "").strip()
        if not isinstance(slug, str) or len(slug) > 64 or not SLUG.fullmatch(slug) or (slug not in DEFECT_CLASSES and not registered):
            raise ValueError("REGRESSION_DEFECT_CLASS: use a registered short slug")
    if fields.get("regression") is not None and not isinstance(fields["regression"], dict):
        raise ValueError("REGRESSION_OBJECT: regression must be an object")
    # Field-only backfills on historical ACCEPTED rows are deliberately allowed. The board upper-cases
    # --state, so `accepted` is ACCEPTED here too; a first acceptance straight to INTEGRATED is one as well.
    import hostdag_ticket_state as ts
    target = str(fields.get("state") or "").upper()
    if not (target == ts.ACCEPTED or fields.get("status") == "ACCEPTED"
            or (target == ts.INTEGRATED and ts.derive_state(before or {"deps": []}) not in ts.DONE)):
        return
    if not slug and not is_release_fix(tid, row):
        return
    reg = row.get("regression")
    if not isinstance(reg, dict) or not reg:
        raise ValueError("REGRESSION_REQUIRED: acceptance needs regression")
    runtime = row.get("kind") == "runtime-only" or reg.get("kind") == "runtime-only"
    def required(key):
        value = reg.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("REGRESSION_REQUIRED: " + key)
        return value
    if runtime:
        required("why_no_static_check")
    if not runtime or reg.get("check_cmd"):
        required("check_cmd")
    logs = {}
    for key in ("red_ref", "green_ref"):
        path = Path(required(key))
        if not path.is_absolute():
            path = Path(root) / path
        if not path.is_file():
            raise ValueError("REGRESSION_FILE: " + key + " must name an existing file")
        try:
            logs[key] = path.read_text()
        except (OSError, UnicodeError) as exc:
            raise ValueError("REGRESSION_FILE: unreadable " + key) from exc
    def bound(line):
        normalized = line.lower().replace("_", "-")
        return any(str(key).lower() in normalized for key in (slug, tid) if key)
    red = logs["red_ref"]
    if CRASH.search(red) or not any(re.search(r"\bFAIL(?:ED)?\b", line) and bound(line) for line in red.splitlines()):
        raise ValueError("REGRESSION_RED: need defect-bound FAIL, not checker crash/missing input")
    if runtime:
        for key, verdict in (("red_ref", "FAIL"), ("green_ref", "PASS")):
            if not any(bound(line) and re.search(r"\bruntime\b", line, re.I) and verdict in line for line in logs[key].splitlines()):
                raise ValueError("REGRESSION_RUNTIME: need defect-bound runtime " + verdict + " evidence")
    if slug and any(k != tid and t.get("defect_class") == slug for k, t in all_rows(run).items()):
        required("why_existing_check_missed")

"""Generic adjunct lock policy hook.

Project-specific repositories can override this module to enforce lockfile
integrity across secondary payload repositories. By default no adjunct locks
are configured and checks pass cleanly.
"""
from __future__ import annotations

LOCKS: dict[str, tuple[str, str]] = {}
REPOS: dict[str, dict[str, list[str]]] = {}


def validate_update(*args, **kwargs):
    return None


def check_adjunct_locks(*args, **kwargs):
    return {"ok": True, "checked": 0, "findings": []}


def verify_ticket_adjunct_locks(*args, **kwargs):
    return {"ok": True, "checked": 0, "findings": []}

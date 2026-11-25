"""Shared finality rules for advisor log consumers; missing flags retain legacy behavior."""
from __future__ import annotations

from collections.abc import Mapping
import math
from typing import Any


def validate_answer(record: Mapping[str, Any]) -> None:
    """Reject incomplete rulings before routing or publishing them."""
    if record.get("verdict") not in ("decided", "needs_user"):
        raise ValueError("advisor verdict missing or invalid; expected decided or needs_user")


def validate_record(record: Mapping[str, Any]) -> None:
    """The formal log uses numeric epoch timestamps, never ISO strings or booleans."""
    at = record.get("at")
    if type(at) not in (int, float) or not math.isfinite(at):
        raise ValueError("advisor at must be a finite numeric epoch timestamp")
    validate_answer(record)


def is_provisional_answer(record: Mapping[str, Any] | None) -> bool:
    return bool(record and (record.get("provisional") is True or record.get("event") == "l1-sample"))


def is_final_answer(record: Mapping[str, Any] | None) -> bool:
    return bool(record and record.get("verdict") and not is_provisional_answer(record))

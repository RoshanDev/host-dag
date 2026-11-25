"""Shared human-readable formatting for DAG views and notifications."""

import re


def memory_text(text: str) -> str:
    """Format memory quantities for display; retain exact units in the run receipt."""
    scales = {"KiB": 1024, "MiB": 1024**2, "GiB": 1024**3,
              "TiB": 1024**4, "kB": 1000, "KB": 1000,
              "MB": 1000**2, "GB": 1000**3, "TB": 1000**4}

    def replace(match: re.Match[str]) -> str:
        size = float(match[1]) * scales[match[2]]
        return f"{size / 1e9:.2f} GB" if size >= 1e9 else f"{size / 1e6:.2f} MB"

    return re.sub(r"(?<![\d.])(\d+(?:\.\d+)?)\s*(KiB|MiB|GiB|TiB|kB|KB|MB|GB|TB)(?![A-Za-z])",
                  replace, text)

from __future__ import annotations

import subprocess


def _git(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def git_short_sha() -> str:
    return _git("rev-parse", "--short", "HEAD") or "unknown"


def git_branch() -> str:
    return _git("rev-parse", "--abbrev-ref", "HEAD") or "unknown"

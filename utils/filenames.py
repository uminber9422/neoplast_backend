"""Safe filename helpers for uploaded files.

Prevents path traversal (`../etc/passwd`) and Windows-reserved names.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path

_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}
ALLOWED_EXTENSIONS = frozenset({".csv", ".xlsx", ".xls"})


def sanitize_filename(filename: str) -> str:
    """Return a safe, unique filename. Preserves extension. Never returns ''."""
    if not filename:
        return f"upload_{uuid.uuid4().hex[:8]}"
    # Take only the basename — drop any directory components attackers may inject.
    base = Path(filename).name
    stem, dot, ext = base.rpartition(".")
    if not dot:
        stem, ext = base, ""
    stem = _SAFE_CHARS.sub("_", stem).strip("._-") or "upload"
    if stem.upper() in _WINDOWS_RESERVED:
        stem = f"file_{stem}"
    ext = _SAFE_CHARS.sub("", ext).lower()
    suffix = f"_{uuid.uuid4().hex[:8]}"
    safe = f"{stem}{suffix}.{ext}" if ext else f"{stem}{suffix}"
    return safe[:200]


def has_allowed_extension(filename: str) -> bool:
    return Path(filename).suffix.lower() in ALLOWED_EXTENSIONS


def is_within_directory(child: Path, parent: Path) -> bool:
    """Defense-in-depth check that `child` resolves inside `parent`."""
    try:
        child.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True

"""
Retention and garbage collection utilities for generated artifacts.

Implements TTL-based cleanup and optional disk cap enforcement for the
tables viewer output directory (`backend/temp/tables`).
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Iterable, Optional, Tuple


logger = logging.getLogger(__name__)


def _dir_size_bytes(path: Path) -> int:
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except Exception:
            # Ignore files that may have been concurrently deleted
            pass
    return total


def _list_run_dirs(root: Path) -> Iterable[Tuple[Path, float, int]]:
    """
    Yield (dir_path, mtime, size_bytes) for each immediate child directory.
    """
    for child in root.iterdir():
        try:
            if child.is_dir():
                mtime = child.stat().st_mtime
                size = _dir_size_bytes(child)
                yield (child, mtime, size)
        except Exception:
            continue


def gc_tables_dir(
    tables_root: Path,
    ttl_days: int = 7,
    max_bytes: Optional[int] = 5 * 1024 * 1024 * 1024,  # 5 GiB
) -> None:
    """
    Garbage-collect the tables output directory based on TTL and size cap.

    - Deletes subdirectories older than ttl_days.
    - If max_bytes is set, deletes oldest remaining subdirectories until
      total usage is <= max_bytes.
    """
    try:
        now = time.time()
        ttl_seconds = ttl_days * 24 * 3600
        tables_root.mkdir(parents=True, exist_ok=True)

        # Pass 1: TTL cleanup
        for dir_path, mtime, _size in list(_list_run_dirs(tables_root)):
            if now - mtime > ttl_seconds:
                try:
                    shutil.rmtree(dir_path, ignore_errors=True)
                    logger.info("GC (TTL) removed %s", str(dir_path))
                except Exception:
                    logger.warning("GC (TTL) failed to remove %s", str(dir_path))

        # Pass 2: cap enforcement
        if max_bytes is not None and max_bytes > 0:
            entries = list(_list_run_dirs(tables_root))
            total_bytes = sum(sz for _p, _mt, sz in entries)
            if total_bytes > max_bytes:
                # Sort by mtime ascending (oldest first)
                entries.sort(key=lambda t: t[1])
                for dir_path, _mtime, size in entries:
                    try:
                        shutil.rmtree(dir_path, ignore_errors=True)
                        total_bytes -= size
                        logger.info("GC (cap) removed %s (%d bytes)", str(dir_path), size)
                    except Exception:
                        logger.warning("GC (cap) failed to remove %s", str(dir_path))
                    if total_bytes <= max_bytes:
                        break
    except Exception:
        logger.exception("Tables directory GC encountered an error")



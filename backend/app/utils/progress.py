"""
Lightweight in-memory progress store for tracking job status and percentage.

Intended for single-process FastAPI app usage. Not persistent across restarts.
"""

from __future__ import annotations

from typing import Dict, Optional
from dataclasses import dataclass, asdict
import time
import threading


@dataclass
class ProgressState:
    job_id: str
    task: str  # e.g., "preflight" | "validate" | "render"
    status: str  # "idle" | "running" | "success" | "error"
    percent: int
    message: str
    started_at: float
    updated_at: float


class ProgressStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._jobs: Dict[str, ProgressState] = {}

    def start(self, job_id: str, task: str, message: str = "") -> None:
        now = time.time()
        with self._lock:
            self._jobs[job_id] = ProgressState(
                job_id=job_id,
                task=task,
                status="running",
                percent=0,
                message=message,
                started_at=now,
                updated_at=now,
            )

    def update(self, job_id: str, percent: int, message: Optional[str] = None) -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if not st:
                return
            st.percent = max(0, min(100, int(percent)))
            if message is not None:
                st.message = message
            st.updated_at = time.time()

    def finish(self, job_id: str, success: bool = True, message: str = "") -> None:
        with self._lock:
            st = self._jobs.get(job_id)
            if not st:
                return
            st.status = "success" if success else "error"
            st.percent = 100 if success else max(0, min(100, st.percent))
            if message:
                st.message = message
            st.updated_at = time.time()

    def error(self, job_id: str, message: str = "") -> None:
        self.finish(job_id, success=False, message=message)

    def get(self, job_id: str) -> Optional[dict]:
        with self._lock:
            st = self._jobs.get(job_id)
            return asdict(st) if st else None

    def cleanup(self, max_age_seconds: int = 3600) -> None:
        cutoff = time.time() - max_age_seconds
        with self._lock:
            to_del = [k for k, v in self._jobs.items() if v.updated_at < cutoff]
            for k in to_del:
                del self._jobs[k]



# etl/pipeline_logger.py
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psutil
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from etl.database_client import DatabaseClient, ProcessingLog

logger = logging.getLogger(__name__)

_SAMPLE_INTERVAL_SEC = 0.5
_TRACEBACK_MAX_CHARS = 4000


def _slug_filename(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]", "_", name).strip("_")
    return slug or "dataset"


# The whole `etl` package logger, not just this module's. Stage transitions
# come from here ([PLOG]), but the tracebacks that explain a failure come from
# etl.module5_orchestrator, etl.module9_fusion, etc. Attaching to the parent
# captures both.
_ETL_LOGGER_NAME = __name__.split(".")[0]

# threading.Thread does not inherit contextvars, and two datasets can process
# concurrently (etl.dataset_manager keys its worker threads per dataset), so
# thread ident -> log path is what keeps one run's lines out of another's file.
_scope_lock = threading.Lock()
_thread_scopes: dict[int, Path] = {}
_scope_depth = 0
_saved_level: int | None = None
# One FileHandler per log path, reference-counted. Two runs can resolve to the
# same file (the path is derived from the dataset *name*, so re-creating a
# deleted dataset under the same name collides), and attaching a second
# handler to the same path made the `etl` logger write every record twice --
# the scope filter matches on path, so it cannot tell the handlers apart.
_path_handlers: dict[Path, tuple[logging.Handler, int]] = {}


class _DatasetScopeFilter(logging.Filter):
    """Pass only records emitted by threads enrolled in this dataset run."""

    def __init__(self, log_path: Path) -> None:
        super().__init__()
        self._log_path = log_path

    def filter(self, record: logging.LogRecord) -> bool:
        with _scope_lock:
            return _thread_scopes.get(record.thread) == self._log_path


def adopt_dataset_log_scope(log_path: Path) -> None:
    """Enroll the calling thread in `log_path`'s dataset run.

    Worker threads started inside a `dataset_log_file(...)` block must call
    this first; records from threads that never enroll are filtered out of the
    file (they still reach the console via propagation)."""
    with _scope_lock:
        _thread_scopes[threading.get_ident()] = log_path


def dataset_log_path(dataset_name: str, logs_dir: str | None = None) -> Path:
    """Resolve the .txt path a dataset's run log is appended to."""
    logs_dir = logs_dir or os.getenv("LOGS_DIR", "logs_pipeline")
    log_dir_path = Path(logs_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    return log_dir_path / f"{_slug_filename(dataset_name)}.txt"


@contextmanager
def dataset_log_file(dataset_name: str, logs_dir: str | None = None) -> Iterator[Path]:
    """Attach a per-dataset FileHandler to the `etl` package logger for the
    duration of a batch run, so every [PLOG] event (download progress and
    stage transitions alike) plus any module traceback is appended to one .txt
    file immediately, without waiting for the batch to finish. Removed again
    on exit.

    The `etl` logger is forced to INFO for the duration: it is NOTSET by
    default, so it inherited root's WARNING and dropped every INFO record
    before any handler could see it — which is why these files previously
    contained FAILED lines only, or nothing at all."""
    global _scope_depth, _saved_level

    log_path = dataset_log_path(dataset_name, logs_dir)

    etl_logger = logging.getLogger(_ETL_LOGGER_NAME)
    with _scope_lock:
        if _scope_depth == 0:
            _saved_level = etl_logger.level
            etl_logger.setLevel(logging.INFO)
        _scope_depth += 1
        _thread_scopes[threading.get_ident()] = log_path
        existing = _path_handlers.get(log_path)
        if existing is None:
            handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s %(message)s"
                )
            )
            handler.setLevel(logging.INFO)
            handler.addFilter(_DatasetScopeFilter(log_path))
            _path_handlers[log_path] = (handler, 1)
            etl_logger.addHandler(handler)
        else:
            handler, refs = existing
            _path_handlers[log_path] = (handler, refs + 1)
    logger.info(
        "[PLOG-FILE] run log opened for dataset=%r -> %s", dataset_name, log_path
    )
    try:
        yield log_path
    finally:
        logger.info("[PLOG-FILE] run log closed for dataset=%r", dataset_name)
        with _scope_lock:
            handler, refs = _path_handlers[log_path]
            if refs <= 1:
                del _path_handlers[log_path]
                etl_logger.removeHandler(handler)
                handler.close()
                # Thread scopes are keyed by path too, so they may only be
                # dropped once the last run using this file has finished.
                for ident, path in [
                    (i, p) for i, p in _thread_scopes.items() if p == log_path
                ]:
                    del _thread_scopes[ident]
            else:
                _path_handlers[log_path] = (handler, refs - 1)
                _thread_scopes.pop(threading.get_ident(), None)
            _scope_depth -= 1
            if _scope_depth == 0 and _saved_level is not None:
                etl_logger.setLevel(_saved_level)
                _saved_level = None


class PipelineLogManager:
    """Append-only structured pipeline log, backed by the `processing_logs`
    table. One row per stage STARTED/RUNNING/COMPLETED/FAILED event."""

    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def log_event(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        status: str,
        message: str,
        details: dict | None = None,
    ) -> int:
        try:
            with self._db.session() as sess:
                row = ProcessingLog(
                    dataset_id=dataset_id,
                    scene_id=scene_id,
                    module=module,
                    stage=stage,
                    status=status,
                    message=message,
                    details=details or {},
                )
                sess.add(row)
                sess.flush()
                log_id = row.log_id
        except IntegrityError:
            # processing_logs.dataset_id is ON DELETE CASCADE, so a dataset
            # deleted with force=True while its job threads are still running
            # takes its rows with it and every later insert violates the FK.
            # That is an expected end-of-life race, not a pipeline failure:
            # losing a log line must not abort (or mask) the work being
            # logged, and must not turn the failure handler into a second,
            # noisier exception.
            logger.warning(
                "[PLOG] dataset_id=%s sudah tidak ada - event %s/%s (%s) tidak disimpan",
                dataset_id, module, stage, status,
            )
            return -1

        level = logging.ERROR if status == "FAILED" else logging.INFO
        logger.log(
            level, "[PLOG] %s",
            json.dumps(
                {
                    "log_id": log_id, "dataset_id": dataset_id, "scene_id": scene_id,
                    "module": module, "stage": stage, "status": status, "message": message,
                    **(details or {}),
                },
                default=str,
            ),
        )
        return log_id

    def query_logs(
        self,
        dataset_id: int,
        stage: str | None = None,
        status: str | None = None,
        scene_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
        order: str = "desc",
    ) -> tuple[list[dict], int]:
        with self._db.session() as sess:
            stmt = select(ProcessingLog).where(ProcessingLog.dataset_id == dataset_id)
            if stage:
                stmt = stmt.where(ProcessingLog.stage == stage)
            if status:
                stmt = stmt.where(ProcessingLog.status == status)
            if scene_id:
                stmt = stmt.where(ProcessingLog.scene_id == scene_id)

            total = sess.scalar(select(func.count()).select_from(stmt.subquery())) or 0

            order_col = ProcessingLog.created_at.desc() if order == "desc" else ProcessingLog.created_at.asc()
            rows = sess.scalars(stmt.order_by(order_col).limit(limit).offset(offset)).all()
            return [self._to_dict(r) for r in rows], total

    @staticmethod
    def _to_dict(r: ProcessingLog) -> dict:
        return {
            "log_id": r.log_id,
            "timestamp": r.created_at,
            "module": r.module,
            "dataset_id": r.dataset_id,
            "scene_id": r.scene_id,
            "stage": r.stage,
            "status": r.status,
            "message": r.message,
            "details": r.details or {},
        }


class _StageHandle:
    """Passed into a `with plog.stage(...) as st:` block so the caller can
    attach result fields (output path, size, quality score, ...) that get
    merged into the COMPLETED event's details. `st.output(message=...)`
    overrides the default completion message."""

    def __init__(self) -> None:
        self.details: dict[str, Any] = {}
        # Populated after the `with` block exits successfully, so callers can
        # read timing/resource numbers back out (e.g. to also populate
        # processing_jobs.cpu_usage_percent / memory_usage_mb).
        self.duration_seconds: float | None = None
        self.memory_peak_mb: float | None = None
        self.cpu_peak_percent: float | None = None

    def output(self, **kwargs: Any) -> None:
        self.details.update(kwargs)


class _MemorySampler:
    """Background peak memory/CPU sampler for the lifetime of a stage."""

    def __init__(self) -> None:
        self._process = psutil.Process()
        self._stop = threading.Event()
        self._peak_rss_mb = 0.0
        self._peak_cpu_percent = 0.0
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._process.cpu_percent(interval=None)  # prime the counter (first call always returns 0)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rss_mb = self._process.memory_info().rss / (1024 ** 2)
                cpu_percent = self._process.cpu_percent(interval=None)
                self._peak_rss_mb = max(self._peak_rss_mb, rss_mb)
                self._peak_cpu_percent = max(self._peak_cpu_percent, cpu_percent)
            except Exception:
                pass
            self._stop.wait(_SAMPLE_INTERVAL_SEC)

    def stop(self) -> tuple[float, float]:
        self._stop.set()
        self._thread.join(timeout=2.0)
        return round(self._peak_rss_mb, 2), round(self._peak_cpu_percent, 2)


class PipelineLogger:
    """Thin facade over PipelineLogManager: one-shot `log_event(...)` for
    summary lines, plus the `stage(...)` context manager for timed,
    memory/CPU-sampled STARTED->COMPLETED/FAILED stage blocks."""

    def __init__(self, db: DatabaseClient) -> None:
        self._mgr = PipelineLogManager(db)

    def log_event(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        status: str,
        message: str,
        details: dict | None = None,
    ) -> int:
        return self._mgr.log_event(dataset_id, scene_id, module, stage, status, message, details)

    @contextmanager
    def stage(
        self,
        dataset_id: int,
        scene_id: str,
        module: str,
        stage: str,
        message: str,
        **input_details: Any,
    ) -> Iterator[_StageHandle]:
        handle = _StageHandle()
        self._mgr.log_event(dataset_id, scene_id, module, stage, "STARTED", message, input_details)

        sampler = _MemorySampler()
        sampler.start()
        started = time.monotonic()
        try:
            yield handle
        except Exception as exc:
            duration = round(time.monotonic() - started, 3)
            peak_mb, peak_cpu = sampler.stop()
            self._mgr.log_event(
                dataset_id, scene_id, module, stage, "FAILED",
                f"{stage} failed: {exc}",
                {
                    **input_details,
                    "duration_seconds": duration,
                    "memory_peak_mb": peak_mb,
                    "cpu_peak_percent": peak_cpu,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "traceback": traceback.format_exc()[-_TRACEBACK_MAX_CHARS:],
                },
            )
            raise
        else:
            duration = round(time.monotonic() - started, 3)
            peak_mb, peak_cpu = sampler.stop()
            handle.duration_seconds = duration
            handle.memory_peak_mb = peak_mb
            handle.cpu_peak_percent = peak_cpu
            completion_message = handle.details.pop("message", None) or f"{stage} completed"
            self._mgr.log_event(
                dataset_id, scene_id, module, stage, "COMPLETED",
                completion_message,
                {
                    **input_details,
                    **handle.details,
                    "duration_seconds": duration,
                    "memory_peak_mb": peak_mb,
                    "cpu_peak_percent": peak_cpu,
                },
            )

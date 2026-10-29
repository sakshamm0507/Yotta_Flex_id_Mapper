"""Observability: run ids, structured JSON logs and per-stage timing.

Every stage emits one JSON log line with run_id, stage, rows_in, rows_out and
duration_ms. Lines go to `<log_dir>/<run_id>.jsonl`, and a short text version
goes to stderr. `pipeline_run` stays the single source of truth for run status
and counts. The logs explain how a run got there.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER_NAME = "idmap"
_STANDARD_ATTRS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


def utc_now() -> datetime:
    """Naive UTC timestamp (DuckDB TIMESTAMP columns hold UTC wall time)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def new_run_id(now: datetime | None = None) -> str:
    now = now or utc_now()
    return f"run_{now.strftime('%Y%m%dT%H%M%S%f')[:-3]}Z_{uuid.uuid4().hex[:8]}"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "event": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        parts = [f"{record.levelname:<7}", getattr(record, "run_id", "-")]
        stage = getattr(record, "stage", None)
        if stage:
            parts.append(f"{stage:<9}")
        parts.append(record.getMessage())
        if getattr(record, "rows_in", None) is not None:
            parts.append(f"rows {record.rows_in} -> {record.rows_out}")
        if getattr(record, "duration_ms", None) is not None:
            parts.append(f"({record.duration_ms} ms)")
        for key in ("detail", "failure_reason"):
            if getattr(record, key, None):
                parts.append(f"- {getattr(record, key)}")
        return "  ".join(str(p) for p in parts)


def get_logger() -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(TextFormatter())
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def set_console_level(level: int) -> None:
    for handler in get_logger().handlers:
        if not isinstance(handler, logging.FileHandler):
            handler.setLevel(level)


@contextmanager
def run_log_file(log_dir: Path, run_id: str) -> Iterator[Path]:
    """Attach a JSONL file handler for the duration of one run."""
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"{run_id}.jsonl"
    handler = logging.FileHandler(path, encoding="utf-8")
    handler.setFormatter(JsonFormatter())
    handler.setLevel(logging.DEBUG)
    logger = get_logger()
    logger.addHandler(handler)
    try:
        yield path
    finally:
        logger.removeHandler(handler)
        handler.close()


@dataclass
class StageTiming:
    stage: str
    duration_ms: int
    rows_in: int | None
    rows_out: int | None


@dataclass
class RunLog:
    """Logger bound to a run: every line carries run_id and source_file."""

    run_id: str
    source_file: str
    timings: list[StageTiming] = field(default_factory=list)

    def _extra(self, **fields: Any) -> dict[str, Any]:
        return {"run_id": self.run_id, "source_file": self.source_file, **fields}

    def info(self, event: str, **fields: Any) -> None:
        get_logger().info(event, extra=self._extra(**fields))

    def warning(self, event: str, **fields: Any) -> None:
        get_logger().warning(event, extra=self._extra(**fields))

    def error(self, event: str, **fields: Any) -> None:
        get_logger().error(event, extra=self._extra(**fields))

    @contextmanager
    def stage(self, name: str, rows_in: int | None = None) -> Iterator[dict[str, Any]]:
        """Time a stage. The body sets `ctx['rows_out']` (and optionally extra fields)."""
        ctx: dict[str, Any] = {"rows_in": rows_in, "rows_out": None}
        start = time.perf_counter()
        yield ctx
        duration_ms = int(round((time.perf_counter() - start) * 1000))
        self.timings.append(StageTiming(name, duration_ms, ctx["rows_in"], ctx["rows_out"]))
        extras = {k: v for k, v in ctx.items() if k not in ("rows_in", "rows_out")}
        self.info(
            "stage complete",
            stage=name,
            rows_in=ctx["rows_in"],
            rows_out=ctx["rows_out"],
            duration_ms=duration_ms,
            **extras,
        )

# =============================================================================
# logger_utils.py  –  Centralised Structured Logging
# =============================================================================
# Provides a consistent logger for every notebook / module.
# Emits JSON-formatted log lines so Databricks log aggregation (e.g. Splunk,
# Datadog, Azure Monitor) can parse structured fields without regex.
# =============================================================================

from __future__ import annotations
import json
import logging
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    """Emit every log record as a single-line JSON object."""

    RESERVED = {"message", "timestamp", "level", "logger", "host", "pid"}

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "message":   record.getMessage(),
            "host":      socket.gethostname(),
            "pid":       record.process,
        }

        # Merge any extra= kwargs the caller passed
        for key, val in record.__dict__.items():
            if key not in logging.LogRecord.__dict__ and key not in self.RESERVED:
                payload[key] = val

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def get_logger(
    name: str,
    level: str = "INFO",
    *,
    pipeline: Optional[str] = None,
    env: Optional[str] = None,
) -> logging.Logger:
    """
    Return a named logger with JSON output to stdout (captured by Databricks).

    Parameters
    ----------
    name     : logger name, usually __name__ or the notebook name
    level    : 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR'
    pipeline : optional label attached to every log record (e.g. 'bronze_trades')
    env      : optional environment label (e.g. 'prod')
    """
    log = logging.getLogger(name)

    if log.handlers:          # avoid duplicate handlers on re-import
        return log

    log.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    log.addHandler(handler)
    log.propagate = False

    # Attach contextual extras that appear in every record
    old_factory = logging.getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        if pipeline:
            record.pipeline = pipeline
        if env:
            record.env = env
        return record

    logging.setLogRecordFactory(record_factory)
    return log


# ---------------------------------------------------------------------------
# Pipeline timer context manager
# ---------------------------------------------------------------------------

class PipelineTimer:
    """
    Context manager that logs start / end / duration of a pipeline stage.

    Usage::

        with PipelineTimer(log, "silver_trades_transform"):
            do_work()
    """

    def __init__(self, logger: logging.Logger, stage: str):
        self.logger = logger
        self.stage  = stage
        self._t0: float = 0.0

    def __enter__(self) -> "PipelineTimer":
        self._t0 = time.perf_counter()
        self.logger.info("Stage started", extra={"stage": self.stage, "event": "start"})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        elapsed = round(time.perf_counter() - self._t0, 3)
        if exc_type:
            self.logger.error(
                "Stage FAILED",
                extra={
                    "stage":        self.stage,
                    "event":        "error",
                    "elapsed_s":    elapsed,
                    "error":        str(exc_val),
                    "traceback":    traceback.format_exc(),
                },
            )
            return False   # re-raise
        self.logger.info(
            "Stage completed",
            extra={"stage": self.stage, "event": "end", "elapsed_s": elapsed},
        )
        return False

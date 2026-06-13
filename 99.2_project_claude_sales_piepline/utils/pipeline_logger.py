# =============================================================================
# utils/pipeline_logger.py
# Production-grade structured logger — writes JSON logs to ADLS Gen2
# =============================================================================

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


class PipelineLogger:
    """
    Structured JSON logger for Databricks pipelines.

    Writes logs to:
      - Console (Databricks notebook stdout / cluster driver log)
      - ADLS Gen2 log path as newline-delimited JSON files

    Usage
    -----
    logger = PipelineLogger(spark, config, layer="bronze")
    logger.info("Starting ingest", extra={"source_path": "/..."})
    logger.log_dq_result(dq_result_dict)
    logger.log_run_summary(rows_in=1000, rows_out=990, rows_quarantined=10)
    """

    LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        layer: str,
        run_id: Optional[str] = None,
    ):
        self.spark = spark
        self.config = config
        self.layer = layer.upper()
        self.env = config.get("env", "dev").upper()
        self.pipeline_name = config.get("pipeline_name", "sales_pipeline")
        self.run_id = run_id or self._get_run_id()
        self.log_level = self.LEVELS.get(
            config.get("log_level", "INFO").upper(), 20
        )

        # ADLS Gen2 log path
        adls_account = config["adls_account"]
        adls_container = config.get("log_container", "pipeline-logs")
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        self.adls_log_path = (
            f"abfss://{adls_container}@{adls_account}.dfs.core.windows.net/"
            f"{self.env}/{self.pipeline_name}/{self.layer}/{today}/"
            f"run_{self.run_id}.jsonl"
        )

        self._buffer: list = []
        self._console = logging.getLogger(
            f"{self.pipeline_name}.{self.layer}"
        )
        if not self._console.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            )
            self._console.addHandler(handler)
        self._console.setLevel(self.log_level)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def debug(self, message: str, extra: Optional[Dict] = None):
        self._log("DEBUG", message, extra)

    def info(self, message: str, extra: Optional[Dict] = None):
        self._log("INFO", message, extra)

    def warning(self, message: str, extra: Optional[Dict] = None):
        self._log("WARNING", message, extra)

    def error(self, message: str, extra: Optional[Dict] = None, exc: Optional[Exception] = None):
        if exc:
            extra = extra or {}
            extra["exception"] = traceback.format_exc()
        self._log("ERROR", message, extra)

    def log_dq_result(self, dq_result: Dict[str, Any]):
        """Log a DQX data-quality check result block."""
        self._log(
            level="INFO",
            message="DQ check result",
            extra={"dq_result": dq_result},
        )

    def log_run_summary(
        self,
        rows_in: int,
        rows_out: int,
        rows_quarantined: int = 0,
        extra: Optional[Dict] = None,
    ):
        """Log a standardised run-summary record (used for monitoring dashboards)."""
        payload = {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": rows_quarantined,
            "pass_rate_pct": round((rows_out / rows_in * 100) if rows_in else 0, 2),
        }
        if extra:
            payload.update(extra)
        self._log("INFO", "Run summary", payload)
        self.flush()                 # always flush at end of run

    def flush(self):
        """Write buffered log records to ADLS Gen2 as a JSONL file."""
        if not self._buffer:
            return
        try:
            content = "\n".join(json.dumps(r) for r in self._buffer)
            # Write via Spark (supports managed identity / service principal auth)
            rdd = self.spark.sparkContext.parallelize([content])
            rdd.saveAsTextFile(self.adls_log_path)
            self._buffer.clear()
        except Exception as exc:        # never let logging crash the pipeline
            self._console.warning(
                f"Failed to flush logs to ADLS: {exc}. Logs remain in console."
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _log(self, level: str, message: str, extra: Optional[Dict] = None):
        if self.LEVELS.get(level, 20) < self.log_level:
            return
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "env": self.env,
            "pipeline": self.pipeline_name,
            "layer": self.layer,
            "run_id": self.run_id,
            "message": message,
        }
        if extra:
            record["extra"] = extra
        self._buffer.append(record)
        self._console.log(
            self.LEVELS[level],
            f"[{self.layer}] [run={self.run_id}] {message}"
            + (f" | {json.dumps(extra)}" if extra else ""),
        )

    @staticmethod
    def _get_run_id() -> str:
        """Use Databricks job run ID when available, otherwise timestamp."""
        try:
            from dbruntime.dbutils import DBUtils   # noqa: F401
            ctx = (
                __import__("IPython").get_ipython()
                .user_ns.get("dbutils")
                .notebook.entry_point.getDbutils()
                .notebook()
                .getContext()
            )
            return ctx.currentRunId().toString()
        except Exception:
            return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

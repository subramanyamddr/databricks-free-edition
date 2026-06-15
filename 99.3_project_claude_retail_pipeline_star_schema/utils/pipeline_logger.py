# =============================================================================
# utils/pipeline_logger.py
# Structured JSON logger -> console + ADLS Gen2 (newline-delimited JSON)
# =============================================================================

import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class PipelineLogger:
    """
    Structured logger for Databricks pipelines.

    Writes:
      - Console (driver stdout / job run logs)
      - ADLS Gen2: abfss://<log_container>@<account>.dfs.core.windows.net/
                     <ENV>/<pipeline>/<LAYER>/<yyyy>/<mm>/<dd>/run_<run_id>.jsonl
    """

    LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}

    def __init__(self, spark, config: Dict[str, Any], layer: str, run_id: Optional[str] = None):
        self.spark = spark
        self.config = config
        self.layer = layer.upper()
        self.env = config.get("env", "dev").upper()
        self.pipeline_name = config.get("pipeline_name", "retail_pipeline")
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.log_level = self.LEVELS.get(config.get("log_level", "INFO").upper(), 20)

        adls_account = config["adls_account"]
        adls_container = config.get("log_container", "pipeline-logs")
        today = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        self.adls_log_path = (
            f"abfss://{adls_container}@{adls_account}.dfs.core.windows.net/"
            f"{self.env}/{self.pipeline_name}/{self.layer}/{today}/run_{self.run_id}.jsonl"
        )

        self._buffer = []
        self._console = logging.getLogger(f"{self.pipeline_name}.{self.layer}")
        if not self._console.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
            self._console.addHandler(handler)
        self._console.setLevel(self.log_level)

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
        self._log("INFO", "DQ check result", {"dq_result": dq_result})

    def log_run_summary(self, rows_in: int, rows_out: int, rows_quarantined: int = 0,
                         extra: Optional[Dict] = None):
        payload = {
            "rows_in": rows_in,
            "rows_out": rows_out,
            "rows_quarantined": rows_quarantined,
            "pass_rate_pct": round((rows_out / rows_in * 100) if rows_in else 0, 2),
        }
        if extra:
            payload.update(extra)
        self._log("INFO", "Run summary", payload)
        self.flush()

    def flush(self):
        if not self._buffer:
            return
        try:
            content = "\n".join(json.dumps(r) for r in self._buffer)
            rdd = self.spark.sparkContext.parallelize([content])
            rdd.saveAsTextFile(self.adls_log_path)
            self._buffer.clear()
        except Exception as exc:
            self._console.warning(f"Failed to flush logs to ADLS: {exc}. Logs remain in console.")

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
            f"[{self.layer}] [run={self.run_id}] {message}" + (f" | {json.dumps(extra, default=str)}" if extra else ""),
        )

"""
utils/logger.py
---------------
Structured pipeline logger.

Writes:
  • Human-readable logs  → Databricks console  (print)
  • JSON-structured logs → ADLS Gen2 Delta table  (pipeline_logs.run_log)
  • Per-run summary      → ADLS Gen2 JSON file     (pipeline_logs/<env>/<run_id>.json)

Usage:
    from utils.logger import PipelineLogger
    logger = PipelineLogger(spark, cfg)
    logger.info("bronze", "Starting CSV ingest", row_count=0)
    logger.success("bronze", "Ingest complete", row_count=1234)
    logger.error("silver", "Cast failed", error=str(e))
    logger.finalize()   # flush summary to ADLS at end of notebook
"""

import json
from datetime import datetime, timezone
from typing import Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, LongType, IntegerType
)

LOG_SCHEMA = StructType([
    StructField("run_id",       StringType(),   False),
    StructField("env",          StringType(),   False),
    StructField("notebook",     StringType(),   False),
    StructField("layer",        StringType(),   False),
    StructField("level",        StringType(),   False),  # INFO|WARN|ERROR|SUCCESS
    StructField("message",      StringType(),   False),
    StructField("row_count",    LongType(),     True),
    StructField("duration_ms",  LongType(),     True),
    StructField("extra",        StringType(),   True),   # JSON blob for KV extras
    StructField("logged_at",    TimestampType(), False),
])


class PipelineLogger:
    def __init__(self, spark: SparkSession, cfg):
        self.spark      = spark
        self.cfg        = cfg
        self._records   = []
        self._start_ts  = datetime.now(timezone.utc)
        self._log_table = f"{cfg.catalog_name}.pipeline_logs.run_log"
        self._log_path  = f"{cfg.log_base_path}/{cfg.job_run_id}.json"

    # ── Public API ────────────────────────────────────────────
    def info(self, layer: str, message: str, **kwargs):
        self._log("INFO", layer, message, **kwargs)

    def warn(self, layer: str, message: str, **kwargs):
        self._log("WARN", layer, message, **kwargs)

    def error(self, layer: str, message: str, **kwargs):
        self._log("ERROR", layer, message, **kwargs)

    def success(self, layer: str, message: str, **kwargs):
        self._log("SUCCESS", layer, message, **kwargs)

    def finalize(self, status: str = "COMPLETED"):
        """Flush all buffered log records to ADLS Delta table and JSON summary."""
        self._flush_to_delta()
        self._write_json_summary(status)

    # ── Internal ──────────────────────────────────────────────
    def _log(self, level: str, layer: str, message: str, **kwargs):
        ts       = datetime.now(timezone.utc)
        duration = int((ts - self._start_ts).total_seconds() * 1000)
        row_count = kwargs.pop("row_count", None)
        extra_str = json.dumps(kwargs) if kwargs else None

        record = {
            "run_id":      self.cfg.job_run_id,
            "env":         self.cfg.env,
            "notebook":    self.cfg.notebook_name,
            "layer":       layer,
            "level":       level,
            "message":     message,
            "row_count":   row_count,
            "duration_ms": duration,
            "extra":       extra_str,
            "logged_at":   ts,
        }
        self._records.append(record)

        # Console output (always)
        icon = {"INFO": "ℹ️", "WARN": "⚠️", "ERROR": "❌", "SUCCESS": "✅"}.get(level, "·")
        rc_str = f"  rows={row_count:,}" if row_count is not None else ""
        extra_display = f"  {kwargs}" if kwargs else ""
        print(f"[{ts.strftime('%H:%M:%S')}] {icon} [{level}] [{layer.upper()}] {message}{rc_str}{extra_display}")

    def _flush_to_delta(self):
        if not self._records:
            return
        try:
            # Ensure log schema/table exists
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.cfg.catalog_name}.pipeline_logs"
            )
            df = self.spark.createDataFrame(self._records, schema=LOG_SCHEMA)
            (
                df.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(self._log_table)
            )
            print(f"ℹ️  [{len(self._records)} log records written to {self._log_table}]")
        except Exception as exc:
            # Logger must never crash the pipeline
            print(f"⚠️  Logger flush failed (non-fatal): {exc}")

    def _write_json_summary(self, status: str):
        try:
            end_ts   = datetime.now(timezone.utc)
            duration = round((end_ts - self._start_ts).total_seconds(), 2)
            summary  = {
                "run_id":        self.cfg.job_run_id,
                "env":           self.cfg.env,
                "notebook":      self.cfg.notebook_name,
                "status":        status,
                "started_at":    self._start_ts.isoformat(),
                "ended_at":      end_ts.isoformat(),
                "duration_sec":  duration,
                "total_events":  len(self._records),
                "errors":        sum(1 for r in self._records if r["level"] == "ERROR"),
                "warnings":      sum(1 for r in self._records if r["level"] == "WARN"),
                "records":       [
                    {k: (v.isoformat() if isinstance(v, datetime) else v)
                     for k, v in r.items()}
                    for r in self._records
                ],
            }
            summary_json = json.dumps(summary, indent=2)
            # Write via Spark (single-file) to ADLS
            (
                self.spark.createDataFrame([{"content": summary_json}])
                .coalesce(1)
                .write
                .mode("overwrite")
                .text(self._log_path)
            )
            print(f"✅  Run summary written → {self._log_path}")
        except Exception as exc:
            print(f"⚠️  Summary write failed (non-fatal): {exc}")

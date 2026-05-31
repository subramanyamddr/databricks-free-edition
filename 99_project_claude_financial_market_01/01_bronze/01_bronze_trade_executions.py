# Databricks notebook source
# =============================================================================
# 01_bronze_trade_executions.py
# BRONZE LAYER – Trade Executions Ingestion
#
# Trigger  : Daily Databricks Job (09:00 UTC) or on-demand
# Input    : /Volumes/<catalog>/landing/raw_ingest/trade_executions/
#            file_date=YYYY-MM-DD/  *.csv
# Output   : <catalog>.bronze.trade_executions  (Delta, append)
# =============================================================================

# COMMAND ----------

# %md
# ## Bronze – Trade Executions Ingestion
# Auto Loader (cloudFiles) incrementally picks up new CSV files from the
# landing Volume and appends them to the bronze Delta table.

# COMMAND ----------

# %python
# dbutils.widgets.text('env','dev')
# env = dbutils.widgets.get('env')
# env

# COMMAND ----------

import sys, uuid
from datetime import datetime, timezone

# ── Ensure shared utils are importable ──────────────────────────────────────
# In Databricks, mount the repo or use %pip install / library cluster.
# For notebook-relative imports, dbutils.notebook.run is used in production;
# here we show the repo-based import pattern.

sys.path.insert(0, "/Workspace/Users/subramanyamddr03@gmail.com/databricks-free-edition/99_project_claude_financial_market_01/05_utils")

from env_config   import get_config
from logger_utils import get_logger, PipelineTimer
from dq_utils     import DataQualityRunner, Severity

# COMMAND ----------

# ── Resolve environment ──────────────────────────────────────────────────────
# In a Databricks Job, pass  --conf spark.env=prod  under Advanced → Spark config
try:
    env_param = dbutils.widgets.get("env")         # interactive widget
except Exception:
    env_param = spark.conf.get("env", "dev")       # job cluster conf

cfg = get_config(env_param)
log = get_logger(
    "bronze.trade_executions",
    level=cfg.log_level,
    pipeline="bronze_trade_executions",
    env=cfg.env,
)

PIPELINE      = "bronze_trade_executions"
TABLE         = cfg.bronze_table("trade_executions")
SOURCE_PATH   = cfg.landing_path("trade_executions")
CHECKPOINT    = cfg.checkpoint_path("bronze_trade_executions")
RUN_ID        = str(uuid.uuid4())
STARTED_AT    = datetime.now(timezone.utc)

log.info("Pipeline starting", extra={
    "run_id": RUN_ID, "env": cfg.env, "table": TABLE,
    "source_path": SOURCE_PATH,
})

# COMMAND ----------

# ── Register pipeline run ────────────────────────────────────────────────────

def _register_run(status: str, rows_read=None, rows_written=None, error=None):
    try:
        audit_row = [(
            RUN_ID, PIPELINE, "bronze", cfg.env, status,
            STARTED_AT, datetime.now(timezone.utc),
            rows_read, rows_written, None, error,
            "serverless",
        )]
        schema = (
            "run_id STRING, pipeline_name STRING, layer STRING, env STRING, "
            "status STRING, started_at TIMESTAMP, completed_at TIMESTAMP, "
            "rows_read LONG, rows_written LONG, source_files STRING, "
            "error_message STRING, spark_app_id STRING"
        )
        (spark.createDataFrame(audit_row, schema=schema)
              .write.format("delta").mode("append")
              .saveAsTable(f"{cfg.catalog}.audit.pipeline_runs"))
    except Exception as e:
        log.warning("Could not write audit record: %s", e)

_register_run("RUNNING")

# COMMAND ----------

# ── Define bronze schema (all strings – preserves source fidelity) ───────────

from pyspark.sql.types import StructType, StructField, StringType

BRONZE_CSV_SCHEMA = StructType([
    StructField("trade_id",         StringType()),
    StructField("trade_date",       StringType()),
    StructField("settlement_date",  StringType()),
    StructField("instrument_id",    StringType()),
    StructField("instrument_code",  StringType()),
    StructField("asset_class",      StringType()),
    StructField("counterparty_id",  StringType()),
    StructField("trader_id",        StringType()),
    StructField("portfolio_id",     StringType()),
    StructField("account_id",       StringType()),
    StructField("side",             StringType()),
    StructField("quantity",         StringType()),
    StructField("execution_price",  StringType()),
    StructField("gross_value",      StringType()),
    StructField("commission",       StringType()),
    StructField("currency",         StringType()),
    StructField("venue",            StringType()),
    StructField("trade_status",     StringType()),
    StructField("source_system",    StringType()),
    StructField("load_date",        StringType()),
    StructField("file_date",        StringType()),
])

# COMMAND ----------

# ── Auto Loader ingest ───────────────────────────────────────────────────────
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

with PipelineTimer(log, "auto_loader_read"):
    raw_df: DataFrame = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",              "csv")
             .option("cloudFiles.schemaLocation",      CHECKPOINT + "/_schema")
             .option("cloudFiles.inferColumnTypes",    "false")    # always string in bronze
             .option("cloudFiles.validateOptions",     "true")
             .option("cloudFiles.useNotifications",    "false")    # polling – no event setup needed
             .option("header",                         "true")
             .option("mode",                           "PERMISSIVE")  # bad rows → _corrupt_record
             .option("columnNameOfCorruptRecord",      "_corrupt_record")
             .schema(BRONZE_CSV_SCHEMA)
             .load(SOURCE_PATH)
    )

# COMMAND ----------

# ── Add pipeline metadata columns ────────────────────────────────────────────

def add_metadata(df: DataFrame, run_id: str) -> DataFrame:
    hash_cols = [F.coalesce(F.col(c), F.lit("")).cast("string")
                 for c in df.columns if not c.startswith("_")]
    return (
        df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file",      F.col("_metadata.file_path"))
        .withColumn("_pipeline_run_id",  F.lit(run_id))
        .withColumn("_row_hash",
                    F.md5(F.concat_ws("|", *hash_cols)))
    )

enriched_df = add_metadata(raw_df, RUN_ID)

# COMMAND ----------

# ── Write to bronze Delta table (micro-batch trigger Once) ───────────────────

with PipelineTimer(log, "bronze_write"):
    query = (
        enriched_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema",        "false")          # schema locked in bronze
        .trigger(availableNow=True)                      # process all pending, then stop
        .toTable(TABLE)
    )
    query.awaitTermination()

log.info("Bronze write complete", extra={"run_id": RUN_ID, "table": TABLE})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fin_platform_dev.bronze.trade_executions

# COMMAND ----------

# MAGIC %skip
# MAGIC # ── Data Quality on today's partition ────────────────────────────────────────
# MAGIC # Read the partition that was just written to run synchronous DQ checks.
# MAGIC
# MAGIC file_date_param = spark.conf.get("file_date",
# MAGIC                                  datetime.now(timezone.utc).strftime("%Y-%m-%d"))
# MAGIC
# MAGIC today_df = (spark.read.format("delta")
# MAGIC                  .table(TABLE)
# MAGIC                  .filter(F.col("file_date") == file_date_param))
# MAGIC
# MAGIC rows_read    = today_df.count()
# MAGIC rows_written = rows_read   # bronze is 1-to-1
# MAGIC
# MAGIC dq = (
# MAGIC     DataQualityRunner(
# MAGIC         spark, PIPELINE, "bronze", TABLE,
# MAGIC         halt_on_critical=cfg.enable_data_quality_halt,
# MAGIC     )
# MAGIC     .expect_row_count_gt(0, Severity.CRITICAL)
# MAGIC     .expect_no_nulls("trade_id",   Severity.CRITICAL)
# MAGIC     .expect_no_nulls("trade_date", Severity.CRITICAL)
# MAGIC     .expect_no_nulls("side",       Severity.CRITICAL)
# MAGIC     .expect_column_values_in_set("side", ["BUY", "SELL"], Severity.CRITICAL)
# MAGIC     .expect_no_duplicates(["trade_id", "file_date"], Severity.CRITICAL)
# MAGIC )
# MAGIC
# MAGIC with PipelineTimer(log, "dq_checks_bronze"):
# MAGIC     report = dq.run(today_df)
# MAGIC
# MAGIC if report.passed:
# MAGIC     log.info("DQ PASSED – all rules satisfied", extra={"run_id": RUN_ID})
# MAGIC else:
# MAGIC     log.warning("DQ WARNING – some rules failed", extra={
# MAGIC         "run_id":   RUN_ID,
# MAGIC         "failures": [r.rule_name for r in report.results if not r.passed],
# MAGIC     })
# MAGIC
# MAGIC _register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
# MAGIC log.info("Pipeline finished", extra={"run_id": RUN_ID, "rows_written": rows_written})

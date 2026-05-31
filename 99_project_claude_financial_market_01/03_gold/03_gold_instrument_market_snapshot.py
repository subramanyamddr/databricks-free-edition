# Databricks notebook source
# =============================================================================
# 03_gold_instrument_market_snapshot.py
# GOLD LAYER – Instrument Market Snapshot with 52-Week Rolling Stats
#
# Reads from  : <catalog>.silver.market_prices  (all history for rolling window)
# Writes to   : <catalog>.gold.instrument_market_snapshot  (REPLACE partition)
#
# Business logic:
#   • Today's OHLCV + VWAP + derived metrics from silver
#   • Rolling 52-week high / low using Window functions over historical silver
# =============================================================================

# COMMAND ----------

import sys, uuid
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/Workspace/Users/subramanyamddr03@gmail.com/databricks-free-edition/99_project_claude_financial_market_01/05_utils")

from env_config   import get_config
from logger_utils import get_logger, PipelineTimer
from dq_utils     import DataQualityRunner, Severity

# COMMAND ----------

try:
    env_param = dbutils.widgets.get("env")
except Exception:
    env_param = spark.conf.get("env", "dev")

file_date_param = spark.conf.get("file_date",
                                  datetime.now(timezone.utc).strftime("%Y-%m-%d"))

cfg = get_config(env_param)
log = get_logger(
    "gold.market_snapshot",
    level=cfg.log_level,
    pipeline="gold_market_snapshot",
    env=cfg.env,
)

PIPELINE      = "gold_market_snapshot"
SILVER_TABLE  = cfg.silver_table("market_prices")
GOLD_TABLE    = cfg.gold_table("instrument_market_snapshot")
RUN_ID        = str(uuid.uuid4())
STARTED_AT    = datetime.now(timezone.utc)

LOOKBACK_DAYS = 365   # 52 weeks

log.info("Pipeline starting", extra={"run_id": RUN_ID, "file_date": file_date_param})

# COMMAND ----------

def _register_run(status, rows_read=None, rows_written=None, error=None):
    try:
        row = [(RUN_ID, PIPELINE, "gold", cfg.env, status,
                STARTED_AT, datetime.now(timezone.utc),
                rows_read, rows_written, None, error,
                spark.sparkContext.applicationId)]
        schema = (
            "run_id STRING, pipeline_name STRING, layer STRING, env STRING, "
            "status STRING, started_at TIMESTAMP, completed_at TIMESTAMP, "
            "rows_read LONG, rows_written LONG, source_files STRING, "
            "error_message STRING, spark_app_id STRING"
        )
        spark.createDataFrame(row, schema=schema).write.format("delta").mode("append").saveAsTable(
            f"{cfg.catalog}.audit.pipeline_runs"
        )
    except Exception as e:
        log.warning("Audit write failed: %s", e)

_register_run("RUNNING")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

trade_date_lit = F.lit(file_date_param).cast("date")
window_start   = (datetime.strptime(file_date_param, "%Y-%m-%d")
                  - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

with PipelineTimer(log, "read_silver"):
    # Load 52-week window of silver prices for rolling stats
    history_df = (
        spark.read.format("delta").table(SILVER_TABLE)
             .filter(
                 (F.col("price_date") >= F.lit(window_start).cast("date"))
                 & (F.col("price_date") <= trade_date_lit)
             )
             .select("price_date", "instrument_code", "asset_class",
                     "currency", "close_price", "price_source")
    )

    today_df = (
        spark.read.format("delta").table(SILVER_TABLE)
             .filter(F.col("price_date") == trade_date_lit)
    )
    rows_read = today_df.count()

# COMMAND ----------

with PipelineTimer(log, "compute_rolling"):
    w = Window.partitionBy("instrument_code").orderBy("price_date").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    rolling_df = (
        history_df
        .withColumn("high_52w", F.max("close_price").over(w).cast("decimal(18,6)"))
        .withColumn("low_52w",  F.min("close_price").over(w).cast("decimal(18,6)"))
        .filter(F.col("price_date") == trade_date_lit)
        .select("instrument_code", "high_52w", "low_52w")
    )

# COMMAND ----------

with PipelineTimer(log, "build_snapshot"):
    snapshot_df = (
        today_df
        .join(rolling_df, on="instrument_code", how="left")
        .withColumn("_pipeline_run_id",   F.lit(RUN_ID))
        .withColumn("_gold_processed_at", F.current_timestamp())
        .select(
            "price_date", "instrument_code", "asset_class", "currency",
            "close_price", "vwap", "volume",
            "daily_return_pct", "intraday_range",
            "high_52w", "low_52w", "price_source",
            "_pipeline_run_id", "_gold_processed_at",
        )
    )

# COMMAND ----------

dq = (
    DataQualityRunner(
        spark, PIPELINE, "gold", GOLD_TABLE,
        halt_on_critical=cfg.enable_data_quality_halt,
    )
    .expect_row_count_gt(0, Severity.CRITICAL)
    .expect_no_nulls("price_date",      Severity.CRITICAL)
    .expect_no_nulls("instrument_code", Severity.CRITICAL)
    .expect_no_nulls("close_price",     Severity.CRITICAL)
    .expect_no_duplicates(["price_date", "instrument_code"], Severity.CRITICAL)
)

with PipelineTimer(log, "dq_checks_gold"):
    dq.run(snapshot_df)

# COMMAND ----------

with PipelineTimer(log, "gold_write"):
    (
        snapshot_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"price_date = '{file_date_param}'")
        .saveAsTable(GOLD_TABLE)
    )
    rows_written = snapshot_df.count()

_register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
log.info("Pipeline finished", extra={
    "run_id": RUN_ID, "rows_read": rows_read, "rows_written": rows_written,
})

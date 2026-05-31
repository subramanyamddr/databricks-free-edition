# Databricks notebook source
# =============================================================================
# 03_gold_daily_trade_summary.py
# GOLD LAYER – Daily Trade Summary by Instrument
#
# Reads from  : <catalog>.silver.trade_executions
# Writes to   : <catalog>.gold.daily_trade_summary  (REPLACE partition)
#
# Business logic:
#   • Aggregate trades per trade_date / asset_class / instrument_code / currency
#   • Buy vs sell counts, quantities, values
#   • Net value  = total_buy_value - total_sell_value
#   • Unique trader, portfolio, counterparty counts
# =============================================================================

# COMMAND ----------

import sys, uuid
from datetime import datetime, timezone

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
    "gold.daily_trade_summary",
    level=cfg.log_level,
    pipeline="gold_daily_trade_summary",
    env=cfg.env,
)

PIPELINE      = "gold_daily_trade_summary"
SILVER_TABLE  = cfg.silver_table("trade_executions")
GOLD_TABLE    = cfg.gold_table("daily_trade_summary")
RUN_ID        = str(uuid.uuid4())
STARTED_AT    = datetime.now(timezone.utc)

log.info("Pipeline starting", extra={
    "run_id": RUN_ID, "file_date": file_date_param,
    "source": SILVER_TABLE, "target": GOLD_TABLE,
})

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
from delta.tables import DeltaTable

# ── 1. Read silver ────────────────────────────────────────────────────────────

with PipelineTimer(log, "read_silver"):
    silver_df = (
        spark.read.format("delta")
             .table(SILVER_TABLE)
             .filter(F.col("trade_date") == F.lit(file_date_param).cast("date"))
    )
    rows_read = silver_df.count()
    log.info("Silver rows read", extra={"count": rows_read})

# COMMAND ----------

# ── 2. Aggregate ──────────────────────────────────────────────────────────────

with PipelineTimer(log, "aggregate"):
    GROUP_KEYS = ["trade_date", "asset_class", "instrument_code", "currency"]

    # Split buy / sell for separate aggregation then join
    buy_df  = silver_df.filter(F.col("side") == "BUY")
    sell_df = silver_df.filter(F.col("side") == "SELL")

    buy_agg = buy_df.groupBy(*GROUP_KEYS).agg(
        F.count("*")                          .alias("buy_count"),
        F.sum("quantity")                     .alias("total_buy_quantity"),
        F.sum("gross_value")                  .alias("total_buy_value"),
    )

    sell_agg = sell_df.groupBy(*GROUP_KEYS).agg(
        F.count("*")                          .alias("sell_count"),
        F.sum("quantity")                     .alias("total_sell_quantity"),
        F.sum("gross_value")                  .alias("total_sell_value"),
    )

    # Overall aggregates (buy + sell combined)
    all_agg = silver_df.groupBy(*GROUP_KEYS).agg(
        F.count("*")                                        .alias("total_trade_count"),
        F.avg("execution_price").cast("decimal(18,6)")      .alias("avg_execution_price"),
        F.sum("commission").cast("decimal(18,2)")           .alias("total_commission"),
        F.countDistinct("trader_id")                        .alias("unique_traders"),
        F.countDistinct("portfolio_id")                     .alias("unique_portfolios"),
        F.countDistinct("counterparty_id")                  .alias("unique_counterparties"),
    )

    # Join all three
    gold_df = (
        all_agg
        .join(buy_agg,  GROUP_KEYS, "left")
        .join(sell_agg, GROUP_KEYS, "left")
        .fillna(0, subset=["buy_count", "sell_count",
                           "total_buy_quantity", "total_sell_quantity",
                           "total_buy_value", "total_sell_value"])
        .withColumn(
            "net_value",
            (F.col("total_buy_value") - F.col("total_sell_value")).cast("decimal(24,2)")
        )
        .withColumn("_pipeline_run_id",   F.lit(RUN_ID))
        .withColumn("_gold_processed_at", F.current_timestamp())
        # Reorder to match DDL
        .select(
            "trade_date", "asset_class", "instrument_code", "currency",
            "buy_count", "sell_count", "total_trade_count",
            "total_buy_quantity", "total_sell_quantity",
            "total_buy_value", "total_sell_value", "net_value",
            "avg_execution_price", "total_commission",
            "unique_traders", "unique_portfolios", "unique_counterparties",
            "_pipeline_run_id", "_gold_processed_at",
        )
    )

# COMMAND ----------

# MAGIC %skip
# MAGIC # ── 3. Data Quality ───────────────────────────────────────────────────────────
# MAGIC
# MAGIC dq = (
# MAGIC     DataQualityRunner(
# MAGIC         spark, PIPELINE, "gold", GOLD_TABLE,
# MAGIC         halt_on_critical=cfg.enable_data_quality_halt,
# MAGIC     )
# MAGIC     .expect_row_count_gt(0, Severity.CRITICAL)
# MAGIC     .expect_no_nulls("trade_date",      Severity.CRITICAL)
# MAGIC     .expect_no_nulls("instrument_code", Severity.CRITICAL)
# MAGIC     .expect_no_nulls("total_trade_count", Severity.CRITICAL)
# MAGIC     .expect_no_duplicates(
# MAGIC         ["trade_date", "asset_class", "instrument_code", "currency"],
# MAGIC         Severity.CRITICAL
# MAGIC     )
# MAGIC )
# MAGIC
# MAGIC with PipelineTimer(log, "dq_checks_gold"):
# MAGIC     dq.run(gold_df)

# COMMAND ----------

# ── 4. Write gold – replace today's partition ─────────────────────────────────
# Use replaceWhere to atomically replace the partition without touching others.

with PipelineTimer(log, "gold_write"):
    (
        gold_df
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"trade_date = '{file_date_param}'")
        .saveAsTable(GOLD_TABLE)
    )
    rows_written = gold_df.count()

_register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
log.info("Pipeline finished", extra={
    "run_id": RUN_ID, "rows_read": rows_read, "rows_written": rows_written,
})

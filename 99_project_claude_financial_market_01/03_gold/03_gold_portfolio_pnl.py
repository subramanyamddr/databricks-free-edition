# Databricks notebook source
# =============================================================================
# 03_gold_portfolio_pnl.py
# GOLD LAYER – Portfolio Daily P&L
#
# Reads from  : <catalog>.silver.trade_executions
#               <catalog>.silver.market_prices
# Writes to   : <catalog>.gold.portfolio_daily_pnl  (REPLACE partition)
#
# Business logic:
#   • Join trades with EOD close price for each instrument
#   • Compute net_quantity = buy_qty - sell_qty per portfolio / instrument
#   • avg_cost = weighted average execution price
#   • market_value = net_quantity * close_price
#   • unrealised_pnl = market_value - (net_quantity * avg_cost)
#   • realised_pnl   = value of closed (sell) legs – simplified FIFO proxy
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
    "gold.portfolio_pnl",
    level=cfg.log_level,
    pipeline="gold_portfolio_pnl",
    env=cfg.env,
)

PIPELINE       = "gold_portfolio_pnl"
SILVER_TRADES  = cfg.silver_table("trade_executions")
SILVER_PRICES  = cfg.silver_table("market_prices")
GOLD_TABLE     = cfg.gold_table("portfolio_daily_pnl")
RUN_ID         = str(uuid.uuid4())
STARTED_AT     = datetime.now(timezone.utc)

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

trade_date_lit = F.lit(file_date_param).cast("date")

with PipelineTimer(log, "read_silver"):
    trades_df = (
        spark.read.format("delta").table(SILVER_TRADES)
             .filter(F.col("trade_date") == trade_date_lit)
    )
    prices_df = (
        spark.read.format("delta").table(SILVER_PRICES)
             .filter(F.col("price_date") == trade_date_lit)
             .select("instrument_code", "close_price", "currency")
    )
    rows_read = trades_df.count()

# COMMAND ----------

# ── 2. Position aggregation ───────────────────────────────────────────────────

with PipelineTimer(log, "aggregate_positions"):
    GROUP = ["trade_date", "portfolio_id", "instrument_code", "asset_class", "currency"]

    buy_pos = (
        trades_df.filter(F.col("side") == "BUY")
        .groupBy(*GROUP)
        .agg(
            F.sum("quantity")                              .alias("buy_qty"),
            (F.sum(F.col("quantity") * F.col("execution_price"))
             / F.sum("quantity")).cast("decimal(18,6)")    .alias("avg_cost"),
        )
    )

    sell_pos = (
        trades_df.filter(F.col("side") == "SELL")
        .groupBy(*GROUP)
        .agg(
            F.sum("quantity")                              .alias("sell_qty"),
            (F.sum(F.col("quantity") * F.col("execution_price"))
             / F.sum("quantity")).cast("decimal(18,6)")    .alias("avg_sell_price"),
        )
    )

    positions = (
        buy_pos
        .join(sell_pos, GROUP, "full")
        .fillna(0, subset=["buy_qty", "sell_qty"])
        .withColumn("net_quantity", (F.col("buy_qty") - F.col("sell_qty")).cast("long"))
        # Join EOD price
        .join(prices_df.alias("px"), on="instrument_code", how="left")
        # P&L calculations
        .withColumn(
            "market_value",
            (F.col("net_quantity") * F.coalesce(F.col("close_price"), F.lit(0)))
            .cast("decimal(24,2)")
        )
        .withColumn(
            "unrealised_pnl",
            (F.col("net_quantity") * (
                F.coalesce(F.col("close_price"), F.lit(0))
                - F.coalesce(F.col("avg_cost"), F.lit(0))
            )).cast("decimal(24,2)")
        )
        .withColumn(
            "realised_pnl",
            (F.col("sell_qty") * (
                F.coalesce(F.col("avg_sell_price"), F.lit(0))
                - F.coalesce(F.col("avg_cost"), F.lit(0))
            )).cast("decimal(24,2)")
        )
        .withColumn(
            "total_pnl",
            (F.col("unrealised_pnl") + F.coalesce(F.col("realised_pnl"), F.lit(0)))
            .cast("decimal(24,2)")
        )
        .withColumn("_pipeline_run_id",   F.lit(RUN_ID))
        .withColumn("_gold_processed_at", F.current_timestamp())
        .select(
            "trade_date", "portfolio_id", "instrument_code", "asset_class", "currency",
            "net_quantity", "avg_cost", "close_price", "market_value",
            "realised_pnl", "unrealised_pnl", "total_pnl",
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
    .expect_no_nulls("trade_date",    Severity.CRITICAL)
    .expect_no_nulls("portfolio_id",  Severity.CRITICAL)
    .expect_no_duplicates(["trade_date", "portfolio_id", "instrument_code"], Severity.CRITICAL)
)

with PipelineTimer(log, "dq_checks_gold"):
    dq.run(positions)

# COMMAND ----------

with PipelineTimer(log, "gold_write"):
    (
        positions.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"trade_date = '{file_date_param}'")
        .saveAsTable(GOLD_TABLE)
    )
    rows_written = positions.count()

_register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
log.info("Pipeline finished", extra={
    "run_id": RUN_ID, "rows_read": rows_read, "rows_written": rows_written,
})

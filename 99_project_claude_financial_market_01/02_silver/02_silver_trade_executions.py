# Databricks notebook source
# =============================================================================
# 02_silver_trade_executions.py
# SILVER LAYER – Trade Executions Cleanse & Enrich
#
# Reads from  : <catalog>.bronze.trade_executions  (today's partition)
# Writes to   : <catalog>.silver.trade_executions  (MERGE / upsert on trade_id)
#
# Transformations:
#   • Cast all types (dates, decimals, longs)
#   • Derive net_value  = gross_value ± commission  (negative for SELL)
#   • Standardise side  (upper-case, trim)
#   • Filter corrupt / null-key records to quarantine
#   • MERGE (upsert) into silver to handle late-arriving corrections
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
    "silver.trade_executions",
    level=cfg.log_level,
    pipeline="silver_trade_executions",
    env=cfg.env,
)

PIPELINE      = "silver_trade_executions"
BRONZE_TABLE  = cfg.bronze_table("trade_executions")
SILVER_TABLE  = cfg.silver_table("trade_executions")
QUARANTINE    = cfg.silver_table("quarantine_trade_executions") if cfg.env == "prod" else None
RUN_ID        = str(uuid.uuid4())
STARTED_AT    = datetime.now(timezone.utc)

log.info("Pipeline starting", extra={
    "run_id": RUN_ID, "file_date": file_date_param,
    "source": BRONZE_TABLE, "target": SILVER_TABLE,
})

# COMMAND ----------

def _register_run(status, rows_read=None, rows_written=None, error=None):
    try:
        row = [(RUN_ID, PIPELINE, "silver", cfg.env, status,
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
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

# ── 1. Read today's bronze partition ─────────────────────────────────────────

with PipelineTimer(log, "read_bronze"):
    bronze_df: DataFrame = (
        spark.read.format("delta")
             .table(BRONZE_TABLE)
             .filter(F.col("file_date") == file_date_param)
    )
    rows_read = bronze_df.count()
    log.info("Bronze rows read", extra={"count": rows_read, "file_date": file_date_param})

# COMMAND ----------

# ── 2. Type casting & cleansing ───────────────────────────────────────────────

with PipelineTimer(log, "transform"):
    silver_df: DataFrame = (
        bronze_df
        # ── Cast types
        .withColumn("trade_date",       F.to_date("trade_date",       "yyyy-MM-dd"))
        .withColumn("settlement_date",  F.to_date("settlement_date",  "yyyy-MM-dd"))
        .withColumn("quantity",         F.col("quantity").cast("long"))
        .withColumn("execution_price",  F.col("execution_price").cast("decimal(18,6)"))
        .withColumn("gross_value",      F.col("gross_value").cast("decimal(18,2)"))
        .withColumn("commission",       F.col("commission").cast("decimal(18,2)"))

        # ── Standardise side
        .withColumn("side", F.upper(F.trim(F.col("side"))))

        # ── Derive net_value: sells reduce position (net out commission both ways)
        .withColumn(
            "net_value",
            F.when(
                F.col("side") == "BUY",
                F.col("gross_value") + F.coalesce(F.col("commission"), F.lit(0))
            ).otherwise(
                -(F.col("gross_value") + F.coalesce(F.col("commission"), F.lit(0)))
            ).cast("decimal(18,2)")
        )

        # ── Add silver metadata
        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_pipeline_run_id",     F.lit(RUN_ID))

        # ── Drop raw-only columns not needed in silver
        .drop("load_date", "file_date", "_source_file")
    )

# COMMAND ----------

# ── 3. Quarantine corrupt rows ────────────────────────────────────────────────
# Rows where key casts produced nulls (e.g. trade_date couldn't parse)

corrupt_df = silver_df.filter(
    F.col("trade_id").isNull()
    | F.col("trade_date").isNull()
    | F.col("execution_price").isNull()
    | ~F.col("side").isin("BUY", "SELL")
)

corrupt_count = corrupt_df.count()
if corrupt_count > 0:
    log.warning("Quarantining corrupt rows", extra={
        "count": corrupt_count, "run_id": RUN_ID
    })
    # Write quarantine (best-effort; do not halt for quarantine write failure)
    try:
        (corrupt_df
         .withColumn("_quarantine_reason", F.lit("null_key_or_invalid_side"))
         .withColumn("_quarantine_ts",     F.current_timestamp())
         .write.format("delta").mode("append")
         .saveAsTable(f"{cfg.catalog}.silver.quarantine_trade_executions"))
    except Exception as qe:
        log.error("Quarantine write failed (non-fatal): %s", qe)

# Keep only valid rows
clean_df = silver_df.filter(
    F.col("trade_id").isNotNull()
    & F.col("trade_date").isNotNull()
    & F.col("execution_price").isNotNull()
    & F.col("side").isin("BUY", "SELL")
)

# COMMAND ----------

# ── 4. Data Quality – pre-merge checks ───────────────────────────────────────

dq = (
    DataQualityRunner(
        spark, PIPELINE, "silver", SILVER_TABLE,
        halt_on_critical=cfg.enable_data_quality_halt,
    )
    .expect_row_count_gt(0, Severity.CRITICAL)
    .expect_no_nulls("trade_id",        Severity.CRITICAL)
    .expect_no_nulls("trade_date",      Severity.CRITICAL)
    .expect_no_nulls("execution_price", Severity.CRITICAL)
    .expect_column_values_in_set("side", ["BUY", "SELL"], Severity.CRITICAL)
    .expect_column_values_positive("quantity",        Severity.CRITICAL)
    .expect_column_values_positive("execution_price", Severity.CRITICAL)
    .expect_no_duplicates(["trade_id"], Severity.WARNING)  # warn; MERGE handles true dedup
)

with PipelineTimer(log, "dq_checks_silver"):
    dq_report = dq.run(clean_df)

# COMMAND ----------

# ── 5. MERGE (upsert) into silver Delta table ─────────────────────────────────
# Handles re-runs and late-arriving trade corrections idempotently.

with PipelineTimer(log, "silver_merge"):
    # Select only the columns that exist in the silver table DDL
    silver_cols = [
        "trade_id", "trade_date", "settlement_date", "instrument_id",
        "instrument_code", "asset_class", "counterparty_id", "trader_id",
        "portfolio_id", "account_id", "side", "quantity",
        "execution_price", "gross_value", "commission", "net_value",
        "currency", "venue", "trade_status", "source_system",
        "_ingest_timestamp", "_pipeline_run_id", "_silver_processed_at", "_row_hash",
    ]
    insert_df = clean_df.select(*silver_cols)

    if DeltaTable.isDeltaTable(spark, f"spark-warehouse/{SILVER_TABLE.replace('.', '/')}") \
       or spark.catalog.tableExists(SILVER_TABLE):
        delta_tbl = DeltaTable.forName(spark, SILVER_TABLE)
        (
            delta_tbl.alias("tgt")
            .merge(
                insert_df.alias("src"),
                "tgt.trade_id = src.trade_id"   # natural business key
            )
            .whenMatchedUpdate(
                condition="tgt._row_hash <> src._row_hash",  # only update if data changed
                set={col: f"src.{col}" for col in silver_cols}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        log.info("Silver MERGE complete", extra={"run_id": RUN_ID})
    else:
        # First run – simple write
        (insert_df.write.format("delta").mode("overwrite")
                  .option("overwriteSchema", "true")
                  .saveAsTable(SILVER_TABLE))
        log.info("Silver initial write complete", extra={"run_id": RUN_ID})

    rows_written = spark.read.format("delta").table(SILVER_TABLE).filter(
        F.col("trade_date") == F.lit(file_date_param).cast("date")
    ).count()

_register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
log.info("Pipeline finished", extra={
    "run_id": RUN_ID, "rows_read": rows_read,
    "rows_written": rows_written, "corrupt": corrupt_count,
})

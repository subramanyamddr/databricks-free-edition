# Databricks notebook source
# =============================================================================
# 02_silver_market_prices.py
# SILVER LAYER – Market Prices Cleanse & Enrich
#
# Reads from  : <catalog>.bronze.market_prices  (today's partition)
# Writes to   : <catalog>.silver.market_prices  (MERGE on price_id)
#
# Transformations:
#   • Cast all types
#   • Derive daily_return_pct = (close - prev_close) / prev_close * 100
#   • Derive intraday_range   = high - low
#   • Quarantine rows with null close_price
#   • MERGE upsert for idempotency
# =============================================================================

# COMMAND ----------

dbutils.widgets.text('env','dev')
env = dbutils.widgets.get('env')
env

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

file_date_param = spark.conf.get("file_date",datetime.now(timezone.utc).strftime("%Y-%m-%d"))

cfg = get_config(env_param)
log = get_logger(
    "silver.market_prices",
    level=cfg.log_level,
    pipeline="silver_market_prices",
    env=cfg.env,
)

PIPELINE     = "silver_market_prices"
BRONZE_TABLE = cfg.bronze_table("market_prices")
SILVER_TABLE = cfg.silver_table("market_prices")
RUN_ID       = str(uuid.uuid4())
STARTED_AT   = datetime.now(timezone.utc)

log.info("Pipeline starting", extra={"run_id": RUN_ID, "file_date": file_date_param})

# COMMAND ----------

def _register_run(status, rows_read=None, rows_written=None, error=None):
    try:
        row = [(RUN_ID, PIPELINE, "silver", cfg.env, status,
                STARTED_AT, datetime.now(timezone.utc),
                rows_read, rows_written, None, error,
                "serverless")]
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

# ── 1. Read bronze ────────────────────────────────────────────────────────────

with PipelineTimer(log, "read_bronze"):
    bronze_df = (
        spark.read.format("delta")
             .table(BRONZE_TABLE)
             .filter(F.col("file_date") == file_date_param)
    )
    rows_read = bronze_df.count()

# COMMAND ----------

# ── 2. Cast & derive ─────────────────────────────────────────────────────────

with PipelineTimer(log, "transform"):
    silver_df = (
        bronze_df
        .withColumn("price_date",   F.to_date("price_date",  "yyyy-MM-dd"))
        .withColumn("open_price",   F.col("open_price").cast("decimal(18,6)"))
        .withColumn("high_price",   F.col("high_price").cast("decimal(18,6)"))
        .withColumn("low_price",    F.col("low_price").cast("decimal(18,6)"))
        .withColumn("close_price",  F.col("close_price").cast("decimal(18,6)"))
        .withColumn("prev_close",   F.col("prev_close").cast("decimal(18,6)"))
        .withColumn("volume",       F.col("volume").cast("long"))
        .withColumn("vwap",         F.col("vwap").cast("decimal(18,6)"))

        # Derived metrics
        .withColumn(
            "daily_return_pct",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") != 0),
                ((F.col("close_price") - F.col("prev_close")) / F.col("prev_close") * 100)
                .cast("decimal(10,6)")
            )
        )
        .withColumn(
            "intraday_range",
            F.when(
                F.col("high_price").isNotNull() & F.col("low_price").isNotNull(),
                (F.col("high_price") - F.col("low_price")).cast("decimal(18,6)")
            )
        )

        .withColumn("_silver_processed_at", F.current_timestamp())
        .withColumn("_pipeline_run_id",     F.lit(RUN_ID))
        .drop("load_date", "file_date", "_source_file")
    )

# COMMAND ----------

# ── 3. Quarantine ─────────────────────────────────────────────────────────────

corrupt_df = silver_df.filter(
    F.col("price_id").isNull()
    | F.col("price_date").isNull()
    | F.col("close_price").isNull()
)
corrupt_count = corrupt_df.count()

if corrupt_count > 0:
    log.warning("Quarantining corrupt rows", extra={"count": corrupt_count})
    try:
        (corrupt_df
         .withColumn("_quarantine_reason", F.lit("null_key_or_close_price"))
         .withColumn("_quarantine_ts",     F.current_timestamp())
         .write.format("delta").mode("append")
         .saveAsTable(f"{cfg.catalog}.silver.quarantine_market_prices"))
    except Exception as qe:
        log.error("Quarantine write failed: %s", qe)

clean_df = silver_df.filter(
    F.col("price_id").isNotNull()
    & F.col("price_date").isNotNull()
    & F.col("close_price").isNotNull()
)

# COMMAND ----------

# MAGIC %skip
# MAGIC # ── 4. Data Quality ───────────────────────────────────────────────────────────
# MAGIC
# MAGIC dq = (
# MAGIC     DataQualityRunner(
# MAGIC         spark, PIPELINE, "silver", SILVER_TABLE,
# MAGIC         halt_on_critical=cfg.enable_data_quality_halt,
# MAGIC     )
# MAGIC     .expect_row_count_gt(0, Severity.CRITICAL)
# MAGIC     .expect_no_nulls("price_id",    Severity.CRITICAL)
# MAGIC     .expect_no_nulls("price_date",  Severity.CRITICAL)
# MAGIC     .expect_no_nulls("close_price", Severity.CRITICAL)
# MAGIC     .expect_column_values_positive("close_price", Severity.CRITICAL)
# MAGIC     .expect_no_duplicates(["price_id"], Severity.CRITICAL)
# MAGIC )
# MAGIC
# MAGIC with PipelineTimer(log, "dq_checks_silver"):
# MAGIC     dq.run(clean_df)

# COMMAND ----------

# ── 5. MERGE into silver ──────────────────────────────────────────────────────

silver_cols = [
    "price_id", "price_date", "instrument_id", "instrument_code", "asset_class",
    "open_price", "high_price", "low_price", "close_price", "prev_close",
    "daily_return_pct", "intraday_range", "volume", "vwap",
    "currency", "price_source", "source_system",
    "_ingest_timestamp", "_pipeline_run_id", "_silver_processed_at", "_row_hash",
]
insert_df = clean_df.select(*silver_cols)

with PipelineTimer(log, "silver_merge"):
    if spark.catalog.tableExists(SILVER_TABLE):
        delta_tbl = DeltaTable.forName(spark, SILVER_TABLE)
        (
            delta_tbl.alias("tgt")
            .merge(insert_df.alias("src"), "tgt.price_id = src.price_id")
            .whenMatchedUpdate(
                condition="tgt._row_hash <> src._row_hash",
                set={col: f"src.{col}" for col in silver_cols}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (insert_df.write.format("delta").mode("overwrite")
                  .option("overwriteSchema", "true")
                  .saveAsTable(SILVER_TABLE))

    rows_written = spark.read.format("delta").table(SILVER_TABLE).filter(
        F.col("price_date") == F.lit(file_date_param).cast("date")
    ).count()

_register_run("SUCCESS", rows_read=rows_read, rows_written=rows_written)
log.info("Pipeline finished", extra={
    "run_id": RUN_ID, "rows_read": rows_read,
    "rows_written": rows_written, "corrupt": corrupt_count,
})

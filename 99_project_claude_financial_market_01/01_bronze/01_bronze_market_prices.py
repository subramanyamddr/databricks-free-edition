# Databricks notebook source
# =============================================================================
# 01_bronze_market_prices.py
# BRONZE LAYER – Market Prices Ingestion
#
# Input    : /Volumes/<catalog>/landing/raw_ingest/market_prices/
#            file_date=YYYY-MM-DD/  *.csv
# Output   : <catalog>.bronze.market_prices  (Delta, append)
# =============================================================================

# COMMAND ----------

# dbutils.notebook.exit("Restart required: Please detach and re-attach the notebook to restart the Python kernel.")

# COMMAND ----------

# %python
# dbutils.widgets.text('env','dev')
# env = dbutils.widgets.get('env')
# env

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

cfg = get_config(env_param)
log = get_logger(
    "bronze.market_prices",
    level=cfg.log_level,
    pipeline="bronze_market_prices",
    env=cfg.env,
)

PIPELINE    = "bronze_market_prices"
TABLE       = cfg.bronze_table("market_prices")
SOURCE_PATH = cfg.landing_path("market_prices")
CHECKPOINT  = cfg.checkpoint_path("bronze_market_prices")
RUN_ID      = str(uuid.uuid4())
STARTED_AT  = datetime.now(timezone.utc)

log.info("Pipeline starting", extra={"run_id": RUN_ID, "table": TABLE})

# COMMAND ----------

CHECKPOINT

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

BRONZE_CSV_SCHEMA = StructType([
    StructField("price_id",       StringType()),
    StructField("price_date",     StringType()),
    StructField("instrument_id",  StringType()),
    StructField("instrument_code",StringType()),
    StructField("asset_class",    StringType()),
    StructField("open_price",     StringType()),
    StructField("high_price",     StringType()),
    StructField("low_price",      StringType()),
    StructField("close_price",    StringType()),
    StructField("prev_close",     StringType()),
    StructField("volume",         StringType()),
    StructField("vwap",           StringType()),
    StructField("currency",       StringType()),
    StructField("price_source",   StringType()),
    StructField("source_system",  StringType()),
    StructField("load_date",      StringType()),
    StructField("file_date",      StringType()),
])

# COMMAND ----------

# DBTITLE 1,Cell 6
def _register_run(status, rows_read=None, rows_written=None, error=None):
    try:
        row = [(RUN_ID, PIPELINE, "bronze", cfg.env, status,
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

CHECKPOINT

# COMMAND ----------

# DBTITLE 1,Cell 7
import pyspark.sql.functions as F

with PipelineTimer(log, "auto_loader_read"):
    raw_df = (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",           "csv")
             .option("cloudFiles.schemaLocation",   CHECKPOINT + "/_schema")
             .option("cloudFiles.inferColumnTypes",  "false")
             .option("header",                       "true")
             .option("mode",                         "PERMISSIVE")
             .option("columnNameOfCorruptRecord",    "_corrupt_record")
             .schema(BRONZE_CSV_SCHEMA)
             .load(SOURCE_PATH)
    )

hash_cols = [F.coalesce(F.col(c), F.lit("")).cast("string") for c in BRONZE_CSV_SCHEMA.fieldNames()]

enriched_df = (
    raw_df
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file",      F.col("_metadata.file_path"))
    .withColumn("_pipeline_run_id",  F.lit(RUN_ID))
    .withColumn("_row_hash",         F.md5(F.concat_ws("|", *hash_cols)))
)

# COMMAND ----------

res = F.concat_ws("|", *hash_cols)
print(hash_cols)
#print(res)

# COMMAND ----------

# DBTITLE 1,Cell 8
TABLE

# COMMAND ----------

with PipelineTimer(log, "bronze_write"):
    query = (
        enriched_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .option("mergeSchema", "false")
        .trigger(availableNow=True)
        .toTable(TABLE)
    )
    query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fin_platform_dev.bronze.market_prices

# COMMAND ----------

file_date_param = spark.conf.get("file_date",datetime.now(timezone.utc).strftime("%Y-%m-%d"))

today_df = spark.read.format("delta").table(TABLE).filter(F.col("file_date") == file_date_param)
rows_written = today_df.count()

dq = (
    DataQualityRunner(
        spark, PIPELINE, "bronze", TABLE,
        halt_on_critical=cfg.enable_data_quality_halt,
    )
    .expect_row_count_gt(0, Severity.CRITICAL)
    .expect_no_nulls("price_id",     Severity.CRITICAL)
    .expect_no_nulls("instrument_id",Severity.CRITICAL)
    .expect_no_nulls("close_price",  Severity.CRITICAL)
    .expect_no_duplicates(["price_id", "file_date"], Severity.CRITICAL)
    .expect_column_values_positive("close_price", Severity.WARNING)
)

with PipelineTimer(log, "dq_checks_bronze"):
    report = dq.run(today_df)

_register_run("SUCCESS", rows_read=rows_written, rows_written=rows_written)
log.info("Pipeline finished", extra={"run_id": RUN_ID, "rows_written": rows_written})

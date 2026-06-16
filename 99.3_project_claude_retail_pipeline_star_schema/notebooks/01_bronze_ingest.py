# Databricks notebook source

# =============================================================================
# notebooks/01_bronze_ingest.py
# Layer    : BRONZE
# Purpose  : Ingest daily sales CSV from ADLS Gen2 -> append to Bronze table
# Source   : abfss://<container>@<account>.dfs.core.windows.net/<source_path>/<process_date>/
# Schedule : Daily (first task in job)
# =============================================================================

# COMMAND ----------
# 0. Widgets — all parameters injected by the Databricks Job
dbutils.widgets.text("env",          "dev", "Environment (dev|qa|prod)")
dbutils.widgets.text("pipeline_run", "",    "Databricks job run ID (auto-set)")
dbutils.widgets.text("process_date", "",    "Business date yyyy-MM-dd (default: yesterday)")

env          = dbutils.widgets.get("env").strip().lower()
pipeline_run = dbutils.widgets.get("pipeline_run").strip() or "manual"
process_date_param = dbutils.widgets.get("process_date").strip()

# COMMAND ----------
# 1. Bootstrap
import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Repos/retail_pipeline")

from utils.config_loader import load_config
from utils.pipeline_logger import PipelineLogger
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="bronze", run_id=pipeline_run)

for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

process_date = process_date_param or str(date.today() - timedelta(days=1))

logger.info("Bronze notebook started", {
    "env": env, "pipeline_run": pipeline_run, "process_date": process_date
})

# COMMAND ----------
# 2. Resolve table and path names
catalog    = config["catalog_name"]
schema     = config["bronze_schema"]
table      = config["bronze_table"]
full_table = f"{catalog}.{schema}.{table}"

adls_account  = config["adls_account"]
src_container = config["source_container"]
source_path   = config["source_path"]

# Folder-per-day layout: <container>/<source_path>/<process_date>/*.csv
process_date_folder = process_date.replace("-", "-")  # keep yyyy-MM-dd folder naming
source_uri = (
    f"abfss://{src_container}@{adls_account}.dfs.core.windows.net/"
    f"{source_path.rstrip('/')}/{process_date_folder}/"
)

logger.info("Resolved source/target", {"source_uri": source_uri, "target_table": full_table})

# COMMAND ----------
# 3. Read CSV with explicit schema (Bronze stays as STRING)
csv_schema = StructType([
    StructField("order_id",         StringType(), True),
    StructField("order_date",       StringType(), True),
    StructField("customer_id",      StringType(), True),
    StructField("customer_name",    StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("customer_city",    StringType(), True),
    StructField("customer_state",   StringType(), True),
    StructField("product_id",       StringType(), True),
    StructField("product_name",     StringType(), True),
    StructField("category",         StringType(), True),
    StructField("sub_category",     StringType(), True),
    StructField("store_id",         StringType(), True),
    StructField("store_name",       StringType(), True),
    StructField("region",           StringType(), True),
    StructField("quantity",         StringType(), True),
    StructField("unit_price",       StringType(), True),
    StructField("discount_pct",     StringType(), True),
])

try:
    df_raw = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(csv_schema)
        .csv(source_uri)
    )
    rows_read = df_raw.count()
    logger.info("CSV read complete", {"rows_read": rows_read, "source_uri": source_uri})

    if rows_read == 0:
        logger.warning("No rows found for process_date — exiting", {"process_date": process_date})
        dbutils.notebook.exit(
            f'{{"status":"no_data","layer":"bronze","process_date":"{process_date}","rows":0}}'
        )

except Exception as exc:
    logger.error("Failed to read CSV from ADLS Gen2", exc=exc)
    raise

# COMMAND ----------
# 4. Add audit columns
df_bronze = (
    df_raw
    .withColumn("_ingested_at",  F.current_timestamp())
    .withColumn("_source_file",  F.input_file_name())
    .withColumn("_ingest_date",  F.lit(process_date).cast("date"))
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

# COMMAND ----------
# 5. Append to Bronze Delta table (partitioned by _ingest_date)
try:
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_table)
    )
    logger.info("Bronze append complete", {"rows_written": rows_read, "table": full_table})
except Exception as exc:
    logger.error("Bronze write failed", exc=exc)
    raise

# COMMAND ----------
# 6. Run summary + flush logs to ADLS Gen2
logger.log_run_summary(
    rows_in=rows_read,
    rows_out=rows_read,
    extra={"table": full_table, "source_uri": source_uri, "process_date": process_date},
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"bronze","process_date":"{process_date}",'
    f'"rows":{rows_read},"table":"{full_table}"}}'
)

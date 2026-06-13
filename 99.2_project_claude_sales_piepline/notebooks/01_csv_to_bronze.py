# =============================================================================
# notebooks/01_csv_to_bronze.py
# Layer    : BRONZE
# Purpose  : Ingest CSV from ADLS Gen2 → write raw rows to Bronze Delta table
# Schedule : Daily (first task in job)
# =============================================================================

# COMMAND ----------
# %pip install databricks-labs-dqx       # install once on cluster or in init script

# COMMAND ----------
# ── 0. Notebook widgets (parameters passed by Databricks Job) ──────────────
dbutils.widgets.text("env",           "dev",  "Environment (dev|qa|prod)")
dbutils.widgets.text("pipeline_run",  "",     "Databricks job run ID (auto-set)")

env          = dbutils.widgets.get("env").strip().lower()
pipeline_run = dbutils.widgets.get("pipeline_run").strip() or "manual"

# COMMAND ----------
# ── 1. Bootstrap ──────────────────────────────────────────────────────────
import sys
sys.path.insert(0, "/Workspace/Repos/sales_pipeline")   # repo root on cluster

from utils.config_loader import load_config
from utils.pipeline_logger import PipelineLogger
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="bronze", run_id=pipeline_run)

# Apply Spark tuning from config
for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

logger.info("Bronze notebook started", {"env": env, "pipeline_run": pipeline_run})

# COMMAND ----------
# ── 2. Resolve table and path names ───────────────────────────────────────
catalog    = config["catalog_name"]
schema     = config["bronze_schema"]
table      = config["bronze_table"]
full_table = f"{catalog}.{schema}.{table}"

adls_account   = config["adls_account"]
src_container  = config["source_container"]
source_path    = config["source_path"]

# abfss URI for the source CSV folder
source_uri = (
    f"abfss://{src_container}@{adls_account}.dfs.core.windows.net/{source_path}"
)

logger.info("Resolved source/target", {"source_uri": source_uri, "target_table": full_table})

# COMMAND ----------
# ── 3. Read CSV with explicit schema (Bronze stays as STRING) ──────────────
csv_schema = StructType([
    StructField("order_id",    StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product",     StringType(), True),
    StructField("quantity",    StringType(), True),
    StructField("unit_price",  StringType(), True),
    StructField("order_date",  StringType(), True),
    StructField("region",      StringType(), True),
])

try:
    df_raw = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")        # don't fail on malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(csv_schema)
        .csv(source_uri)
    )
    rows_read = df_raw.count()
    logger.info(f"CSV read complete", {"rows_read": rows_read, "source_uri": source_uri})

except Exception as exc:
    logger.error("Failed to read CSV from ADLS Gen2", exc=exc)
    raise

# COMMAND ----------
# ── 4. Add audit columns ──────────────────────────────────────────────────
df_bronze = (
    df_raw
    .withColumn("_ingested_at",  F.current_timestamp())
    .withColumn("_source_file",  F.input_file_name())
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

# COMMAND ----------
# ── 5. Append to Bronze Delta table ───────────────────────────────────────
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
# ── 6. Log run summary (also flushes logs to ADLS Gen2) ───────────────────
logger.log_run_summary(
    rows_in=rows_read,
    rows_out=rows_read,
    extra={"table": full_table, "source_uri": source_uri},
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"bronze","rows":{rows_read},"table":"{full_table}"}}'
)

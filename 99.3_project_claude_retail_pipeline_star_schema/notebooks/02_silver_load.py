# =============================================================================
# notebooks/02_silver_load.py
# Layer    : SILVER
# Purpose  : Read Bronze (for process_date) -> type-cast -> DQX validation ->
#            append valid rows to Silver (deduplicated against existing data)
# DQX      : SILVER_CHECKS (nulls, ranges, allowed values) -> quarantine table
# Schedule : Daily (second task in job, depends on bronze_ingest)
# =============================================================================

# COMMAND ----------
# %pip install databricks-labs-dqx

# COMMAND ----------
# 0. Widgets
dbutils.widgets.text("env",          "dev", "Environment (dev|qa|prod)")
dbutils.widgets.text("pipeline_run", "",    "Databricks job run ID")
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
from utils.dq_checks import run_dq_checks, SILVER_CHECKS
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType, DecimalType

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="silver", run_id=pipeline_run)

for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

process_date = process_date_param or str(date.today() - timedelta(days=1))

logger.info("Silver notebook started", {
    "env": env, "pipeline_run": pipeline_run, "process_date": process_date
})

# COMMAND ----------
# 2. Resolve table names
catalog    = config["catalog_name"]
bronze_tbl = f"{catalog}.{config['bronze_schema']}.{config['bronze_table']}"
silver_tbl = f"{catalog}.{config['silver_schema']}.{config['silver_table']}"
quar_tbl   = f"{catalog}.{config['quarantine_schema']}.{config['quarantine_table']}"

logger.info("Resolved tables", {"bronze": bronze_tbl, "silver": silver_tbl, "quarantine": quar_tbl})

# COMMAND ----------
# 3. Read Bronze rows for this process_date
try:
    df_bronze = spark.table(bronze_tbl).filter(F.col("_ingest_date") == F.lit(process_date))
    rows_bronze = df_bronze.count()
    logger.info("Bronze read complete", {"rows_from_bronze": rows_bronze, "process_date": process_date})

    if rows_bronze == 0:
        logger.info("No Bronze rows for process_date — exiting", {"process_date": process_date})
        dbutils.notebook.exit(
            f'{{"status":"no_data","layer":"silver","process_date":"{process_date}","rows_in":0,"rows_out":0}}'
        )
except Exception as exc:
    logger.error("Failed to read Bronze table", exc=exc)
    raise

# COMMAND ----------
# 4. Type-cast and compute derived columns
try:
    df_typed = (
        df_bronze
        .withColumn("order_id",     F.col("order_id").cast(LongType()))
        .withColumn("order_date",   F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("customer_id",  F.trim(F.upper(F.col("customer_id"))))
        .withColumn("product_id",   F.trim(F.upper(F.col("product_id"))))
        .withColumn("store_id",     F.trim(F.upper(F.col("store_id"))))
        .withColumn("quantity",     F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",   F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("discount_pct", F.col("discount_pct").cast(DecimalType(5, 2)))
        .withColumn(
            "gross_amount",
            F.round(F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price"), 2)
        )
        .withColumn(
            "net_amount",
            F.round(
                F.col("quantity").cast(DecimalType(12, 2)) * F.col("unit_price")
                * (F.lit(1) - F.col("discount_pct") / F.lit(100)),
                2
            )
        )
        .withColumn("_updated_at", F.current_timestamp())
        .withColumn("_pipeline_run", F.lit(pipeline_run))
        .dropDuplicates(["order_id"])
        .drop("_source_file", "_ingest_date")
    )
    logger.info("Type casting complete", {"rows_after_dedup": df_typed.count()})
except Exception as exc:
    logger.error("Type casting failed", exc=exc)
    raise

# COMMAND ----------
# 5. DQX validation (Silver checks)
try:
    df_valid, df_quarantine, dq_summary = run_dq_checks(
        spark=spark,
        df=df_typed,
        checks=SILVER_CHECKS,
        layer="silver",
        quarantine_table=quar_tbl,
        config=config,
        logger=logger,
        pipeline_run=pipeline_run,
    )
    rows_valid = df_valid.count()
    rows_quar  = df_quarantine.count()
    logger.info("DQX Silver checks complete", {"valid": rows_valid, "quarantined": rows_quar})
except ValueError as dq_err:
    logger.error("DQX threshold breach — halting Silver", exc=dq_err)
    raise
except Exception as exc:
    logger.error("DQX check execution failed", exc=exc)
    raise

# COMMAND ----------
# 6. Drop DQX bookkeeping columns not part of the Silver schema, then
#    remove order_ids already present in Silver for this date (idempotent reruns)
silver_cols = [f.name for f in spark.table(silver_tbl).schema.fields]
df_to_write = df_valid.select(*[c for c in df_valid.columns if c in silver_cols])

existing_ids = (
    spark.table(silver_tbl)
    .filter(F.col("order_date") == F.lit(process_date))
    .select("order_id")
)

df_to_append = df_to_write.join(existing_ids, on="order_id", how="left_anti")
rows_to_append = df_to_append.count()

logger.info("Deduplicated against existing Silver rows", {
    "rows_valid": rows_valid, "rows_new": rows_to_append,
    "rows_already_present": rows_valid - rows_to_append,
})

# COMMAND ----------
# 7. Append new rows to Silver
try:
    (
        df_to_append.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(silver_tbl)
    )
    logger.info("Silver append complete", {"rows_appended": rows_to_append, "table": silver_tbl})
except Exception as exc:
    logger.error("Silver append failed", exc=exc)
    raise

# COMMAND ----------
# 8. Optional OPTIMIZE
if config.get("optimize_on_write", "false").lower() == "true":
    spark.sql(f"OPTIMIZE {silver_tbl} ZORDER BY (order_date, customer_id)")
    logger.info("Silver OPTIMIZE complete")

# COMMAND ----------
# 9. Run summary + flush logs to ADLS Gen2
logger.log_run_summary(
    rows_in=rows_bronze,
    rows_out=rows_to_append,
    rows_quarantined=rows_quar,
    extra={
        "table": silver_tbl, "process_date": process_date,
        "dq_pass_rate_pct": dq_summary.get("pass_rate_pct"),
    },
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"silver","process_date":"{process_date}",'
    f'"rows_in":{rows_bronze},"rows_out":{rows_to_append},"rows_quarantined":{rows_quar},'
    f'"dq_pass_rate_pct":{dq_summary.get("pass_rate_pct")}}}'
)

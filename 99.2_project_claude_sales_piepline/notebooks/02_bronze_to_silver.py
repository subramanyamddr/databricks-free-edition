# =============================================================================
# notebooks/02_bronze_to_silver.py
# Layer    : SILVER
# Purpose  : Read Bronze -> type-cast -> DQX checks -> MERGE into Silver
# DQX      : Validates nulls, ranges, allowed values before writing
# Schedule : Daily (second task in job, depends on bronze_ingest)
# =============================================================================

# COMMAND ----------
# %pip install databricks-labs-dqx

# COMMAND ----------
# 0. Widgets
dbutils.widgets.text("env",                     "dev", "Environment (dev|qa|prod)")
dbutils.widgets.text("pipeline_run",            "",    "Databricks job run ID")
dbutils.widgets.text("watermark_days_override", "",    "Override watermark days (optional)")

env          = dbutils.widgets.get("env").strip().lower()
pipeline_run = dbutils.widgets.get("pipeline_run").strip() or "manual"
wm_override  = dbutils.widgets.get("watermark_days_override").strip()

# COMMAND ----------
# 1. Bootstrap
import sys
sys.path.insert(0, "/Workspace/Repos/sales_pipeline")

from utils.config_loader import load_config
from utils.pipeline_logger import PipelineLogger
from utils.dq_checks import run_dq_checks, SILVER_CHECKS
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from delta.tables import DeltaTable

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="silver", run_id=pipeline_run)

for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

logger.info("Silver notebook started", {"env": env, "pipeline_run": pipeline_run})

# COMMAND ----------
# 2. Resolve names
catalog     = config["catalog_name"]
bronze_tbl  = f"{catalog}.{config['bronze_schema']}.{config['bronze_table']}"
silver_tbl  = f"{catalog}.{config['silver_schema']}.{config['silver_table']}"
quar_tbl    = f"{catalog}.{config['quarantine_schema']}.{config['quarantine_table']}"

watermark_days = int(wm_override) if wm_override else int(config.get("silver_watermark_days", 1))
logger.info("Resolved tables", {
    "bronze": bronze_tbl, "silver": silver_tbl,
    "quarantine": quar_tbl, "watermark_days": watermark_days
})

# COMMAND ----------
# 3. Read new Bronze rows (watermark on _ingested_at)
try:
    df_bronze = (
        spark.table(bronze_tbl)
        .filter(F.col("_ingested_at") >= F.date_sub(F.current_date(), watermark_days))
    )
    rows_bronze = df_bronze.count()
    logger.info("Bronze read complete", {"rows_from_bronze": rows_bronze})

    if rows_bronze == 0:
        logger.info("No new Bronze rows — Silver notebook exiting early")
        dbutils.notebook.exit(
            '{"status":"no_data","layer":"silver","rows_in":0,"rows_out":0}'
        )
except Exception as exc:
    logger.error("Failed to read Bronze table", exc=exc)
    raise

# COMMAND ----------
# 4. Type-cast and clean
try:
    df_typed = (
        df_bronze
        .withColumn("order_id",    F.col("order_id").cast(IntegerType()))
        .withColumn("quantity",    F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price",  F.col("unit_price").cast(DecimalType(10, 2)))
        .withColumn("order_date",  F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("customer_id", F.trim(F.upper(F.col("customer_id"))))
        .withColumn("region",      F.trim(F.col("region")))
        .withColumn("revenue",
            F.round(
                F.col("quantity").cast(DecimalType(10, 2)) *
                F.col("unit_price").cast(DecimalType(10, 2)),
                2
            )
        )
        .withColumn("_updated_at",   F.current_timestamp())
        .withColumn("_pipeline_run", F.lit(pipeline_run))
        .dropDuplicates(["order_id"])
        .drop("_source_file")
    )
    logger.info("Type casting complete", {"rows_after_dedup": df_typed.count()})
except Exception as exc:
    logger.error("Type casting failed", exc=exc)
    raise

# COMMAND ----------
# 5. DQX data-quality checks
try:
    df_pass, df_quarantine, dq_summary = run_dq_checks(
        spark=spark,
        df=df_typed,
        checks=SILVER_CHECKS,
        layer="silver",
        quarantine_table=quar_tbl,
        config=config,
        logger=logger,
    )
    rows_pass = df_pass.count()
    rows_quar = df_quarantine.count()
    logger.info("DQX Silver checks complete", {"passed": rows_pass, "quarantined": rows_quar})
except ValueError as dq_err:
    logger.error("DQX threshold breach -- halting Silver", exc=dq_err)
    raise
except Exception as exc:
    logger.error("DQX check execution failed", exc=exc)
    raise

# COMMAND ----------
# 6. MERGE (upsert) into Silver
try:
    silver_delta = DeltaTable.forName(spark, silver_tbl)
    (
        silver_delta.alias("target")
        .merge(df_pass.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Silver MERGE complete", {"rows_merged": rows_pass, "table": silver_tbl})
except Exception as exc:
    logger.error("Silver MERGE failed", exc=exc)
    raise

# COMMAND ----------
# 7. OPTIMIZE + ZORDER (optional, config-driven)
if config.get("optimize_on_write", "false").lower() == "true":
    spark.sql(f"OPTIMIZE {silver_tbl} ZORDER BY (order_date, region)")
    logger.info("Silver OPTIMIZE complete")

# COMMAND ----------
# 8. Run summary + flush logs to ADLS Gen2
logger.log_run_summary(
    rows_in=rows_bronze,
    rows_out=rows_pass,
    rows_quarantined=rows_quar,
    extra={"table": silver_tbl, "dq_pass_rate_pct": dq_summary.get("pass_rate_pct")},
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"silver","rows_in":{rows_bronze},'
    f'"rows_out":{rows_pass},"rows_quarantined":{rows_quar},'
    f'"dq_pass_rate_pct":{dq_summary.get("pass_rate_pct")}}}'
)

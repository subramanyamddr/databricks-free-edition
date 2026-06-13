# =============================================================================
# notebooks/03_silver_to_gold.py
# Layer    : GOLD
# Purpose  : Aggregate Silver -> DQX checks -> MERGE into Gold daily summary
# DQX      : Validates aggregated metrics before persisting
# Schedule : Daily (third task in job, depends on silver_clean)
# =============================================================================

# COMMAND ----------
# %pip install databricks-labs-dqx

# COMMAND ----------
# 0. Widgets
dbutils.widgets.text("env",          "dev", "Environment (dev|qa|prod)")
dbutils.widgets.text("pipeline_run", "",    "Databricks job run ID")
dbutils.widgets.text("run_date",     "",    "Process date yyyy-MM-dd (default: yesterday)")

env          = dbutils.widgets.get("env").strip().lower()
pipeline_run = dbutils.widgets.get("pipeline_run").strip() or "manual"
run_date_str = dbutils.widgets.get("run_date").strip()

# COMMAND ----------
# 1. Bootstrap
import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Repos/sales_pipeline")

from utils.config_loader import load_config
from utils.pipeline_logger import PipelineLogger
from utils.dq_checks import run_dq_checks, GOLD_CHECKS
from pyspark.sql import functions as F
from delta.tables import DeltaTable

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="gold", run_id=pipeline_run)

for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

# Resolve processing date
if run_date_str:
    process_date = run_date_str
else:
    process_date = str(date.today() - timedelta(days=1))  # default: yesterday

logger.info("Gold notebook started", {
    "env": env, "pipeline_run": pipeline_run, "process_date": process_date
})

# COMMAND ----------
# 2. Resolve names
catalog    = config["catalog_name"]
silver_tbl = f"{catalog}.{config['silver_schema']}.{config['silver_table']}"
gold_tbl   = f"{catalog}.{config['gold_schema']}.{config['gold_table']}"
quar_tbl   = f"{catalog}.{config['quarantine_schema']}.{config['quarantine_table']}"

logger.info("Resolved tables", {"silver": silver_tbl, "gold": gold_tbl})

# COMMAND ----------
# 3. Read Silver for the processing date
try:
    df_silver = (
        spark.table(silver_tbl)
        .filter(F.col("order_date") == F.lit(process_date))
    )
    rows_silver = df_silver.count()
    logger.info("Silver read complete", {"rows_for_date": rows_silver, "date": process_date})

    if rows_silver == 0:
        logger.info(f"No Silver rows for {process_date} -- Gold notebook exiting early")
        dbutils.notebook.exit(
            f'{{"status":"no_data","layer":"gold","process_date":"{process_date}","rows_in":0}}'
        )
except Exception as exc:
    logger.error("Failed to read Silver table", exc=exc)
    raise

# COMMAND ----------
# 4. Aggregate
try:
    df_gold = (
        df_silver
        .groupBy("order_date", "region", "product")
        .agg(
            F.count("order_id")           .alias("total_orders"),
            F.sum("quantity")             .alias("total_quantity"),
            F.round(F.sum("revenue"), 2)  .alias("total_revenue"),
            F.round(F.avg("revenue"), 2)  .alias("avg_order_value"),
        )
        .withColumnRenamed("order_date", "summary_date")
        .withColumn("_updated_at",   F.current_timestamp())
        .withColumn("_pipeline_run", F.lit(pipeline_run))
    )
    rows_gold = df_gold.count()
    logger.info("Aggregation complete", {"aggregate_rows": rows_gold})
except Exception as exc:
    logger.error("Aggregation failed", exc=exc)
    raise

# COMMAND ----------
# 5. DQX data-quality checks on Gold aggregates
try:
    df_gold_pass, df_gold_quar, dq_summary = run_dq_checks(
        spark=spark,
        df=df_gold,
        checks=GOLD_CHECKS,
        layer="gold",
        quarantine_table=quar_tbl,
        config=config,
        logger=logger,
    )
    rows_pass = df_gold_pass.count()
    rows_quar = df_gold_quar.count()
    logger.info("DQX Gold checks complete", {"passed": rows_pass, "quarantined": rows_quar})
except ValueError as dq_err:
    logger.error("DQX threshold breach -- halting Gold", exc=dq_err)
    raise
except Exception as exc:
    logger.error("DQX Gold check failed", exc=exc)
    raise

# COMMAND ----------
# 6. MERGE into Gold (idempotent: re-running same date is safe)
try:
    gold_delta = DeltaTable.forName(spark, gold_tbl)
    (
        gold_delta.alias("target")
        .merge(
            df_gold_pass.alias("source"),
            """target.summary_date = source.summary_date
               AND target.region   = source.region
               AND target.product  = source.product"""
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("Gold MERGE complete", {"rows_merged": rows_pass, "table": gold_tbl})
except Exception as exc:
    logger.error("Gold MERGE failed", exc=exc)
    raise

# COMMAND ----------
# 7. OPTIMIZE Gold (config-driven)
if config.get("optimize_on_write", "false").lower() == "true":
    spark.sql(f"OPTIMIZE {gold_tbl} ZORDER BY (summary_date, region)")
    logger.info("Gold OPTIMIZE complete")

# COMMAND ----------
# 8. Run summary + flush logs to ADLS Gen2
logger.log_run_summary(
    rows_in=rows_silver,
    rows_out=rows_pass,
    rows_quarantined=rows_quar,
    extra={
        "table": gold_tbl,
        "process_date": process_date,
        "dq_pass_rate_pct": dq_summary.get("pass_rate_pct"),
    },
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"gold","process_date":"{process_date}",'
    f'"rows_in":{rows_silver},"rows_out":{rows_pass},"rows_quarantined":{rows_quar},'
    f'"dq_pass_rate_pct":{dq_summary.get("pass_rate_pct")}}}'
)

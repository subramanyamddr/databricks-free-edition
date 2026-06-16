# Databricks notebook source

# =============================================================================
# notebooks/00_setup_dim_date.py
# ONE-TIME SETUP notebook — populates gold.dim_date for a fixed date range.
# Run manually once per environment after running the SQL DDL scripts.
# Safe to re-run: it overwrites the table (idempotent, static dimension).
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("env", "dev", "Environment (dev|qa|prod)")
env = dbutils.widgets.get("env").strip().lower()

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/retail_pipeline")

from utils.config_loader import load_config
from utils.pipeline_logger import PipelineLogger
from pyspark.sql import functions as F

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="setup_dim_date", run_id="setup")

catalog = config["catalog_name"]
dim_date_tbl = f"{catalog}.{config['gold_schema']}.{config['dim_date_table']}"

start_date = config["dim_date_start"]
end_date = config["dim_date_end"]

logger.info("Generating dim_date", {"start": start_date, "end": end_date, "table": dim_date_tbl})

# COMMAND ----------
# Generate one row per day in the configured range using sequence()
df_dates = (
    spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS full_date
    """)
    .withColumn("date_key",     F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year",         F.year("full_date"))
    .withColumn("quarter",      F.quarter("full_date"))
    .withColumn("month",        F.month("full_date"))
    .withColumn("month_name",   F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week",  ((F.dayofweek("full_date") + 5) % 7) + 1)  # ISO: Mon=1..Sun=7
    .withColumn("day_name",     F.date_format("full_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend",   F.col("day_of_week").isin(6, 7))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "day_of_month", "day_of_week", "day_name", "week_of_year", "is_weekend",
    )
)

row_count = df_dates.count()
logger.info("dim_date rows generated", {"row_count": row_count})

# COMMAND ----------
(
    df_dates.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dim_date_tbl)
)

logger.info("dim_date load complete", {"table": dim_date_tbl, "rows": row_count})
logger.flush()

dbutils.notebook.exit(f'{{"status":"success","table":"{dim_date_tbl}","rows":{row_count}}}')

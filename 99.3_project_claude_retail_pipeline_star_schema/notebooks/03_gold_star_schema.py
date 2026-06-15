# =============================================================================
# notebooks/03_gold_star_schema.py
# Layer    : GOLD (star schema)
# Purpose  : Silver (process_date) -> upsert dimensions (SCD1) -> build
#            fact_sales staging via dimension-key lookups -> DQX validation
#            -> MERGE into fact_sales
# DQX      : GOLD_DIM_* checks on staged dimension rows,
#            GOLD_FACT_CHECKS on the fact staging dataframe
# Schedule : Daily (third task in job, depends on silver_clean)
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
from utils.dq_checks import (
    run_dq_checks,
    GOLD_FACT_CHECKS,
    GOLD_DIM_CUSTOMER_CHECKS,
    GOLD_DIM_PRODUCT_CHECKS,
    GOLD_DIM_STORE_CHECKS,
)
from pyspark.sql import functions as F
from delta.tables import DeltaTable

config = load_config(spark, env, dbutils)
logger = PipelineLogger(spark, config, layer="gold", run_id=pipeline_run)

for k, v in config.get("spark_conf", {}).items():
    spark.conf.set(k, v)

process_date = process_date_param or str(date.today() - timedelta(days=1))

logger.info("Gold notebook started", {
    "env": env, "pipeline_run": pipeline_run, "process_date": process_date
})

# COMMAND ----------
# 2. Resolve table names
catalog    = config["catalog_name"]
silver_tbl = f"{catalog}.{config['silver_schema']}.{config['silver_table']}"
quar_tbl   = f"{catalog}.{config['quarantine_schema']}.{config['quarantine_table']}"

dim_customer_tbl = f"{catalog}.{config['gold_schema']}.{config['dim_customer_table']}"
dim_product_tbl  = f"{catalog}.{config['gold_schema']}.{config['dim_product_table']}"
dim_store_tbl    = f"{catalog}.{config['gold_schema']}.{config['dim_store_table']}"
dim_date_tbl     = f"{catalog}.{config['gold_schema']}.{config['dim_date_table']}"
fact_sales_tbl   = f"{catalog}.{config['gold_schema']}.{config['fact_sales_table']}"

logger.info("Resolved tables", {
    "silver": silver_tbl, "dim_customer": dim_customer_tbl, "dim_product": dim_product_tbl,
    "dim_store": dim_store_tbl, "dim_date": dim_date_tbl, "fact_sales": fact_sales_tbl,
})

# COMMAND ----------
# 3. Read Silver rows for this process_date
try:
    df_silver = spark.table(silver_tbl).filter(F.col("order_date") == F.lit(process_date))
    rows_silver = df_silver.count()
    logger.info("Silver read complete", {"rows_for_date": rows_silver, "process_date": process_date})

    if rows_silver == 0:
        logger.info("No Silver rows for process_date — exiting", {"process_date": process_date})
        dbutils.notebook.exit(
            f'{{"status":"no_data","layer":"gold","process_date":"{process_date}","rows_in":0}}'
        )
except Exception as exc:
    logger.error("Failed to read Silver table", exc=exc)
    raise

# COMMAND ----------
# 4a. Stage dim_customer updates (SCD Type 1 — last value wins)
df_customer_staging = (
    df_silver
    .select("customer_id", "customer_name", "customer_segment", "customer_city", "customer_state")
    .dropDuplicates(["customer_id"])
    .withColumn("_updated_at", F.current_timestamp())
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

df_customer_valid, df_customer_quar, dq_cust_summary = run_dq_checks(
    spark=spark, df=df_customer_staging, checks=GOLD_DIM_CUSTOMER_CHECKS,
    layer="gold_dim_customer", quarantine_table=quar_tbl, config=config,
    logger=logger, pipeline_run=pipeline_run,
)
logger.info("dim_customer staging validated", {"valid": df_customer_valid.count(), "quarantined": df_customer_quar.count()})

# COMMAND ----------
# 4b. Stage dim_product updates (SCD Type 1)
df_product_staging = (
    df_silver
    .select("product_id", "product_name", "category", "sub_category")
    .dropDuplicates(["product_id"])
    .withColumn("_updated_at", F.current_timestamp())
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

df_product_valid, df_product_quar, dq_prod_summary = run_dq_checks(
    spark=spark, df=df_product_staging, checks=GOLD_DIM_PRODUCT_CHECKS,
    layer="gold_dim_product", quarantine_table=quar_tbl, config=config,
    logger=logger, pipeline_run=pipeline_run,
)
logger.info("dim_product staging validated", {"valid": df_product_valid.count(), "quarantined": df_product_quar.count()})

# COMMAND ----------
# 4c. Stage dim_store updates (SCD Type 1)
df_store_staging = (
    df_silver
    .select("store_id", "store_name", "region")
    .dropDuplicates(["store_id"])
    .withColumn("_updated_at", F.current_timestamp())
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

df_store_valid, df_store_quar, dq_store_summary = run_dq_checks(
    spark=spark, df=df_store_staging, checks=GOLD_DIM_STORE_CHECKS,
    layer="gold_dim_store", quarantine_table=quar_tbl, config=config,
    logger=logger, pipeline_run=pipeline_run,
)
logger.info("dim_store staging validated", {"valid": df_store_valid.count(), "quarantined": df_store_quar.count()})

# COMMAND ----------
# 5. MERGE dimension updates — SCD Type 1 (insert new, update changed attrs)
def merge_dimension(target_table: str, source_df, key_col: str, update_cols: list):
    target = DeltaTable.forName(spark, target_table)
    update_set = {c: f"source.{c}" for c in update_cols}
    update_set["_updated_at"] = "source._updated_at"
    update_set["_pipeline_run"] = "source._pipeline_run"

    (
        target.alias("target")
        .merge(source_df.alias("source"), f"target.{key_col} = source.{key_col}")
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values={
            key_col: f"source.{key_col}",
            **update_set,
        })
        .execute()
    )

try:
    merge_dimension(dim_customer_tbl, df_customer_valid, "customer_id",
                    ["customer_name", "customer_segment", "customer_city", "customer_state"])
    merge_dimension(dim_product_tbl, df_product_valid, "product_id",
                    ["product_name", "category", "sub_category"])
    merge_dimension(dim_store_tbl, df_store_valid, "store_id",
                    ["store_name", "region"])
    logger.info("Dimension MERGE complete", {
        "dim_customer_rows": df_customer_valid.count(),
        "dim_product_rows": df_product_valid.count(),
        "dim_store_rows": df_store_valid.count(),
    })
except Exception as exc:
    logger.error("Dimension MERGE failed", exc=exc)
    raise

# COMMAND ----------
# 6. Re-read dimensions (now containing surrogate keys for new rows) and
#    build the fact staging dataframe via lookups on natural keys.
dim_customer = spark.table(dim_customer_tbl).select("customer_key", "customer_id")
dim_product  = spark.table(dim_product_tbl).select("product_key", "product_id")
dim_store    = spark.table(dim_store_tbl).select("store_key", "store_id")

df_fact_staging = (
    df_silver
    .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("int"))
    .join(dim_customer, on="customer_id", how="left")
    .join(dim_product,  on="product_id",  how="left")
    .join(dim_store,    on="store_id",    how="left")
    .select(
        "order_id", "date_key", "customer_key", "product_key", "store_key",
        "quantity", "unit_price", "discount_pct", "gross_amount", "net_amount",
    )
    .withColumn("_updated_at",   F.current_timestamp())
    .withColumn("_pipeline_run", F.lit(pipeline_run))
)

logger.info("Fact staging built", {"rows": df_fact_staging.count()})

# COMMAND ----------
# 7. DQX validation on fact staging (catches failed dimension lookups too —
#    a null *_key means the natural key wasn't found in the dimension)
try:
    df_fact_valid, df_fact_quar, dq_fact_summary = run_dq_checks(
        spark=spark, df=df_fact_staging, checks=GOLD_FACT_CHECKS,
        layer="gold_fact", quarantine_table=quar_tbl, config=config,
        logger=logger, pipeline_run=pipeline_run,
    )
    rows_fact_valid = df_fact_valid.count()
    rows_fact_quar  = df_fact_quar.count()
    logger.info("DQX Gold fact checks complete", {"valid": rows_fact_valid, "quarantined": rows_fact_quar})
except ValueError as dq_err:
    logger.error("DQX threshold breach — halting Gold fact load", exc=dq_err)
    raise
except Exception as exc:
    logger.error("DQX Gold fact check failed", exc=exc)
    raise

# COMMAND ----------
# 8. MERGE into fact_sales (idempotent — keyed on order_id)
try:
    fact_delta = DeltaTable.forName(spark, fact_sales_tbl)
    (
        fact_delta.alias("target")
        .merge(df_fact_valid.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("fact_sales MERGE complete", {"rows_merged": rows_fact_valid, "table": fact_sales_tbl})
except Exception as exc:
    logger.error("fact_sales MERGE failed", exc=exc)
    raise

# COMMAND ----------
# 9. Optional OPTIMIZE
if config.get("optimize_on_write", "false").lower() == "true":
    spark.sql(f"OPTIMIZE {fact_sales_tbl} ZORDER BY (date_key, customer_key)")
    logger.info("fact_sales OPTIMIZE complete")

# COMMAND ----------
# 10. Run summary + flush logs to ADLS Gen2
logger.log_run_summary(
    rows_in=rows_silver,
    rows_out=rows_fact_valid,
    rows_quarantined=rows_fact_quar,
    extra={
        "table": fact_sales_tbl,
        "process_date": process_date,
        "dq_fact_pass_rate_pct": dq_fact_summary.get("pass_rate_pct"),
        "dim_customer_quarantined": df_customer_quar.count(),
        "dim_product_quarantined": df_product_quar.count(),
        "dim_store_quarantined": df_store_quar.count(),
    },
)

dbutils.notebook.exit(
    f'{{"status":"success","layer":"gold","process_date":"{process_date}",'
    f'"rows_in":{rows_silver},"rows_out":{rows_fact_valid},"rows_quarantined":{rows_fact_quar},'
    f'"dq_pass_rate_pct":{dq_fact_summary.get("pass_rate_pct")}}}'
)

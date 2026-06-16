# Databricks notebook source

# =============================================================================
# notebooks/setup_run_ddl.py
# ONE-TIME SETUP notebook — runs all SQL DDL scripts for the target environment.
# Substitutes ${catalog} with the env's catalog_name from config.
# Run manually once per environment (dev / qa / prod) before the daily job.
# =============================================================================

# COMMAND ----------
dbutils.widgets.text("env", "dev", "Environment (dev|qa|prod)")
env = dbutils.widgets.get("env").strip().lower()

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace/Repos/retail_pipeline")

from utils.config_loader import load_config

config = load_config(spark, env, dbutils)
catalog = config["catalog_name"]

print(f"Running DDL setup for env='{env}', catalog='{catalog}'")

# COMMAND ----------
DDL_FILES = [
    "sql/01_create_catalog_schemas.sql",
    "sql/02_create_bronze_tables.sql",
    "sql/03_create_silver_tables.sql",
    "sql/04_create_gold_tables.sql",
]

REPO_ROOT = "/Workspace/Repos/retail_pipeline"

for ddl_file in DDL_FILES:
    path = f"{REPO_ROOT}/{ddl_file}"
    print(f"\n--- Running {ddl_file} ---")
    with open(path) as f:
        sql_text = f.read()

    # Substitute ${catalog} placeholder
    sql_text = sql_text.replace("${catalog}", catalog)

    # Split into statements on semicolons, then strip full-line comments
    # from EACH chunk before checking whether anything executable remains.
    # (A naive "skip chunks starting with --" filter drops every statement,
    # since every CREATE block is preceded by a comment header.)
    for raw_chunk in sql_text.split(";"):
        clean_stmt = "\n".join(
            line for line in raw_chunk.splitlines()
            if not line.strip().startswith("--") and line.strip()
        ).strip()
        if not clean_stmt:
            continue
        print(f"Executing:\n{clean_stmt[:120]}...")
        spark.sql(clean_stmt)

print("\nDDL setup complete for catalog:", catalog)

# COMMAND ----------
dbutils.notebook.exit(f'{{"status":"success","catalog":"{catalog}"}}')

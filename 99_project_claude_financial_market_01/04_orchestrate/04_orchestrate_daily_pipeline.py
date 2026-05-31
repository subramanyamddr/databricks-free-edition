# Databricks notebook source
# =============================================================================
# 04_orchestrate_daily_pipeline.py
# MASTER ORCHESTRATOR – Daily End-to-End Pipeline
#
# Runs all Bronze → Silver → Gold notebooks in dependency order.
# Designed to be the single entry-point Databricks Job task.
#
# Job Parameters (set in Databricks Job UI or via CLI):
#   env       : dev | uat | prod
#   file_date : YYYY-MM-DD  (defaults to today UTC if omitted)
#
# Execution order:
#   1. Bronze  – trade_executions      (parallel)
#   2. Bronze  – market_prices         (parallel)
#   3. Silver  – trade_executions      (after bronze trade)
#   4. Silver  – market_prices         (after bronze prices)
#   5. Gold    – daily_trade_summary   (after silver trade)
#   6. Gold    – portfolio_pnl         (after silver trade + silver prices)
#   7. Gold    – market_snapshot       (after silver prices)
# =============================================================================

# COMMAND ----------

import sys, uuid, concurrent.futures
from datetime import datetime, timezone

sys.path.insert(0, "/Workspace/Users/subramanyamddr03@gmail.com/databricks-free-edition/99_project_claude_financial_market_01/05_utils")

from env_config   import get_config
from logger_utils import get_logger, PipelineTimer

# COMMAND ----------

try:
    env_param = dbutils.widgets.get("env")
except Exception:
    env_param = spark.conf.get("env", "dev")

try:
    file_date_param = dbutils.widgets.get("file_date")
except Exception:
    file_date_param = spark.conf.get("file_date",
                                      datetime.now(timezone.utc).strftime("%Y-%m-%d"))

cfg = get_config(env_param)
log = get_logger(
    "orchestrator",
    level=cfg.log_level,
    pipeline="daily_pipeline",
    env=cfg.env,
)

MASTER_RUN_ID = str(uuid.uuid4())

log.info("Orchestrator starting", extra={
    "master_run_id": MASTER_RUN_ID,
    "env":           cfg.env,
    "file_date":     file_date_param,
})

# COMMAND ----------

# ── Helper to run a child notebook ───────────────────────────────────────────

def run_notebook(path: str, timeout_seconds: int = 3600) -> str:
    """
    Run a Databricks notebook and return its exit value.
    Raises on failure so the orchestrator can catch and fail-fast.
    """
    params = {"env": cfg.env, "file_date": file_date_param}
    log.info("Launching notebook", extra={"notebook": path, "params": params})
    result = dbutils.notebook.run(
        path,
        timeout_seconds,
        params,
    )
    log.info("Notebook completed", extra={"notebook": path, "result": result})
    return result


def run_notebooks_parallel(paths: list[str], timeout: int = 3600):
    """Run multiple notebooks in parallel using ThreadPoolExecutor."""
    errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(paths)) as executor:
        futures = {executor.submit(run_notebook, p, timeout): p for p in paths}
        for future in concurrent.futures.as_completed(futures):
            nb = futures[future]
            try:
                future.result()
            except Exception as exc:
                log.error("Notebook FAILED", extra={"notebook": nb, "error": str(exc)})
                errors.append((nb, str(exc)))
    if errors:
        raise RuntimeError(f"Parallel notebook failures: {errors}")

# COMMAND ----------

# ── Notebook paths (relative to repo root in Databricks Repos) ───────────────

REPO_BASE = "/Workspace/Users/subramanyamddr03@gmail.com/databricks-free-edition/99_project_claude_financial_market_01"

BRONZE_TRADE   = f"{REPO_BASE}/01_bronze/01_bronze_trade_executions"
BRONZE_PRICES  = f"{REPO_BASE}/01_bronze/01_bronze_market_prices"
SILVER_TRADE   = f"{REPO_BASE}/02_silver/02_silver_trade_executions"
SILVER_PRICES  = f"{REPO_BASE}/02_silver/02_silver_market_prices"
GOLD_SUMMARY   = f"{REPO_BASE}/03_gold/03_gold_daily_trade_summary"
GOLD_PNL       = f"{REPO_BASE}/03_gold/03_gold_portfolio_pnl"
GOLD_SNAPSHOT  = f"{REPO_BASE}/03_gold/03_gold_instrument_market_snapshot"

# COMMAND ----------

# ── Stage 1: Bronze (parallel) ───────────────────────────────────────────────

with PipelineTimer(log, "stage_1_bronze"):
    log.info("Stage 1: Bronze ingestion (parallel)")
    run_notebooks_parallel([BRONZE_TRADE, BRONZE_PRICES])

# COMMAND ----------

# ── Stage 2: Silver (parallel – each depends on its own bronze) ──────────────

with PipelineTimer(log, "stage_2_silver"):
    log.info("Stage 2: Silver cleanse (parallel)")
    run_notebooks_parallel([SILVER_TRADE, SILVER_PRICES])

# COMMAND ----------

# ── Stage 3: Gold (parallel – all silver tables ready) ───────────────────────

with PipelineTimer(log, "stage_3_gold"):
    log.info("Stage 3: Gold aggregation (parallel)")
    run_notebooks_parallel([GOLD_SUMMARY, GOLD_PNL, GOLD_SNAPSHOT])

# COMMAND ----------

log.info("All pipeline stages completed successfully", extra={
    "master_run_id": MASTER_RUN_ID,
    "env":           cfg.env,
    "file_date":     file_date_param,
})

dbutils.notebook.exit("SUCCESS")

# =============================================================================
# utils/dq_checks.py
# Databricks Labs DQX check definitions + a thin wrapper around DQEngine.
#
# DQX reference: https://databrickslabs.github.io/dqx/
# Checks are declared as metadata dicts and applied with
# DQEngine.apply_checks_by_metadata_and_split(), which returns
# (valid_df, quarantine_df). DQX adds `_errors` and `_warnings` MAP columns
# to the quarantine_df describing which checks failed and why.
# =============================================================================

from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# SILVER layer checks — applied to sales_cleaned before MERGE
# ---------------------------------------------------------------------------
SILVER_CHECKS: List[Dict[str, Any]] = [
    {
        "name": "order_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "order_id"}},
    },
    {
        "name": "order_date_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "order_date"}},
    },
    {
        "name": "customer_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}},
    },
    {
        "name": "product_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "product_id"}},
    },
    {
        "name": "store_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "store_id"}},
    },
    {
        "name": "quantity_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "quantity"}},
    },
    {
        "name": "quantity_in_valid_range",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "quantity", "min_limit": 1, "max_limit": 10000},
        },
    },
    {
        "name": "unit_price_in_valid_range",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "unit_price", "min_limit": 0.01, "max_limit": 999999},
        },
    },
    {
        "name": "discount_pct_in_valid_range",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "discount_pct", "min_limit": 0, "max_limit": 100},
        },
    },
    {
        "name": "customer_segment_is_valid",
        "criticality": "warn",
        "check": {
            "function": "is_in_list",
            "arguments": {
                "column": "customer_segment",
                "allowed": ["Consumer", "Corporate", "Home Office"],
            },
        },
    },
    {
        "name": "order_date_not_in_future",
        "criticality": "warn",
        "check": {
            "function": "is_not_greater_than",
            "arguments": {"column": "order_date", "limit": "current_date()"},
        },
    },
]


# ---------------------------------------------------------------------------
# GOLD fact_sales checks — applied after dimension-key lookups, before MERGE
# ---------------------------------------------------------------------------
GOLD_FACT_CHECKS: List[Dict[str, Any]] = [
    {
        "name": "date_key_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "date_key"}},
    },
    {
        "name": "customer_key_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "customer_key"}},
    },
    {
        "name": "product_key_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "product_key"}},
    },
    {
        "name": "store_key_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "store_key"}},
    },
    {
        "name": "quantity_positive",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "quantity", "min_limit": 1, "max_limit": 10000},
        },
    },
    {
        "name": "net_amount_non_negative",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "net_amount", "min_limit": 0, "max_limit": 99999999},
        },
    },
    {
        "name": "gross_amount_non_negative",
        "criticality": "warn",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "gross_amount", "min_limit": 0, "max_limit": 99999999},
        },
    },
]


# ---------------------------------------------------------------------------
# GOLD dimension checks — applied to staged dimension records before MERGE
# ---------------------------------------------------------------------------
GOLD_DIM_CUSTOMER_CHECKS: List[Dict[str, Any]] = [
    {
        "name": "customer_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "customer_id"}},
    },
    {
        "name": "customer_id_is_unique",
        "criticality": "error",
        "check": {"function": "is_unique", "arguments": {"columns": ["customer_id"]}},
    },
]

GOLD_DIM_PRODUCT_CHECKS: List[Dict[str, Any]] = [
    {
        "name": "product_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "product_id"}},
    },
    {
        "name": "product_id_is_unique",
        "criticality": "error",
        "check": {"function": "is_unique", "arguments": {"columns": ["product_id"]}},
    },
]

GOLD_DIM_STORE_CHECKS: List[Dict[str, Any]] = [
    {
        "name": "store_id_is_not_null",
        "criticality": "error",
        "check": {"function": "is_not_null", "arguments": {"column": "store_id"}},
    },
    {
        "name": "store_id_is_unique",
        "criticality": "error",
        "check": {"function": "is_unique", "arguments": {"columns": ["store_id"]}},
    },
]


# ---------------------------------------------------------------------------
# DQX runner
# ---------------------------------------------------------------------------

def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    checks: List[Dict[str, Any]],
    layer: str,
    quarantine_table: str,
    config: Dict[str, Any],
    logger,
    pipeline_run: str = "manual",
) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    """
    Apply DQX checks to `df`, split into (valid_df, quarantine_df), persist
    quarantined rows, log a summary, and enforce the configured minimum
    pass-rate threshold.

    Returns
    -------
    valid_df       : rows with no `error`-criticality failures (DQX's "good"
                      dataframe; result columns dropped). Rows that only
                      triggered `warn`-criticality checks ARE included here.
    quarantine_df  : rows with at least one `error`-criticality failure,
                      carrying the `_errors` / `_warnings` result columns.
                      This is the set persisted to `quarantine_table`.
    dq_summary     : dict with pass-rate / failure stats for logging
    """
    total = df.count()

    try:
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
    except ImportError:
        logger.warning(
            "databricks-labs-dqx not installed — skipping DQ checks. "
            "Install via the cluster init script (init/install_dqx.sh)."
        )
        return df, spark.createDataFrame([], df.schema), {
            "skipped": True, "layer": layer, "total_rows": total,
            "passed_rows": total, "quarantined_rows": 0, "pass_rate_pct": 100.0,
        }

    dq_engine = DQEngine(WorkspaceClient())

    # apply_checks_by_metadata_and_split -> (good_df, bad_df)
    #   good_df = rows with _errors IS NULL (passed all `error` checks; DQX
    #             drops the _errors/_warnings result columns from this df).
    #             May still include rows that triggered `warn` checks.
    #   bad_df  = rows with _errors IS NOT NULL OR _warnings IS NOT NULL
    #             (i.e. ANY check fired — errors AND warn-only rows both
    #             land here, and warn-only rows ALSO appear in good_df).
    #
    # "Quarantined" (excluded from the downstream layer) means _errors IS
    # NOT NULL specifically — warn-only rows pass through in good_df.
    valid_df, bad_df = dq_engine.apply_checks_by_metadata_and_split(df, checks)

    if "_errors" in bad_df.columns:
        quarantine_df = bad_df.filter(F.col("_errors").isNotNull())
    else:
        quarantine_df = bad_df

    rows_valid = valid_df.count()
    rows_quarantined = quarantine_df.count()

    # Persist only true error-check failures to the quarantine table
    _write_quarantine(quarantine_df, quarantine_table, layer, pipeline_run, config)

    dq_summary = {
        "layer": layer,
        "total_rows": total,
        "passed_rows": rows_valid,
        "quarantined_rows": rows_quarantined,
        "pass_rate_pct": round((rows_valid / total * 100) if total else 100.0, 2),
        "checks_run": len(checks),
        "error_checks": len([c for c in checks if c.get("criticality") == "error"]),
        "warn_checks": len([c for c in checks if c.get("criticality") == "warn"]),
    }
    logger.log_dq_result(dq_summary)

    min_pass_rate = float(config.get("dq_min_pass_rate_pct", 80.0))
    if dq_summary["pass_rate_pct"] < min_pass_rate and total > 0:
        raise ValueError(
            f"[DQX] {layer} pass rate {dq_summary['pass_rate_pct']}% is below "
            f"minimum threshold {min_pass_rate}%. Pipeline halted. "
            f"Inspect quarantine table: {quarantine_table}"
        )

    return valid_df, quarantine_df, dq_summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _write_quarantine(
    df_quarantine: DataFrame,
    quarantine_table: str,
    layer: str,
    pipeline_run: str,
    config: Dict[str, Any],
):
    """Append quarantined rows (with DQX _errors/_warnings) to the quarantine table."""
    if df_quarantine.rdd.isEmpty():
        return

    df_out = df_quarantine

    # DQX represents _errors / _warnings as MAP<STRING,STRING>; cast to
    # JSON-ish STRING so they fit the fixed quarantine schema.
    for col_name in ("_errors", "_warnings"):
        if col_name in df_out.columns:
            df_out = df_out.withColumn(col_name, F.to_json(F.col(col_name)))
        else:
            df_out = df_out.withColumn(col_name, F.lit(None).cast("string"))

    df_out = (
        df_out
        .withColumn("_quarantine_layer", F.lit(layer))
        .withColumn("_quarantine_ts", F.current_timestamp())
        .withColumn("_pipeline_run", F.lit(pipeline_run))
    )

    (
        df_out.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table)
    )

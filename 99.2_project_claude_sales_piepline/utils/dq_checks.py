# =============================================================================
# utils/dq_checks.py
# Centralised DQX (Databricks Data Quality Framework) check definitions
# and quarantine/pass-through split logic
# =============================================================================

from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Silver-layer DQX check definitions
# ---------------------------------------------------------------------------

SILVER_CHECKS = [
    # ── Null checks ────────────────────────────────────────────────────────
    {
        "name": "order_id_not_null",
        "criticality": "error",          # "error" = quarantine row; "warn" = flag only
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "order_id"},
        },
    },
    {
        "name": "customer_id_not_null",
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "customer_id"},
        },
    },
    {
        "name": "order_date_not_null",
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "order_date"},
        },
    },
    # ── Range checks ───────────────────────────────────────────────────────
    {
        "name": "quantity_positive",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "quantity", "min_value": 1, "max_value": 10000},
        },
    },
    {
        "name": "unit_price_positive",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "unit_price", "min_value": 0.01, "max_value": 999999},
        },
    },
    # ── Allowed-values check ───────────────────────────────────────────────
    {
        "name": "region_valid",
        "criticality": "warn",           # warn only — don't quarantine
        "check": {
            "function": "is_in_list",
            "arguments": {
                "column": "region",
                "values": ["North", "South", "East", "West", "Central"],
            },
        },
    },
    # ── Date freshness ─────────────────────────────────────────────────────
    {
        "name": "order_date_not_future",
        "criticality": "warn",
        "check": {
            "function": "is_not_greater_than",
            "arguments": {"column": "order_date", "value": "current_date()"},
        },
    },
]


# ---------------------------------------------------------------------------
# Gold-layer DQX check definitions
# ---------------------------------------------------------------------------

GOLD_CHECKS = [
    {
        "name": "total_orders_positive",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "total_orders", "min_value": 1, "max_value": 1000000},
        },
    },
    {
        "name": "total_revenue_non_negative",
        "criticality": "error",
        "check": {
            "function": "is_in_range",
            "arguments": {"column": "total_revenue", "min_value": 0, "max_value": 999999999},
        },
    },
    {
        "name": "summary_date_not_null",
        "criticality": "error",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "summary_date"},
        },
    },
    {
        "name": "avg_order_value_not_null",
        "criticality": "warn",
        "check": {
            "function": "is_not_null",
            "arguments": {"column": "avg_order_value"},
        },
    },
]


# ---------------------------------------------------------------------------
# DQX runner (wraps databricks-dqx library)
# ---------------------------------------------------------------------------

def run_dq_checks(
    spark: SparkSession,
    df: DataFrame,
    checks: List[Dict[str, Any]],
    layer: str,
    quarantine_table: str,
    config: Dict[str, Any],
    logger,
) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    """
    Run DQX checks against a DataFrame.

    Returns
    -------
    df_pass        : rows that passed all ERROR-level checks
    df_quarantine  : rows that failed at least one ERROR-level check
    dq_summary     : dict with check stats (for logging / monitoring)
    """
    try:
        from databricks.labs.dqx.engine import DQEngine          # noqa: E402
        from databricks.labs.dqx.col_functions import (          # noqa: E402
            is_not_null, is_in_range, is_in_list, is_not_greater_than,
        )
    except ImportError:
        logger.warning(
            "databricks-dqx not installed — skipping DQ checks. "
            "Install via: %pip install databricks-labs-dqx"
        )
        return df, spark.createDataFrame([], df.schema), {"skipped": True}

    dqe = DQEngine(spark)

    # Build check objects from the declarative dict definitions
    check_objects = _build_check_objects(checks)

    # Apply checks — DQX adds a _dq_* column per check
    df_checked = dqe.apply_checks(df, check_objects)

    # Split pass vs quarantine based on ERROR-level failures
    error_check_names = [
        c["name"] for c in checks if c.get("criticality") == "error"
    ]
    fail_condition = " OR ".join(
        [f"_dq_{name} IS NOT NULL" for name in error_check_names]
    ) if error_check_names else "1=0"

    df_pass = df_checked.filter(f"NOT ({fail_condition})")
    df_quarantine = df_checked.filter(fail_condition)

    # Persist quarantine rows to Delta table for investigation
    _write_quarantine(df_quarantine, quarantine_table, layer, config)

    # Build summary dict for logging
    total = df.count()
    failed = df_quarantine.count()
    passed = total - failed

    dq_summary = {
        "layer": layer,
        "total_rows": total,
        "passed_rows": passed,
        "quarantined_rows": failed,
        "pass_rate_pct": round((passed / total * 100) if total else 0, 2),
        "checks_run": len(checks),
        "error_checks": len(error_check_names),
        "warn_checks": len(checks) - len(error_check_names),
        "check_results": _collect_check_stats(df_checked, checks),
    }

    logger.log_dq_result(dq_summary)

    # Raise if pass rate is below minimum threshold
    min_pass_rate = float(config.get("dq_min_pass_rate_pct", 80.0))
    if dq_summary["pass_rate_pct"] < min_pass_rate and total > 0:
        raise ValueError(
            f"[DQ] {layer} pass rate {dq_summary['pass_rate_pct']}% "
            f"is below minimum threshold {min_pass_rate}%. "
            f"Pipeline halted. Check quarantine table: {quarantine_table}"
        )

    return df_pass, df_quarantine, dq_summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_check_objects(checks: List[Dict]) -> List:
    """Convert declarative check dicts to DQX Check objects."""
    from databricks.labs.dqx.col_functions import (
        is_not_null, is_in_range, is_in_list, is_not_greater_than,
    )
    from databricks.labs.dqx.engine import DQRowRule

    fn_map = {
        "is_not_null": is_not_null,
        "is_in_range": is_in_range,
        "is_in_list": is_in_list,
        "is_not_greater_than": is_not_greater_than,
    }

    result = []
    for c in checks:
        fn_name = c["check"]["function"]
        args = c["check"]["arguments"]
        fn = fn_map.get(fn_name)
        if fn is None:
            raise ValueError(f"Unknown DQX check function: {fn_name}")
        result.append(
            DQRowRule(
                name=c["name"],
                criticality=c.get("criticality", "error"),
                check=fn(**args),
            )
        )
    return result


def _write_quarantine(
    df_quarantine: DataFrame,
    quarantine_table: str,
    layer: str,
    config: Dict[str, Any],
):
    """Append quarantined rows to the quarantine Delta table."""
    if df_quarantine.rdd.isEmpty():
        return
    (
        df_quarantine
        .withColumn("_quarantine_layer", F.lit(layer))
        .withColumn("_quarantine_ts", F.current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(quarantine_table)
    )


def _collect_check_stats(df_checked: DataFrame, checks: List[Dict]) -> List[Dict]:
    """Count failures per individual check."""
    stats = []
    for c in checks:
        col_name = f"_dq_{c['name']}"
        if col_name in df_checked.columns:
            fail_count = df_checked.filter(F.col(col_name).isNotNull()).count()
            stats.append(
                {
                    "check": c["name"],
                    "criticality": c.get("criticality", "error"),
                    "failures": fail_count,
                }
            )
    return stats

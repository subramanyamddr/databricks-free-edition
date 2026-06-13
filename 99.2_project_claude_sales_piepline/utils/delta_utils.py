"""
utils/delta_utils.py
---------------------
Shared Delta / Spark utilities used across all pipeline notebooks.

Includes:
  - retry decorator with exponential back-off
  - row-level data quality check (DQ) framework
  - schema drift detection helper
  - ADLS Gen2 mount / access helper
"""

import functools
import time
from typing import Callable, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# ── Retry decorator ───────────────────────────────────────────
def with_retry(max_attempts: int = 3, backoff_seconds: float = 5.0):
    """
    Decorator: retry a function on any Exception with exponential back-off.

    Usage:
        @with_retry(max_attempts=3, backoff_seconds=5)
        def my_write(...): ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    wait = backoff_seconds * (2 ** (attempt - 1))
                    print(
                        f"⚠️  [{func.__name__}] attempt {attempt}/{max_attempts} failed: {exc}. "
                        f"Retrying in {wait:.0f}s…"
                    )
                    if attempt < max_attempts:
                        time.sleep(wait)
            raise RuntimeError(
                f"[{func.__name__}] failed after {max_attempts} attempts"
            ) from last_exc
        return wrapper
    return decorator


# ── Data quality framework ────────────────────────────────────
class DQResult:
    def __init__(self):
        self.checks: List[dict] = []

    def add(self, name: str, passed: bool, failed_count: int, total: int, note: str = ""):
        pct = round(100 * (total - failed_count) / max(total, 1), 2)
        self.checks.append({
            "check":         name,
            "passed":        passed,
            "failed_rows":   failed_count,
            "total_rows":    total,
            "pass_pct":      pct,
            "note":          note,
        })

    @property
    def all_passed(self) -> bool:
        return all(c["passed"] for c in self.checks)

    def summary(self) -> str:
        lines = ["──────────────────── DQ Report ────────────────────"]
        for c in self.checks:
            icon = "✅" if c["passed"] else "❌"
            lines.append(
                f"  {icon}  {c['check']:<40} "
                f"pass={c['pass_pct']}%  failed_rows={c['failed_rows']:,}"
                + (f"  ({c['note']})" if c["note"] else "")
            )
        lines.append("───────────────────────────────────────────────────")
        return "\n".join(lines)


def run_dq_checks(
    df: DataFrame,
    checks: List[Tuple[str, str, float]],
    logger=None,
    layer: str = "dq",
) -> Tuple[DataFrame, DQResult]:
    """
    Run data quality checks on a DataFrame.

    checks: list of (check_name, spark_filter_expr_for_BAD_rows, max_fail_pct)

    Returns:
        - df_clean : rows that passed ALL checks
        - dq_result: DQResult with per-check stats

    Raises:
        ValueError if any check exceeds its max_fail_pct threshold.
    """
    result = DQResult()
    total  = df.count()
    df_clean = df

    for name, bad_expr, max_fail_pct in checks:
        failed_count = df.filter(bad_expr).count()
        passed = (failed_count / max(total, 1) * 100) <= max_fail_pct
        result.add(name, passed, failed_count, total)

        if logger:
            level = "info" if passed else "error"
            getattr(logger, level)(
                layer, f"DQ [{name}]",
                failed_rows=failed_count,
                total=total,
                threshold_pct=max_fail_pct,
            )

    print(result.summary())

    if not result.all_passed:
        failed = [c["check"] for c in result.checks if not c["passed"]]
        raise ValueError(f"DQ checks FAILED: {failed}. Aborting pipeline step.")

    # Remove bad rows across all checks combined
    all_bad_exprs = " OR ".join(f"({expr})" for _, expr, _ in checks)
    df_clean = df.filter(f"NOT ({all_bad_exprs})")
    return df_clean, result


# ── Schema drift detection ────────────────────────────────────
def check_schema_drift(
    df_incoming: DataFrame,
    expected_schema: StructType,
    strict: bool = False,
) -> List[str]:
    """
    Compare incoming DataFrame schema to expected.

    Returns list of drift messages (empty = no drift).
    If strict=True, raises on any drift.
    """
    incoming_fields = {f.name.lower(): f.dataType for f in df_incoming.schema}
    expected_fields = {f.name.lower(): f.dataType for f in expected_schema}

    drifts = []

    for col_name, dtype in expected_fields.items():
        if col_name not in incoming_fields:
            drifts.append(f"MISSING column: '{col_name}' (expected {dtype})")
        elif incoming_fields[col_name] != dtype:
            drifts.append(
                f"TYPE MISMATCH: '{col_name}' expected {dtype}, "
                f"got {incoming_fields[col_name]}"
            )

    for col_name in incoming_fields:
        if col_name not in expected_fields:
            drifts.append(f"EXTRA column: '{col_name}' (not in expected schema)")

    if drifts:
        msg = "\n".join(f"  ⚠️  {d}" for d in drifts)
        print(f"Schema drift detected:\n{msg}")
        if strict:
            raise ValueError(f"Schema drift check failed:\n{msg}")

    return drifts


# ── ADLS Gen2 Spark config helper ────────────────────────────
def configure_adls(spark: SparkSession, adls_account: str, env: str):
    """
    Set Spark ADLS Gen2 OAuth config using a Service Principal stored
    in Databricks secret scope 'adls-secrets'.

    Secret scope keys expected:
        adls-secrets / sp-client-id
        adls-secrets / sp-client-secret
        adls-secrets / sp-tenant-id
    """
    try:
        client_id     = dbutils.secrets.get("adls-secrets", "sp-client-id")     # noqa
        client_secret = dbutils.secrets.get("adls-secrets", "sp-client-secret") # noqa
        tenant_id     = dbutils.secrets.get("adls-secrets", "sp-tenant-id")     # noqa

        prefix = f"fs.azure.account"
        spark.conf.set(f"{prefix}.auth.type.{adls_account}.dfs.core.windows.net",
                       "OAuth")
        spark.conf.set(f"{prefix}.oauth.provider.type.{adls_account}.dfs.core.windows.net",
                       "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"{prefix}.oauth2.client.id.{adls_account}.dfs.core.windows.net",
                       client_id)
        spark.conf.set(f"{prefix}.oauth2.client.secret.{adls_account}.dfs.core.windows.net",
                       client_secret)
        spark.conf.set(f"{prefix}.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
                       f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

        print(f"✅  ADLS Gen2 OAuth configured for account: {adls_account} (env={env})")

    except NameError:
        # dbutils not available (local unit tests / CI)
        print("⚠️  dbutils not found — ADLS config skipped (local/test mode)")
    except Exception as exc:
        raise RuntimeError(f"Failed to configure ADLS Gen2 access: {exc}") from exc

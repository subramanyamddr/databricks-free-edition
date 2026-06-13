# =============================================================================
# utils/config_loader.py
# Environment-independent config loader
# Merges: base config → env override → notebook widget params
# =============================================================================

import json
from typing import Any, Dict

from pyspark.sql import SparkSession


def load_config(spark: SparkSession, env: str, dbutils=None) -> Dict[str, Any]:
    """
    Load pipeline configuration for the given environment.

    Resolution order (last wins):
      1. Base config  — conf/base.json
      2. Env override — conf/{env}.json
      3. Widget params passed as notebook arguments

    Parameters
    ----------
    spark   : active SparkSession
    env     : 'dev' | 'qa' | 'prod'
    dbutils : Databricks dbutils (passed in from notebook)

    Returns
    -------
    Merged config dict
    """
    env = env.lower()

    base = _read_json(spark, f"conf/base.json")
    override = _read_json(spark, f"conf/{env}.json")

    config = {**base, **override, "env": env}

    # Overlay any notebook widget values
    if dbutils:
        for key in config.keys():
            try:
                widget_val = dbutils.widgets.get(key)
                if widget_val and widget_val.strip():
                    config[key] = widget_val.strip()
            except Exception:
                pass

    _validate(config, env)
    return config


# ── Helpers ────────────────────────────────────────────────────────────────

def _read_json(spark: SparkSession, path: str) -> Dict[str, Any]:
    """Read a JSON config file from the repo (mounted or workspace-relative)."""
    import os

    # When running via Databricks Repos the CWD is the repo root
    repo_root = os.environ.get("REPO_ROOT", "/Workspace/Repos")
    abs_path = os.path.join(repo_root, "sales_pipeline", path)

    try:
        with open(abs_path) as f:
            return json.load(f)
    except FileNotFoundError:
        # Fallback: try DBFS
        try:
            rdd = spark.sparkContext.textFile(f"dbfs:/sales_pipeline/{path}")
            return json.loads("\n".join(rdd.collect()))
        except Exception:
            return {}


def _validate(config: Dict[str, Any], env: str):
    required = [
        "adls_account",
        "catalog_name",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "source_container",
        "source_path",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(
            f"[ConfigLoader] Missing required config keys for env='{env}': {missing}"
        )

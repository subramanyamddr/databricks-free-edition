# =============================================================================
# utils/config_loader.py
# Environment-independent config loader.
# Resolution order (last wins):
#   1. conf/base.json   (shared defaults)
#   2. conf/{env}.json  (environment overrides)
#   3. Notebook widget params (highest priority)
# =============================================================================

import json
import os
from typing import Any, Dict


def load_config(spark, env: str, dbutils=None) -> Dict[str, Any]:
    env = env.lower()

    base = _read_json("conf/base.json")
    override = _read_json(f"conf/{env}.json")

    config = {**base, **override, "env": env}

    if dbutils:
        for key in list(config.keys()):
            try:
                widget_val = dbutils.widgets.get(key)
                if widget_val and widget_val.strip():
                    config[key] = widget_val.strip()
            except Exception:
                pass

    _validate(config, env)
    return config


def _read_json(relative_path: str) -> Dict[str, Any]:
    """Read a JSON config file relative to the repo root."""
    repo_root = os.environ.get("REPO_ROOT", "/Workspace/Repos/retail_pipeline")
    abs_path = os.path.join(repo_root, relative_path)
    try:
        with open(abs_path) as f:
            return json.load(f)
    except FileNotFoundError:
        # Fallback: relative to current working directory (local/tests)
        try:
            with open(relative_path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


def _validate(config: Dict[str, Any], env: str):
    required = [
        "adls_account",
        "catalog_name",
        "bronze_schema",
        "silver_schema",
        "gold_schema",
        "quarantine_schema",
        "source_container",
        "source_path",
    ]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(
            f"[ConfigLoader] Missing required config keys for env='{env}': {missing}"
        )

# =============================================================================
# env_config.py  –  Environment & Catalog Configuration
# =============================================================================
# Resolves catalog / schema / volume paths based on the current Databricks
# environment tag (dev | uat | prod).  Import this in every notebook.
#
# Usage:
#   from env_config import get_config
#   cfg = get_config()          # auto-detects env from Databricks job param
#   cfg = get_config("uat")     # override for local testing
# =============================================================================

from __future__ import annotations
import logging
import os
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("env_config")


# ---------------------------------------------------------------------------
# Dataclass – one instance per environment
# ---------------------------------------------------------------------------

@dataclass
class EnvironmentConfig:
    env: str                        # dev | uat | prod

    # Unity Catalog
    catalog: str = field(init=False)
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str   = "gold"

    # Volume roots  (Unity Catalog external / managed volumes)
    volume_root: str = field(init=False)

    # Job / cluster settings
    log_level: str = field(init=False)
    enable_data_quality_halt: bool = field(init=False)   # halt pipeline on DQ failure in prod
    checkpoint_base: str = field(init=False)

    def __post_init__(self):
        env = self.env.lower()
        if env not in ("dev", "uat", "prod"):
            raise ValueError(f"Invalid environment '{env}'. Must be dev | uat | prod.")

        self.catalog = f"fin_platform_{env}"
        self.volume_root = f"/Volumes/{self.catalog}/landing/raw_ingest"
        self.checkpoint_base = f"/Volumes/{self.catalog}/landing/checkpoints"

        self.log_level = "DEBUG" if env == "dev" else "INFO"
        self.enable_data_quality_halt = env == "prod"

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def bronze_table(self, name: str) -> str:
        return f"{self.catalog}.{self.bronze_schema}.{name}"

    def silver_table(self, name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{name}"

    def gold_table(self, name: str) -> str:
        return f"{self.catalog}.{self.gold_schema}.{name}"

    def checkpoint_path(self, stream_name: str) -> str:
        return f"{self.checkpoint_base}/{stream_name}"

    def landing_path(self, subfolder: str) -> str:
        return f"{self.volume_root}/{subfolder}"

    def __repr__(self) -> str:
        return (
            f"EnvironmentConfig(env={self.env}, catalog={self.catalog}, "
            f"volume_root={self.volume_root})"
        )


# ---------------------------------------------------------------------------
# Factory – resolve env from multiple sources in priority order
# ---------------------------------------------------------------------------

def get_config(env_override: Optional[str] = None) -> EnvironmentConfig:
    """
    Resolve environment in priority order:
      1. explicit env_override argument
      2. Databricks job / widget parameter  'env'
      3. OS environment variable            ENV
      4. default → 'dev'
    """
    env = env_override

    if env is None:
        # Try Databricks widget (works in interactive & job contexts)
        try:
            from pyspark.sql import SparkSession          # noqa: PLC0415
            spark = SparkSession.getActiveSession()
            if spark:
                env = spark.conf.get("env", None)        # set via --conf in job
        except Exception:
            pass

    if env is None:
        env = os.environ.get("ENV", "dev")

    cfg = EnvironmentConfig(env=env.strip().lower())
    logger.info("Resolved environment config: %s", cfg)
    return cfg

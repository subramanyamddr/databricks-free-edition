"""
utils/config.py
---------------
Environment-independent configuration loader.

Resolution order (highest → lowest priority):
  1. Databricks Widget parameters   (interactive / job parameter override)
  2. Environment variables          (CI / local testing)
  3. Hard-coded defaults            (safe fallbacks)

Usage in any notebook:
    from utils.config import PipelineConfig
    cfg = PipelineConfig.load(spark)
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PipelineConfig:
    # ── Catalog & storage ─────────────────────────────────────
    catalog_name: str = "my_catalog"
    env: str = "dev"                          # dev | qa | prod

    # ADLS Gen2 — abfss://<container>@<account>.dfs.core.windows.net
    adls_account: str = "mystorageaccount"
    adls_container: str = "datalake"
    adls_base_path: str = ""                  # computed in __post_init__

    # ── Source ────────────────────────────────────────────────
    source_path: str = ""                     # computed in __post_init__
    source_format: str = "csv"
    source_has_header: bool = True

    # ── Log path (ADLS Gen2) ──────────────────────────────────
    log_base_path: str = ""                   # computed in __post_init__

    # ── Delta options ─────────────────────────────────────────
    bronze_table: str = ""                    # computed
    silver_table: str = ""                    # computed
    gold_table: str = ""                      # computed

    # ── Watermark ─────────────────────────────────────────────
    watermark_hours: int = 25                 # how far back Silver reads from Bronze

    # ── Job / Run metadata ────────────────────────────────────
    job_run_id: str = "local"
    notebook_name: str = "unknown"

    def __post_init__(self):
        base = f"abfss://{self.adls_container}@{self.adls_account}.dfs.core.windows.net"
        self.adls_base_path = base

        if not self.source_path:
            self.source_path = f"{base}/landing/{self.env}/sales/"

        if not self.log_base_path:
            self.log_base_path = f"{base}/pipeline_logs/{self.env}"

        self.bronze_table = f"{self.catalog_name}.bronze.sales_raw"
        self.silver_table = f"{self.catalog_name}.silver.sales_cleaned"
        self.gold_table   = f"{self.catalog_name}.gold.sales_summary"

    # ── Factory ───────────────────────────────────────────────
    @classmethod
    def load(cls, spark) -> "PipelineConfig":
        """
        Load config from Databricks widgets (if running in a job/notebook)
        then fall back to environment variables, then defaults.
        """
        def _get(widget_name: str, env_var: str, default: str) -> str:
            # 1. Try Databricks widget
            try:
                val = dbutils.widgets.get(widget_name)  # noqa: F821  (dbutils is injected)
                if val:
                    return val
            except Exception:
                pass
            # 2. Try env var
            return os.environ.get(env_var, default)

        env            = _get("env",            "PIPELINE_ENV",            "dev")
        catalog_name   = _get("catalog_name",   "PIPELINE_CATALOG",        f"{env}_catalog")
        adls_account   = _get("adls_account",   "ADLS_ACCOUNT",            "mystorageaccount")
        adls_container = _get("adls_container", "ADLS_CONTAINER",          "datalake")
        source_path    = _get("source_path",    "PIPELINE_SOURCE_PATH",    "")
        watermark_hours = int(_get("watermark_hours", "PIPELINE_WATERMARK_HOURS", "25"))

        # Grab Databricks run context when available
        job_run_id = "local"
        notebook_name = "unknown"
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # noqa
            job_run_id    = ctx.currentRunId().get() or "local"
            notebook_name = ctx.notebookPath().get() or "unknown"
        except Exception:
            pass

        return cls(
            env=env,
            catalog_name=catalog_name,
            adls_account=adls_account,
            adls_container=adls_container,
            source_path=source_path,
            watermark_hours=watermark_hours,
            job_run_id=str(job_run_id),
            notebook_name=notebook_name,
        )

    def as_dict(self) -> dict:
        return self.__dict__.copy()

# Databricks notebook source
# FILE: notebooks/utils/framework_utils.py
# PURPOSE: Core utility functions — secrets, audit logging, notifications,
#          schema registry, watermark management
# VERSION: 1.0 — Production Grade

# COMMAND ----------
# %pip install azure-identity azure-keyvault-secrets great-expectations
# dbutils.library.restartPython()

# COMMAND ----------

import uuid
import json
import hashlib
import logging
import requests
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("framework_utils")

# COMMAND ----------
# ================================================================
# CONSTANTS
# ================================================================

FRAMEWORK_CATALOG   = "framework_catalog"
FRAMEWORK_SCHEMA    = "framework"
BRONZE_CATALOG      = "bronze_catalog"
SILVER_CATALOG      = "silver_catalog"
GOLD_CATALOG        = "gold_catalog"
CHECKPOINT_BASE     = "abfss://checkpoints@storageaccount.dfs.core.windows.net/streaming"
QUARANTINE_TABLE    = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.quarantine"
AUDIT_TABLE         = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.pipeline_audit"
CONFIG_TABLE        = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.pipeline_config"
DQ_RULES_TABLE      = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.dq_rules"
DQ_RESULTS_TABLE    = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.dq_results"
SCHEMA_REG_TABLE    = f"{FRAMEWORK_CATALOG}.{FRAMEWORK_SCHEMA}.schema_registry"


# COMMAND ----------
# ================================================================
# SECRET MANAGEMENT — Azure Key Vault via Databricks Secret Scope
# ================================================================

class SecretManager:
    """
    Wraps Databricks secret scope backed by Azure Key Vault.
    Never hardcode credentials. Always fetch at runtime.
    """

    def __init__(self, scope_name: str = "akv-secret-scope"):
        self.scope = scope_name

    def get_secret(self, key: str) -> str:
        """Fetch a secret from the Databricks secret scope."""
        try:
            value = dbutils.secrets.get(scope=self.scope, key=key)
            logger.info(f"Secret retrieved successfully: {key}")
            return value
        except Exception as e:
            logger.error(f"Failed to retrieve secret '{key}': {e}")
            raise RuntimeError(f"Secret retrieval failed for key: {key}") from e

    def get_jdbc_connection_string(self, key: str) -> str:
        return self.get_secret(key)

    def get_storage_key(self, key: str) -> str:
        return self.get_secret(key)


# COMMAND ----------
# ================================================================
# PIPELINE CONFIG READER
# ================================================================

class PipelineConfigReader:
    """Reads pipeline config from the metadata control table."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_config(self, pipeline_name: str) -> Dict[str, Any]:
        """Fetch single pipeline config as a dictionary."""
        df = self.spark.sql(f"""
            SELECT * FROM {CONFIG_TABLE}
            WHERE pipeline_name = '{pipeline_name}'
              AND active_flag = TRUE
        """)
        rows = df.collect()
        if not rows:
            raise ValueError(f"No active pipeline config found for: {pipeline_name}")
        return rows[0].asDict()

    def get_active_pipelines(
        self,
        pipeline_group: Optional[str] = None,
        target_layer: Optional[str] = None,
        source_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Fetch all active pipeline configs with optional filters."""
        filters = ["active_flag = TRUE"]
        if pipeline_group:
            filters.append(f"pipeline_group = '{pipeline_group}'")
        if target_layer:
            filters.append(f"target_layer = '{target_layer}'")
        if source_type:
            filters.append(f"source_type = '{source_type}'")

        where_clause = " AND ".join(filters)
        df = self.spark.sql(f"SELECT * FROM {CONFIG_TABLE} WHERE {where_clause}")
        return [row.asDict() for row in df.collect()]

    def get_dq_rules(self, pipeline_id: int) -> List[Dict[str, Any]]:
        """Fetch active DQ rules for a pipeline."""
        df = self.spark.sql(f"""
            SELECT * FROM {DQ_RULES_TABLE}
            WHERE pipeline_id = {pipeline_id}
              AND active_flag = TRUE
        """)
        return [row.asDict() for row in df.collect()]


# COMMAND ----------
# ================================================================
# AUDIT MANAGER
# ================================================================

class AuditManager:
    """
    Manages pipeline run audit trail.
    Call start_run() at beginning, end_run() at end.
    All state stored in Delta table — survives job failures.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.run_id = str(uuid.uuid4())
        self.audit_id: Optional[int] = None

    def start_run(self, config: Dict[str, Any], retry_attempt: int = 0) -> str:
        """Log pipeline start. Returns run_id."""
        try:
            cluster_id = (
                spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
                if hasattr(spark, 'conf') else "unknown"
            )
            self.spark.sql(f"""
                INSERT INTO {AUDIT_TABLE} (
                    run_id, pipeline_id, pipeline_name, target_layer,
                    run_date, start_time, status,
                    cluster_id, retry_attempt
                ) VALUES (
                    '{self.run_id}',
                    {config['pipeline_id']},
                    '{config['pipeline_name']}',
                    '{config['target_layer']}',
                    CURRENT_DATE(),
                    CURRENT_TIMESTAMP(),
                    'running',
                    '{cluster_id}',
                    {retry_attempt}
                )
            """)
            logger.info(f"Audit started — run_id: {self.run_id}, pipeline: {config['pipeline_name']}")
        except Exception as e:
            logger.warning(f"Audit start failed (non-blocking): {e}")
        return self.run_id

    def end_run(
        self,
        config: Dict[str, Any],
        status: str,
        metrics: Dict[str, Any],
        error_message: str = None,
        error_stack: str = None
    ) -> None:
        """Log pipeline completion with metrics."""
        try:
            rows_read      = metrics.get("rows_read", 0)
            rows_written   = metrics.get("rows_written", 0)
            rows_rejected  = metrics.get("rows_rejected", 0)
            rows_inserted  = metrics.get("rows_inserted", 0)
            rows_updated   = metrics.get("rows_updated", 0)
            rows_deleted   = metrics.get("rows_deleted", 0)
            watermark_end  = metrics.get("watermark_end", "")
            data_volume_mb = metrics.get("data_volume_mb", 0.0)

            error_msg   = (error_message or "").replace("'", "''")
            error_stack = (error_stack or "").replace("'", "''")[:4000]

            self.spark.sql(f"""
                UPDATE {AUDIT_TABLE}
                SET
                    end_time        = CURRENT_TIMESTAMP(),
                    status          = '{status}',
                    rows_read       = {rows_read},
                    rows_written    = {rows_written},
                    rows_rejected   = {rows_rejected},
                    rows_inserted   = {rows_inserted},
                    rows_updated    = {rows_updated},
                    rows_deleted    = {rows_deleted},
                    watermark_end   = '{watermark_end}',
                    data_volume_mb  = {data_volume_mb},
                    error_message   = '{error_msg}',
                    error_stack_trace = '{error_stack}',
                    duration_seconds = DATEDIFF(SECOND, start_time, CURRENT_TIMESTAMP())
                WHERE run_id = '{self.run_id}'
            """)
            logger.info(f"Audit ended — run_id: {self.run_id}, status: {status}")

            # Update watermark on success
            if status == "success" and watermark_end:
                self._update_watermark(config, watermark_end)

        except Exception as e:
            logger.warning(f"Audit end failed (non-blocking): {e}")

    def _update_watermark(self, config: Dict[str, Any], new_watermark: str) -> None:
        """Update watermark value in pipeline_config after successful run."""
        self.spark.sql(f"""
            UPDATE {CONFIG_TABLE}
            SET
                watermark_value = '{new_watermark}',
                modified_date   = CURRENT_TIMESTAMP(),
                modified_by     = 'framework_auto'
            WHERE pipeline_id = {config['pipeline_id']}
        """)
        logger.info(f"Watermark updated to: {new_watermark}")


# COMMAND ----------
# ================================================================
# SCHEMA REGISTRY
# ================================================================

class SchemaRegistry:
    """
    Detects and tracks schema changes across pipeline runs.
    Alerts on schema drift before it causes downstream failures.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_schema_fingerprint(self, df: DataFrame) -> str:
        """Generate MD5 fingerprint from DataFrame schema."""
        schema_str = str(sorted([(f.name, str(f.dataType)) for f in df.schema.fields]))
        return hashlib.md5(schema_str.encode()).hexdigest()

    def check_and_register(self, df: DataFrame, config: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Compare current schema against registry.
        Returns (has_changed, change_summary).
        """
        current_fp = self.get_schema_fingerprint(df)
        current_schema_json = df.schema.json()

        existing = self.spark.sql(f"""
            SELECT schema_fingerprint, schema_json, schema_version
            FROM {SCHEMA_REG_TABLE}
            WHERE pipeline_id = {config['pipeline_id']}
              AND is_current = TRUE
            ORDER BY detected_date DESC
            LIMIT 1
        """).collect()

        if not existing:
            # First time — register baseline
            self._register_schema(config, current_fp, current_schema_json, 1, "Initial schema registration")
            logger.info(f"Schema baseline registered for pipeline: {config['pipeline_name']}")
            return False, "Initial registration"

        prev = existing[0]
        if prev["schema_fingerprint"] == current_fp:
            return False, "No change"

        # Schema has changed — detect what changed
        change_summary = self._diff_schemas(
            StructType.fromJson(json.loads(prev["schema_json"])),
            df.schema
        )
        new_version = prev["schema_version"] + 1

        # Mark old as not current
        self.spark.sql(f"""
            UPDATE {SCHEMA_REG_TABLE}
            SET is_current = FALSE
            WHERE pipeline_id = {config['pipeline_id']}
        """)

        # Register new version
        self._register_schema(config, current_fp, current_schema_json, new_version, change_summary)
        logger.warning(f"Schema drift detected for {config['pipeline_name']}: {change_summary}")
        return True, change_summary

    def _register_schema(
        self, config, fingerprint, schema_json, version, change_summary
    ) -> None:
        schema_json_escaped = schema_json.replace("'", "\\'")
        self.spark.sql(f"""
            INSERT INTO {SCHEMA_REG_TABLE} (
                pipeline_id, source_object, schema_version,
                schema_fingerprint, schema_json, is_current, change_summary
            ) VALUES (
                {config['pipeline_id']},
                '{config['source_object']}',
                {version},
                '{fingerprint}',
                '{schema_json_escaped}',
                TRUE,
                '{change_summary}'
            )
        """)

    def _diff_schemas(self, old_schema: StructType, new_schema: StructType) -> str:
        old_fields = {f.name: str(f.dataType) for f in old_schema.fields}
        new_fields = {f.name: str(f.dataType) for f in new_schema.fields}

        added   = [f for f in new_fields if f not in old_fields]
        removed = [f for f in old_fields if f not in new_fields]
        changed = [
            f for f in old_fields
            if f in new_fields and old_fields[f] != new_fields[f]
        ]

        parts = []
        if added:   parts.append(f"ADDED: {added}")
        if removed: parts.append(f"REMOVED: {removed}")
        if changed: parts.append(f"TYPE_CHANGED: {changed}")
        return "; ".join(parts) or "Unknown change"


# COMMAND ----------
# ================================================================
# NOTIFICATION MANAGER
# ================================================================

class NotificationManager:
    """Send alerts to email / Microsoft Teams on pipeline events."""

    def __init__(self, secret_manager: SecretManager):
        self.sm = secret_manager

    def send_teams_alert(
        self,
        webhook_key: str,
        title: str,
        message: str,
        status: str,
        pipeline_name: str,
        run_id: str
    ) -> None:
        """Post adaptive card to Teams channel via webhook."""
        if not webhook_key:
            return

        color = {"success": "Good", "failed": "Attention", "warning": "Warning"}.get(status, "Default")
        try:
            webhook_url = self.sm.get_secret(webhook_key)
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": {"Good": "00FF00", "Attention": "FF0000", "Warning": "FFA500"}.get(color, "0076D7"),
                "summary": title,
                "sections": [{
                    "activityTitle": f"**{title}**",
                    "activitySubtitle": f"Pipeline: `{pipeline_name}` | Run ID: `{run_id}`",
                    "facts": [
                        {"name": "Status",   "value": status.upper()},
                        {"name": "Message",  "value": message},
                        {"name": "Time",     "value": datetime.now(timezone.utc).isoformat()}
                    ]
                }]
            }
            resp = requests.post(webhook_url, json=payload, timeout=10)
            resp.raise_for_status()
            logger.info(f"Teams alert sent: {title}")
        except Exception as e:
            logger.warning(f"Teams alert failed (non-blocking): {e}")


# COMMAND ----------
# ================================================================
# QUARANTINE MANAGER
# ================================================================

class QuarantineManager:
    """Write rejected records to quarantine table for later review/reprocessing."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def write_quarantine(
        self,
        bad_df: DataFrame,
        config: Dict[str, Any],
        run_id: str,
        error_type: str,
        error_message: str,
        failed_rule_id: Optional[int] = None
    ) -> int:
        """
        Convert bad records to JSON and write to quarantine table.
        Returns count of quarantined records.
        """
        if bad_df.isEmpty():
            return 0

        count = bad_df.count()
        rule_id_sql = str(failed_rule_id) if failed_rule_id else "NULL"
        target_table = f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"

        quarantine_df = bad_df.select(
            F.lit(run_id).alias("run_id"),
            F.lit(config["pipeline_id"]).alias("pipeline_id"),
            F.lit(config["pipeline_name"]).alias("pipeline_name"),
            F.lit(config["source_object"]).alias("source_table"),
            F.lit(target_table).alias("target_table"),
            F.to_json(F.struct(*bad_df.columns)).alias("raw_record"),
            F.lit(error_type).alias("error_type"),
            F.lit(error_message).alias("error_message"),
            F.lit(failed_rule_id).cast("bigint").alias("failed_rule_id"),
            F.current_date().alias("run_date")
        )

        quarantine_df.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(QUARANTINE_TABLE)

        logger.warning(f"Quarantined {count} records — pipeline: {config['pipeline_name']}, type: {error_type}")
        return count


# COMMAND ----------
# ================================================================
# HELPER FUNCTIONS
# ================================================================

def add_framework_metadata(df: DataFrame, config: Dict[str, Any], run_id: str) -> DataFrame:
    """Add standard framework metadata columns to any DataFrame."""
    return df \
        .withColumn("_src_system",   F.lit(config.get("pipeline_group", "unknown"))) \
        .withColumn("_src_object",   F.lit(config.get("source_object", "unknown"))) \
        .withColumn("_ingestion_ts", F.current_timestamp()) \
        .withColumn("_run_id",       F.lit(run_id)) \
        .withColumn("_pipeline_id",  F.lit(config.get("pipeline_id", -1)).cast("bigint"))


def get_table_full_name(config: Dict[str, Any]) -> str:
    return f"{config['target_catalog']}.{config['target_schema']}.{config['target_table']}"


def safe_table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def get_row_count(spark: SparkSession, table_name: str) -> int:
    return spark.sql(f"SELECT COUNT(*) AS cnt FROM {table_name}").collect()[0]["cnt"]


logger.info("framework_utils loaded successfully")


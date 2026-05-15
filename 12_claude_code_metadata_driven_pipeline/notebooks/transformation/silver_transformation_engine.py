# Databricks notebook source
# FILE: notebooks/transformation/silver_transformation_engine.py
# PURPOSE: Metadata-driven Silver layer transformation
#          Handles SCD Type 1, Type 2, MERGE (upsert), cleansing
#          Idempotent — safe to rerun
# VERSION: 1.0 — Production Grade

# COMMAND ----------
# %run ../utils/framework_utils
# %run ../utils/dq_engine

# COMMAND ----------

import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from delta.tables import DeltaTable
import logging
import hashlib

logger = logging.getLogger("silver_transformation")

# COMMAND ----------
dbutils.widgets.text("pipeline_name",  "", "Pipeline Name")
dbutils.widgets.text("load_date",      "", "Load Date (optional override)")

PIPELINE_NAME   = dbutils.widgets.get("pipeline_name")
LOAD_DATE       = dbutils.widgets.get("load_date") or None


# COMMAND ----------
# ================================================================
# TRANSFORMATION ENGINE
# ================================================================

class SilverTransformationEngine:
    """
    Reads from Bronze, applies cleansing + business rules,
    writes to Silver using MERGE (SCD1) or SCD2.
    All idempotent — reruns are safe.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ── PUBLIC ENTRY POINT ─────────────────────────────────────

    def transform(self, config: Dict[str, Any], run_id: str) -> Dict[str, Any]:
        """
        Main transformation entry point.
        Returns metrics dict.
        """
        source_table = f"{BRONZE_CATALOG}.{config['target_schema']}.{config['target_table']}"
        target_table = config.get("silver_target_table") or self._derive_silver_table(config)

        logger.info(f"Silver transform: {source_table} → {target_table}")

        # Read incremental Bronze data since last Silver load
        bronze_df = self._read_bronze_incremental(source_table, config, run_id)

        if bronze_df.isEmpty():
            logger.info("No new Bronze records to process.")
            return {"rows_read": 0, "rows_written": 0, "rows_inserted": 0, "rows_updated": 0}

        rows_read = bronze_df.count()

        # Apply generic cleansing
        cleansed_df = self._apply_cleansing(bronze_df, config)

        # Apply business-specific transformation (notebook override or generic)
        transformed_df = self._apply_business_rules(cleansed_df, config)

        # Add checksum for change detection
        transformed_df = self._add_checksum(transformed_df, config)

        # Write to Silver using MERGE or SCD2
        metrics = self._write_silver(transformed_df, config, target_table, run_id)
        metrics["rows_read"] = rows_read

        return metrics

    # ── BRONZE READ ────────────────────────────────────────────

    def _read_bronze_incremental(
        self,
        source_table: str,
        config: Dict[str, Any],
        run_id: str
    ) -> DataFrame:
        """
        Read only records from last successful Silver run.
        Uses _run_id to identify records from the current Bronze run.
        """
        # Use Change Data Feed if available
        try:
            df = self.spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", self._get_last_silver_version(config)) \
                .table(source_table) \
                .filter("_change_type != 'update_preimage'")
            logger.info("Reading Bronze via Change Data Feed")
            return df
        except Exception:
            # Fallback: read by run_id
            logger.info("CDF not available — falling back to run_id filter")
            return self.spark.table(source_table).filter(F.col("_run_id") == run_id)

    def _get_last_silver_version(self, config: Dict[str, Any]) -> int:
        """Get the Delta version of Bronze table at the last successful Silver run."""
        try:
            result = spark.sql(f"""
                SELECT MAX(pa.end_time) AS last_run
                FROM {AUDIT_TABLE} pa
                WHERE pa.pipeline_id = {config['pipeline_id']}
                  AND pa.target_layer = 'silver'
                  AND pa.status = 'success'
            """).collect()[0]["last_run"]

            if result:
                history = spark.sql(f"""
                    DESCRIBE HISTORY {BRONZE_CATALOG}.{config['target_schema']}.{config['target_table']}
                """)
                version_row = history.filter(F.col("timestamp") <= F.lit(result)) \
                    .orderBy(F.col("version").desc()).first()
                return int(version_row["version"]) if version_row else 0
        except Exception:
            pass
        return 0

    # ── CLEANSING ──────────────────────────────────────────────

    def _apply_cleansing(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Generic cleansing applied to all pipelines:
        - Trim string columns
        - Standardise nulls
        - Remove framework metadata columns (they get replaced in Silver)
        """
        # Trim all string columns
        for field in df.schema.fields:
            if str(field.dataType) == "StringType()":
                df = df.withColumn(field.name, F.trim(F.col(field.name)))
                df = df.withColumn(
                    field.name,
                    F.when(F.col(field.name) == "", None).otherwise(F.col(field.name))
                )

        # Drop Bronze metadata columns — Silver will add its own
        bronze_meta = ["_src_system", "_src_object", "_ingestion_ts", "_run_id",
                       "_pipeline_id", "_file_name", "_kafka_offset", "_kafka_partition",
                       "_change_type", "_commit_version", "_commit_timestamp"]
        cols_to_drop = [c for c in bronze_meta if c in df.columns]
        if cols_to_drop:
            df = df.drop(*cols_to_drop)

        # Deduplicate within the batch by primary key (keep latest by ingestion)
        if config.get("primary_keys"):
            pk_cols = [c.strip() for c in config["primary_keys"].split(",")]
            df = df.dropDuplicates(pk_cols)

        logger.info("Generic cleansing applied")
        return df

    # ── BUSINESS RULES ─────────────────────────────────────────

    def _apply_business_rules(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Apply pipeline-specific transformations.
        Dispatches to specialised transformer based on pipeline_group.
        Extend this with more groups as needed.
        """
        group = config.get("pipeline_group", "").lower()

        transformers = {
            "crm":          CRMTransformer(),
            "finance":      FinanceTransformer(),
            "hr":           HRTransformer(),
            "digital":      DigitalTransformer()
        }

        transformer = transformers.get(group)
        if transformer:
            logger.info(f"Applying {group} business rules")
            return transformer.transform(df, config)

        logger.info(f"No specialised transformer for group '{group}' — using raw data")
        return df

    # ── CHECKSUM ───────────────────────────────────────────────

    def _add_checksum(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        """
        Add MD5 checksum of all non-metadata columns.
        Used to detect row-level changes for SCD2.
        """
        value_cols = [c for c in df.columns if not c.startswith("_")]
        if not value_cols:
            return df.withColumn("_checksum", F.lit("unknown"))

        concat_expr = F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in sorted(value_cols)])
        return df.withColumn("_checksum", F.md5(concat_expr))

    # ── SILVER WRITE ───────────────────────────────────────────

    def _write_silver(
        self,
        df: DataFrame,
        config: Dict[str, Any],
        target_table: str,
        run_id: str
    ) -> Dict[str, Any]:
        """
        Write to Silver using MERGE (SCD1) by default.
        Use SCD2 if load_strategy contains 'scd2'.
        """
        write_mode = config.get("write_mode", "merge")
        pk_cols    = [c.strip() for c in config.get("primary_keys", "").split(",") if c.strip()]

        if not pk_cols:
            # No primary key — plain append
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)
            return {"rows_written": df.count(), "rows_inserted": df.count(), "rows_updated": 0}

        if "scd2" in write_mode:
            return self._merge_scd2(df, config, target_table, pk_cols, run_id)
        else:
            return self._merge_scd1(df, config, target_table, pk_cols, run_id)

    def _merge_scd1(
        self,
        source_df: DataFrame,
        config: Dict[str, Any],
        target_table: str,
        pk_cols: List[str],
        run_id: str
    ) -> Dict[str, Any]:
        """
        SCD Type 1 MERGE — latest value always wins.
        Idempotent: safe to rerun with same data.
        """
        # Add Silver metadata
        source_df = source_df \
            .withColumn("_src_system",  F.lit(config.get("pipeline_group", "unknown"))) \
            .withColumn("_pipeline_id", F.lit(config["pipeline_id"]).cast("bigint")) \
            .withColumn("_run_id",      F.lit(run_id)) \
            .withColumn("_updated_ts",  F.current_timestamp())

        # Create table if it doesn't exist
        if not safe_table_exists(self.spark, target_table):
            source_df \
                .withColumn("_created_ts", F.current_timestamp()) \
                .withColumn("_is_deleted", F.lit(False)) \
                .write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true") \
                .saveAsTable(target_table)
            cnt = source_df.count()
            logger.info(f"Silver table created with {cnt} rows: {target_table}")
            return {"rows_written": cnt, "rows_inserted": cnt, "rows_updated": 0, "rows_deleted": 0}

        # Build MERGE condition from primary keys
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

        # All non-PK, non-metadata columns go into the update set
        update_cols = [
            c for c in source_df.columns
            if c not in pk_cols and not c.startswith("_created")
        ]
        update_set = {c: f"source.{c}" for c in update_cols}

        delta_table = DeltaTable.forName(self.spark, target_table)

        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            condition="source._checksum <> target._checksum",  # only update if something changed
            set=update_set
        ).whenNotMatchedInsert(
            values={**{c: f"source.{c}" for c in source_df.columns},
                    "_created_ts": "CURRENT_TIMESTAMP()",
                    "_is_deleted": "false"}
        ).execute()

        # Get merge metrics from Delta operation metrics
        metrics = self.spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1").collect()[0]
        op_metrics = metrics["operationMetrics"] if metrics else {}

        rows_inserted = int(op_metrics.get("numTargetRowsInserted", 0))
        rows_updated  = int(op_metrics.get("numTargetRowsUpdated", 0))

        logger.info(f"SCD1 MERGE complete: {target_table} | inserted: {rows_inserted}, updated: {rows_updated}")
        return {
            "rows_written":  rows_inserted + rows_updated,
            "rows_inserted": rows_inserted,
            "rows_updated":  rows_updated,
            "rows_deleted":  0
        }

    def _merge_scd2(
        self,
        source_df: DataFrame,
        config: Dict[str, Any],
        target_table: str,
        pk_cols: List[str],
        run_id: str
    ) -> Dict[str, Any]:
        """
        SCD Type 2 — preserves full history.
        Closes old records, inserts new versions.
        """
        now = F.current_timestamp()

        source_df = source_df \
            .withColumn("_src_system",  F.lit(config.get("pipeline_group", "unknown"))) \
            .withColumn("_pipeline_id", F.lit(config["pipeline_id"]).cast("bigint")) \
            .withColumn("_run_id",      F.lit(run_id)) \
            .withColumn("_valid_from",  now) \
            .withColumn("_valid_to",    F.lit(None).cast(TimestampType())) \
            .withColumn("_is_current",  F.lit(True)) \
            .withColumn("_is_deleted",  F.lit(False)) \
            .withColumn("_created_ts",  now) \
            .withColumn("_updated_ts",  now)

        if not safe_table_exists(self.spark, target_table):
            source_df.write.format("delta").mode("overwrite") \
                .option("mergeSchema", "true").saveAsTable(target_table)
            cnt = source_df.count()
            return {"rows_written": cnt, "rows_inserted": cnt, "rows_updated": 0, "rows_deleted": 0}

        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

        delta_table = DeltaTable.forName(self.spark, target_table)

        # Step 1: Expire old current records where checksum differs
        delta_table.alias("target").merge(
            source_df.alias("source"),
            f"({merge_condition}) AND target._is_current = TRUE AND target._checksum <> source._checksum"
        ).whenMatchedUpdate(set={
            "_valid_to":    "CURRENT_TIMESTAMP()",
            "_is_current":  "false",
            "_updated_ts":  "CURRENT_TIMESTAMP()"
        }).execute()

        # Step 2: Insert new versions (where no current record matches)
        new_records = source_df.alias("src").join(
            self.spark.table(target_table).filter("_is_current = TRUE").alias("tgt"),
            on=[F.col(f"src.{pk}") == F.col(f"tgt.{pk}") for pk in pk_cols],
            how="left_anti"
        )
        new_records.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target_table)

        cnt = new_records.count()
        logger.info(f"SCD2 MERGE complete: {target_table} | new versions: {cnt}")
        return {"rows_written": cnt, "rows_inserted": cnt, "rows_updated": 0, "rows_deleted": 0}

    def _derive_silver_table(self, config: Dict[str, Any]) -> str:
        """Derive Silver table name from bronze config."""
        return f"{SILVER_CATALOG}.conformed.{config['target_table']}"


# COMMAND ----------
# ================================================================
# DOMAIN-SPECIFIC TRANSFORMERS
# ================================================================

class CRMTransformer:
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        return df \
            .withColumn("email",        F.lower(F.col("Email"))) \
            .withColumn("full_name",    F.concat_ws(" ", F.col("FirstName"), F.col("LastName"))) \
            .withColumn("phone",        F.regexp_replace(F.col("Phone"), r"[^0-9+]", "")) \
            .withColumn("status",       F.upper(F.col("Status"))) \
            .withColumn("country_code", F.upper(F.col("CountryCode")))


class FinanceTransformer:
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        return df \
            .withColumn("amount",           F.round(F.col("AMOUNT").cast("decimal(18,2)"), 2)) \
            .withColumn("currency_code",    F.upper(F.col("CURRENCY_CODE"))) \
            .withColumn("transaction_date", F.to_date(F.col("TRANSACTION_DATE"))) \
            .withColumn("fiscal_year",      F.year(F.col("TRANSACTION_DATE"))) \
            .withColumn("fiscal_quarter",   F.quarter(F.col("TRANSACTION_DATE")))


class HRTransformer:
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        return df \
            .withColumn("employee_id",  F.col("EmployeeId").cast("int")) \
            .withColumn("hire_date",    F.to_date(F.col("HireDate"), "yyyy-MM-dd")) \
            .withColumn("tenure_days",  F.datediff(F.current_date(), F.col("hire_date"))) \
            .withColumn("department",   F.upper(F.col("Department")))


class DigitalTransformer:
    def transform(self, df: DataFrame, config: Dict[str, Any]) -> DataFrame:
        return df \
            .withColumn("event_ts",         F.from_unixtime(F.col("timestamp_ms") / 1000).cast(TimestampType())) \
            .withColumn("event_date",       F.to_date(F.col("event_ts"))) \
            .withColumn("event_hour",       F.hour(F.col("event_ts"))) \
            .withColumn("revenue",          F.coalesce(F.col("revenue").cast("decimal(18,2)"), F.lit(0.0)))


# COMMAND ----------
# ================================================================
# MAIN
# ================================================================

def run_silver_pipeline(pipeline_name: str) -> None:
    spark       = SparkSession.getActiveSession()
    sm          = SecretManager()
    config_rdr  = PipelineConfigReader(spark)
    audit_mgr   = AuditManager(spark)
    notif_mgr   = NotificationManager(sm)
    qm          = QuarantineManager(spark)
    dq_engine   = DataQualityEngine(spark, qm)
    engine      = SilverTransformationEngine(spark)

    config  = config_rdr.get_config(pipeline_name)
    run_id  = audit_mgr.start_run({**config, "target_layer": "silver"})

    metrics = {"rows_read": 0, "rows_written": 0, "rows_inserted": 0, "rows_updated": 0}

    try:
        metrics = engine.transform(config, run_id)
        audit_mgr.end_run({**config, "target_layer": "silver"}, "success", metrics)
        logger.info(f"Silver pipeline SUCCESS: {pipeline_name} | {metrics}")

    except Exception as e:
        error_msg   = str(e)
        error_stack = traceback.format_exc()
        logger.error(f"Silver pipeline FAILED: {pipeline_name} | {error_msg}")
        audit_mgr.end_run({**config, "target_layer": "silver"}, "failed", metrics, error_msg, error_stack)
        notif_mgr.send_teams_alert(
            config.get("teams_webhook_url_key"),
            f"❌ Silver Pipeline Failed — {pipeline_name}",
            f"Error: {error_msg[:500]}",
            "failed", pipeline_name, run_id
        )
        raise


# COMMAND ----------
if PIPELINE_NAME:
    run_silver_pipeline(PIPELINE_NAME)


# Databricks notebook source
# FILE: notebooks/ingestion/bronze_ingestion_orchestrator.py
# PURPOSE: Metadata-driven Bronze ingestion — handles JDBC (SQL Server, Oracle),
#          ADLS Blob (CSV/Parquet/JSON), Event Hubs (streaming), Delta sources
#          NO code changes needed to add a new source — just insert a config row.
# VERSION: 1.0 — Production Grade

# COMMAND ----------
# %run ../utils/framework_utils
# %run ../utils/dq_engine

# COMMAND ----------

import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("bronze_ingestion")

# COMMAND ----------
# Widget parameters — passed by orchestrator (ADF / Databricks Workflow)
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("retry_attempt", "0", "Retry Attempt")

PIPELINE_NAME   = dbutils.widgets.get("pipeline_name")
RETRY_ATTEMPT   = int(dbutils.widgets.get("retry_attempt"))


# COMMAND ----------
# ================================================================
# SOURCE READERS
# ================================================================

class JDBCReader:
    """
    Unified JDBC reader for SQL Server and Oracle.
    Supports full load, incremental (watermark), and partitioned reads.
    """

    DRIVERS = {
        "sqlserver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "oracle":    "oracle.jdbc.OracleDriver"
    }

    URL_TEMPLATES = {
        "sqlserver": "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false",
        "oracle":    "jdbc:oracle:thin:@//{host}:{port}/{service}"
    }

    def __init__(self, spark: SparkSession, secret_manager: SecretManager):
        self.spark  = spark
        self.sm     = secret_manager

    def read(self, config: Dict[str, Any]) -> DataFrame:
        source_type = config["source_type"]
        conn_str    = self.sm.get_jdbc_connection_string(config["source_connection_key"])

        # Build query — use override or construct from table name + watermark
        query = self._build_query(config)
        logger.info(f"JDBC query: {query}")

        reader_opts = {
            "url":      conn_str,
            "driver":   self.DRIVERS[source_type],
            "dbtable":  f"({query}) AS src",
            "fetchSize": str(config.get("jdbc_fetch_size") or 10000)
        }

        # Enable parallel reads if partition column is defined
        if config.get("jdbc_partition_column") and config.get("jdbc_num_partitions", 1) > 1:
            reader_opts.update({
                "partitionColumn":  config["jdbc_partition_column"],
                "lowerBound":       str(config.get("jdbc_lower_bound", 0)),
                "upperBound":       str(config.get("jdbc_upper_bound", 1000000)),
                "numPartitions":    str(config.get("jdbc_num_partitions", 8))
            })
            logger.info(f"Parallel JDBC read: {config['jdbc_num_partitions']} partitions on {config['jdbc_partition_column']}")

        df = self.spark.read.format("jdbc").options(**reader_opts).load()
        logger.info(f"JDBC read complete: {config['source_object']}")
        return df

    def _build_query(self, config: Dict[str, Any]) -> str:
        """Build SQL query with optional watermark pushdown."""
        if config.get("source_query_override"):
            base_query = config["source_query_override"]
        else:
            base_query = f"SELECT * FROM {config.get('source_schema','dbo')}.{config['source_object']}"

        if config.get("load_strategy") == "incremental" and config.get("watermark_column"):
            wm_col   = config["watermark_column"]
            wm_val   = config.get("watermark_value", "1900-01-01 00:00:00")
            wm_type  = config.get("watermark_data_type", "datetime")

            if wm_type == "datetime":
                wm_filter = f"{wm_col} > '{wm_val}'"
            elif wm_type == "integer":
                wm_filter = f"{wm_col} > {wm_val}"
            else:
                wm_filter = f"{wm_col} > '{wm_val}'"

            # Wrap base query to apply filter
            return f"SELECT * FROM ({base_query}) AS base_q WHERE {wm_filter}"

        return base_query


# COMMAND ----------

class BlobReader:
    """
    Reads structured files from ADLS Gen2.
    Supports CSV, JSON, Parquet, Avro.
    Uses Auto Loader for incremental file detection.
    """

    def __init__(self, spark: SparkSession, secret_manager: SecretManager):
        self.spark  = spark
        self.sm     = secret_manager

    def read(self, config: Dict[str, Any], use_autoloader: bool = True) -> DataFrame:
        """Read files from ADLS. Auto Loader used for incremental, batch read for full."""
        storage_key = self.sm.get_storage_key(config["source_connection_key"])
        file_path   = config.get("file_path_pattern") or config["source_object"]
        file_format = config.get("file_format", "csv").lower()

        # Configure storage account access
        account_name = file_path.split("@")[1].split(".")[0]
        self.spark.conf.set(
            f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
            storage_key
        )

        if use_autoloader and config.get("load_strategy") == "incremental":
            return self._read_autoloader(config, file_path, file_format)
        else:
            return self._read_batch(config, file_path, file_format)

    def _read_autoloader(self, config: Dict[str, Any], path: str, fmt: str) -> DataFrame:
        """Auto Loader — detects new files automatically. Ideal for incremental."""
        checkpoint_path = f"{CHECKPOINT_BASE}/autoloader/{config['pipeline_name']}"

        reader = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", fmt) \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/_schema") \
            .option("cloudFiles.inferColumnTypes", "true") \
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")

        if fmt == "csv":
            reader = reader \
                .option("header",       str(config.get("has_header", True)).lower()) \
                .option("delimiter",    config.get("file_delimiter", ",")) \
                .option("multiLine",    "true") \
                .option("escape",       '"')

        df = reader.load(path) \
            .withColumn("_file_name", F.input_file_name())

        logger.info(f"Auto Loader stream started: {path}")
        return df

    def _read_batch(self, config: Dict[str, Any], path: str, fmt: str) -> DataFrame:
        """Batch read for full loads."""
        reader = self.spark.read

        if fmt == "csv":
            reader = reader \
                .option("header",   str(config.get("has_header", True)).lower()) \
                .option("delimiter", config.get("file_delimiter", ",")) \
                .option("multiLine", "true") \
                .option("escape",    '"') \
                .option("inferSchema", "true")
        elif fmt == "json":
            reader = reader.option("multiLine", "true")

        df = reader.format(fmt).load(path) \
            .withColumn("_file_name", F.input_file_name())

        logger.info(f"Batch file read complete: {path}")
        return df


# COMMAND ----------

class EventHubReader:
    """
    Structured Streaming reader for Azure Event Hubs.
    Parses JSON payload, adds event metadata columns.
    """

    def __init__(self, spark: SparkSession, secret_manager: SecretManager):
        self.spark  = spark
        self.sm     = secret_manager

    def read(self, config: Dict[str, Any]) -> DataFrame:
        conn_str        = self.sm.get_secret(config["source_connection_key"])
        topic           = config["source_object"]
        checkpoint_path = f"{CHECKPOINT_BASE}/eventhub/{config['pipeline_name']}"

        eh_conf = {
            "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str),
            "eventhubs.startingPosition": json.dumps({"offset": "-1", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
        }

        raw_df = self.spark.readStream \
            .format("eventhubs") \
            .options(**eh_conf) \
            .load()

        # Parse the binary body as JSON — adjust schema per topic as needed
        parsed_df = raw_df \
            .withColumn("body",             F.col("body").cast(StringType())) \
            .withColumn("event_data",        F.from_json(F.col("body"), self._infer_event_schema(topic))) \
            .withColumn("_kafka_offset",     F.col("offset").cast("bigint")) \
            .withColumn("_kafka_partition",  F.col("partitionId").cast("int")) \
            .withColumn("_event_enqueued_ts", F.col("enqueuedTime")) \
            .select("event_data.*", "_kafka_offset", "_kafka_partition", "_event_enqueued_ts")

        logger.info(f"Event Hub stream connected: {topic}")
        return parsed_df

    def _infer_event_schema(self, topic: str):
        """
        Return schema for known topics.
        In production, store schemas in schema registry or a config table.
        """
        from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

        schemas = {
            "clickstream-events": StructType([
                StructField("event_id",         StringType()),
                StructField("session_id",        StringType()),
                StructField("user_id",           StringType()),
                StructField("event_type",        StringType()),
                StructField("page_url",          StringType()),
                StructField("product_id",        StringType()),
                StructField("timestamp_ms",      LongType()),
                StructField("device_type",       StringType()),
                StructField("country_code",      StringType()),
                StructField("revenue",           DoubleType())
            ])
        }
        return schemas.get(topic, StringType())  # fallback to raw string if schema unknown


# COMMAND ----------
# ================================================================
# BRONZE WRITER
# ================================================================

class BronzeWriter:
    """
    Writes DataFrames to Bronze Delta tables.
    Handles schema evolution, append vs overwrite, streaming writes.
    """

    def write_batch(
        self,
        df: DataFrame,
        config: Dict[str, Any],
        run_id: str
    ) -> Dict[str, Any]:
        """Write batch DataFrame to Bronze Delta table."""
        target      = get_table_full_name(config)
        write_mode  = config.get("write_mode", "append")
        merge_schema = str(config.get("schema_evolution", True)).lower()

        # Add framework metadata columns
        df = add_framework_metadata(df, config, run_id)

        writer = df.write \
            .format("delta") \
            .option("mergeSchema", merge_schema)

        if config.get("partition_columns"):
            writer = writer.partitionBy(*config["partition_columns"].split(","))

        if write_mode == "overwrite":
            writer = writer.mode("overwrite").option("overwriteSchema", "true")
        else:
            writer = writer.mode("append")

        writer.saveAsTable(target)

        rows_written = df.count()
        logger.info(f"Bronze write complete: {target}, rows: {rows_written}")
        return {"rows_written": rows_written}

    def write_stream(
        self,
        df: DataFrame,
        config: Dict[str, Any],
        run_id: str
    ):
        """Write streaming DataFrame to Bronze Delta table."""
        target          = get_table_full_name(config)
        checkpoint_path = f"{CHECKPOINT_BASE}/bronze/{config['pipeline_name']}"

        df = add_framework_metadata(df, config, run_id)

        query = df.writeStream \
            .format("delta") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds") \
            .toTable(target)

        logger.info(f"Bronze streaming write started: {target}")
        return query


# COMMAND ----------
# ================================================================
# MAIN ORCHESTRATION — BRONZE INGESTION PIPELINE
# ================================================================

def run_bronze_pipeline(pipeline_name: str, retry_attempt: int = 0) -> None:
    """
    Main entry point for bronze ingestion.
    Reads config, selects correct reader, applies DQ, writes to Bronze.
    """
    spark = SparkSession.getActiveSession()

    # ── Init framework components ──────────────────────────────
    sm          = SecretManager()
    config_rdr  = PipelineConfigReader(spark)
    audit_mgr   = AuditManager(spark)
    schema_reg  = SchemaRegistry(spark)
    notif_mgr   = NotificationManager(sm)
    qm          = QuarantineManager(spark)
    dq_engine   = DataQualityEngine(spark, qm)
    bronze_wrt  = BronzeWriter()

    # ── Load config ────────────────────────────────────────────
    config  = config_rdr.get_config(pipeline_name)
    run_id  = audit_mgr.start_run(config, retry_attempt)

    metrics = {
        "rows_read": 0, "rows_written": 0, "rows_rejected": 0,
        "watermark_end": config.get("watermark_value", "")
    }

    try:
        logger.info(f"Starting pipeline: {pipeline_name} | source_type: {config['source_type']} | strategy: {config['load_strategy']}")

        # ── Read source ────────────────────────────────────────
        source_type = config["source_type"]

        if source_type in ("sqlserver", "oracle"):
            reader = JDBCReader(spark, sm)
            df     = reader.read(config)

        elif source_type == "blob":
            reader = BlobReader(spark, sm)
            is_streaming = config.get("load_strategy") == "streaming"
            df = reader.read(config, use_autoloader=True)

        elif source_type == "eventhub":
            reader = EventHubReader(spark, sm)
            df     = reader.read(config)

        elif source_type == "delta":
            # Delta-to-Delta copy (e.g., cross-workspace replication)
            df = spark.read.format("delta").load(config["source_object"])

        else:
            raise ValueError(f"Unsupported source_type: {source_type}")

        # ── Handle streaming vs batch separately ───────────────
        is_streaming = config.get("load_strategy") == "streaming"

        if is_streaming:
            # For streaming — DQ done via DLT constraints, write directly
            stream_query = bronze_wrt.write_stream(df, config, run_id)
            # Keep stream running — orchestrator manages lifecycle
            stream_query.awaitTermination()
            return

        # ── Batch path ─────────────────────────────────────────
        rows_read = df.count()
        metrics["rows_read"] = rows_read
        logger.info(f"Rows read from source: {rows_read}")

        # ── Schema drift detection ─────────────────────────────
        schema_changed, change_summary = schema_reg.check_and_register(df, config)
        if schema_changed:
            notif_mgr.send_teams_alert(
                config.get("teams_webhook_url_key"),
                f"⚠️ Schema Drift Detected — {pipeline_name}",
                f"Change: {change_summary}",
                "warning",
                pipeline_name,
                run_id
            )

        # ── Data Quality ───────────────────────────────────────
        if config.get("dq_rules_enabled", True):
            df, dq_summary = dq_engine.run_all_rules(df, config, run_id, audit_id=0)
            metrics["rows_rejected"] = dq_summary.get("failed", 0)
            logger.info(f"DQ complete: {dq_summary}")

        # ── Capture new watermark ──────────────────────────────
        if config.get("load_strategy") == "incremental" and config.get("watermark_column"):
            wm_col = config["watermark_column"]
            if wm_col in df.columns:
                new_wm = df.agg(F.max(wm_col).cast("string").alias("max_wm")).collect()[0]["max_wm"]
                if new_wm:
                    metrics["watermark_end"] = new_wm

        # ── Write to Bronze ────────────────────────────────────
        write_metrics = bronze_wrt.write_batch(df, config, run_id)
        metrics.update(write_metrics)

        # ── Success ────────────────────────────────────────────
        audit_mgr.end_run(config, "success", metrics)
        logger.info(f"Pipeline SUCCESS: {pipeline_name} | rows written: {metrics['rows_written']}")

    except Exception as e:
        error_msg   = str(e)
        error_stack = traceback.format_exc()
        logger.error(f"Pipeline FAILED: {pipeline_name} | error: {error_msg}")

        audit_mgr.end_run(config, "failed", metrics, error_msg, error_stack)

        # Alert on failure
        notif_mgr.send_teams_alert(
            config.get("teams_webhook_url_key"),
            f"❌ Pipeline Failed — {pipeline_name}",
            f"Error: {error_msg[:500]}",
            "failed",
            pipeline_name,
            run_id
        )

        # Re-raise for retry logic in orchestrator
        raise


# COMMAND ----------
# ── Entry point ────────────────────────────────────────────────
if PIPELINE_NAME:
    run_bronze_pipeline(PIPELINE_NAME, RETRY_ATTEMPT)
else:
    logger.warning("No pipeline_name widget provided. Running in interactive mode.")


-- ============================================================
-- FILE: 01_create_control_tables.sql
-- PURPOSE: Create all metadata-driven framework control tables
-- AUTHOR: Data Architecture Team
-- VERSION: 1.0
-- ============================================================

-- -------------------------------------------------------
-- Use the framework catalog and schema
-- -------------------------------------------------------
USE CATALOG framework_catalog;
CREATE SCHEMA IF NOT EXISTS framework;
USE SCHEMA framework;

-- -------------------------------------------------------
-- TABLE: pipeline_config
-- PURPOSE: Master config for every pipeline in the system
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.pipeline_config (
    pipeline_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name           STRING          NOT NULL,
    pipeline_group          STRING          NOT NULL,   -- logical grouping e.g. 'finance', 'crm'
    source_type             STRING          NOT NULL,   -- 'sqlserver','oracle','blob','eventhub','api','delta'
    source_connection_key   STRING          NOT NULL,   -- Key Vault secret name for connection string
    source_database         STRING,                     -- source DB name (JDBC sources)
    source_schema           STRING,                     -- source schema name
    source_object           STRING          NOT NULL,   -- table name / file path pattern / topic name
    source_query_override   STRING,                     -- optional: custom SQL instead of full table read
    watermark_column        STRING,                     -- column used for incremental load
    watermark_value         STRING,                     -- last successfully processed value
    watermark_data_type     STRING,                     -- 'datetime','integer','string'
    load_strategy           STRING          NOT NULL,   -- 'full','incremental','cdc','streaming'
    cdc_type                STRING,                     -- 'debezium','sql_cdc','timestamp_based'
    target_catalog          STRING          NOT NULL,
    target_schema           STRING          NOT NULL,
    target_table            STRING          NOT NULL,
    target_layer            STRING          NOT NULL,   -- 'bronze','silver','gold'
    partition_columns       STRING,                     -- comma separated
    primary_keys            STRING,                     -- comma separated, used for MERGE
    write_mode              STRING          NOT NULL,   -- 'merge','append','overwrite'
    file_format             STRING,
    file_path_pattern       STRING,                     -- for blob/ADLS sources
    file_delimiter          STRING,
    has_header              BOOLEAN,
    schema_evolution        BOOLEAN,
    enforce_schema          BOOLEAN,
    jdbc_fetch_size         INT,
    jdbc_num_partitions     INT,
    jdbc_partition_column   STRING,
    jdbc_lower_bound        BIGINT,
    jdbc_upper_bound        BIGINT,
    max_retries             INT,
    retry_wait_seconds      INT,
    sla_minutes             INT,
    schedule_cron           STRING,
    timezone                STRING,
    notification_email      STRING,
    teams_webhook_url_key   STRING,                     -- Key Vault key for Teams webhook
    transformation_notebook STRING,                     -- path to silver transformation notebook
    dq_rules_enabled        BOOLEAN,
    active_flag             BOOLEAN,
    created_by              STRING          NOT NULL,
    created_date            TIMESTAMP,
    modified_by             STRING,
    modified_date           TIMESTAMP,
    comments                STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
);

-- -------------------------------------------------------
-- TABLE: pipeline_dependency
-- PURPOSE: Define execution order / DAG dependencies
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.pipeline_dependency (
    dependency_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id         BIGINT          NOT NULL,   -- downstream pipeline
    depends_on_id       BIGINT          NOT NULL,   -- upstream pipeline that must complete first
    dependency_type     STRING,                     -- 'hard' blocks, 'soft' warns
    active_flag         BOOLEAN,
    CONSTRAINT fk_pipeline  FOREIGN KEY (pipeline_id)    REFERENCES framework.pipeline_config(pipeline_id),
    CONSTRAINT fk_depends   FOREIGN KEY (depends_on_id) REFERENCES framework.pipeline_config(pipeline_id)
)
USING DELTA;

-- -------------------------------------------------------
-- TABLE: pipeline_audit
-- PURPOSE: Full audit trail of every pipeline run
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.pipeline_audit (
    audit_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              STRING          NOT NULL,   -- UUID per run
    pipeline_id         BIGINT          NOT NULL,
    pipeline_name       STRING          NOT NULL,
    target_layer        STRING          NOT NULL,
    run_date            DATE            NOT NULL,
    start_time          TIMESTAMP       NOT NULL,
    end_time            TIMESTAMP,
    status              STRING          NOT NULL,   -- 'running','success','failed','skipped'
    rows_read           BIGINT,
    rows_written        BIGINT,
    rows_rejected       BIGINT,
    rows_merged         BIGINT,
    rows_inserted       BIGINT,
    rows_updated        BIGINT,
    rows_deleted        BIGINT,
    watermark_start     STRING,
    watermark_end       STRING,
    duration_seconds    INT,
    cluster_id          STRING,
    notebook_path       STRING,
    databricks_run_id   BIGINT,
    error_message       STRING,
    error_stack_trace   STRING,
    retry_attempt       INT,
    data_volume_mb      DECIMAL(18,2),
    created_date        TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

-- -------------------------------------------------------
-- TABLE: dq_rules
-- PURPOSE: Data quality rules per pipeline/table
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.dq_rules (
    rule_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id         BIGINT          NOT NULL,
    rule_name           STRING          NOT NULL,
    rule_type           STRING          NOT NULL,   -- 'not_null','unique','range','regex','custom_sql','referential'
    column_name         STRING,
    rule_expression     STRING          NOT NULL,   -- SQL expression evaluated as boolean
    severity            STRING,                     -- 'error' halts, 'warning' logs
    threshold_pct       DECIMAL(5,2),               -- % of records allowed to fail before halting
    active_flag         BOOLEAN,
    created_date        TIMESTAMP
)
USING DELTA;

-- -------------------------------------------------------
-- TABLE: dq_results
-- PURPOSE: Data quality check results per run
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.dq_results (
    result_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              STRING          NOT NULL,
    audit_id            BIGINT          NOT NULL,
    rule_id             BIGINT          NOT NULL,
    pipeline_id         BIGINT          NOT NULL,
    rule_name           STRING          NOT NULL,
    rule_type           STRING          NOT NULL,
    column_name         STRING,
    total_records       BIGINT,
    passed_records      BIGINT,
    failed_records      BIGINT,
    failure_pct         DECIMAL(10,4),
    status              STRING,         -- 'pass','fail','warning'
    run_date            DATE            NOT NULL,
    created_date        TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_date);

-- -------------------------------------------------------
-- TABLE: quarantine
-- PURPOSE: Dead letter store for rejected records
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.quarantine (
    quarantine_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id              STRING          NOT NULL,
    pipeline_id         BIGINT          NOT NULL,
    pipeline_name       STRING          NOT NULL,
    source_table        STRING          NOT NULL,
    target_table        STRING          NOT NULL,
    raw_record          STRING          NOT NULL,   -- JSON representation of bad record
    error_type          STRING          NOT NULL,   -- 'schema_mismatch','dq_failure','parse_error'
    error_message       STRING          NOT NULL,
    failed_rule_id      BIGINT,
    run_date            DATE            NOT NULL,
    is_reprocessed      BOOLEAN,
    reprocessed_date    TIMESTAMP,
    created_date        TIMESTAMP
)
USING DELTA
PARTITIONED BY (run_date);

-- -------------------------------------------------------
-- TABLE: schema_registry
-- PURPOSE: Track schema versions per source object
-- -------------------------------------------------------
CREATE TABLE IF NOT EXISTS framework.schema_registry (
    registry_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_id         BIGINT          NOT NULL,
    source_object       STRING          NOT NULL,
    schema_version      INT             NOT NULL,
    schema_fingerprint  STRING          NOT NULL,   -- MD5 hash of schema
    schema_json         STRING          NOT NULL,   -- full schema as JSON
    is_current          BOOLEAN,
    detected_date       TIMESTAMP,
    change_summary      STRING          -- what changed vs previous version
)
USING DELTA;

COMMENT ON TABLE framework.pipeline_config   IS 'Master metadata config driving all ingestion pipelines';
COMMENT ON TABLE framework.pipeline_audit    IS 'Full audit trail for every pipeline execution';
COMMENT ON TABLE framework.dq_rules          IS 'Data quality rules applied per pipeline';
COMMENT ON TABLE framework.dq_results        IS 'Results of data quality checks per run';
COMMENT ON TABLE framework.quarantine        IS 'Dead letter table for rejected/invalid records';
COMMENT ON TABLE framework.schema_registry   IS 'Tracks schema versions and drift detection';

-- ============================================================
-- STEP 2: Enable column defaults feature for all tables
-- ============================================================
ALTER TABLE framework.pipeline_config SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.pipeline_dependency SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.pipeline_audit SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.dq_rules SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.dq_results SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.quarantine SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
ALTER TABLE framework.schema_registry SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

-- ============================================================
-- STEP 3: Add DEFAULT values to columns
-- ============================================================

-- pipeline_config defaults
ALTER TABLE framework.pipeline_config ALTER COLUMN write_mode SET DEFAULT 'merge';
ALTER TABLE framework.pipeline_config ALTER COLUMN file_format SET DEFAULT 'delta';
ALTER TABLE framework.pipeline_config ALTER COLUMN file_delimiter SET DEFAULT ',';
ALTER TABLE framework.pipeline_config ALTER COLUMN has_header SET DEFAULT TRUE;
ALTER TABLE framework.pipeline_config ALTER COLUMN schema_evolution SET DEFAULT TRUE;
ALTER TABLE framework.pipeline_config ALTER COLUMN enforce_schema SET DEFAULT FALSE;
ALTER TABLE framework.pipeline_config ALTER COLUMN jdbc_fetch_size SET DEFAULT 10000;
ALTER TABLE framework.pipeline_config ALTER COLUMN jdbc_num_partitions SET DEFAULT 8;
ALTER TABLE framework.pipeline_config ALTER COLUMN max_retries SET DEFAULT 3;
ALTER TABLE framework.pipeline_config ALTER COLUMN retry_wait_seconds SET DEFAULT 60;
ALTER TABLE framework.pipeline_config ALTER COLUMN sla_minutes SET DEFAULT 60;
ALTER TABLE framework.pipeline_config ALTER COLUMN timezone SET DEFAULT 'UTC';
ALTER TABLE framework.pipeline_config ALTER COLUMN dq_rules_enabled SET DEFAULT TRUE;
ALTER TABLE framework.pipeline_config ALTER COLUMN active_flag SET DEFAULT TRUE;
ALTER TABLE framework.pipeline_config ALTER COLUMN created_date SET DEFAULT CURRENT_TIMESTAMP();

-- pipeline_dependency defaults
ALTER TABLE framework.pipeline_dependency ALTER COLUMN dependency_type SET DEFAULT 'hard';
ALTER TABLE framework.pipeline_dependency ALTER COLUMN active_flag SET DEFAULT TRUE;

-- pipeline_audit defaults
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_read SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_written SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_rejected SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_merged SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_inserted SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_updated SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN rows_deleted SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN retry_attempt SET DEFAULT 0;
ALTER TABLE framework.pipeline_audit ALTER COLUMN created_date SET DEFAULT CURRENT_TIMESTAMP();

-- dq_rules defaults
ALTER TABLE framework.dq_rules ALTER COLUMN severity SET DEFAULT 'error';
ALTER TABLE framework.dq_rules ALTER COLUMN threshold_pct SET DEFAULT 0;
ALTER TABLE framework.dq_rules ALTER COLUMN active_flag SET DEFAULT TRUE;
ALTER TABLE framework.dq_rules ALTER COLUMN created_date SET DEFAULT CURRENT_TIMESTAMP();

-- dq_results defaults
ALTER TABLE framework.dq_results ALTER COLUMN created_date SET DEFAULT CURRENT_TIMESTAMP();

-- quarantine defaults
ALTER TABLE framework.quarantine ALTER COLUMN is_reprocessed SET DEFAULT FALSE;
ALTER TABLE framework.quarantine ALTER COLUMN created_date SET DEFAULT CURRENT_TIMESTAMP();

-- schema_registry defaults
ALTER TABLE framework.schema_registry ALTER COLUMN is_current SET DEFAULT TRUE;
ALTER TABLE framework.schema_registry ALTER COLUMN detected_date SET DEFAULT CURRENT_TIMESTAMP();

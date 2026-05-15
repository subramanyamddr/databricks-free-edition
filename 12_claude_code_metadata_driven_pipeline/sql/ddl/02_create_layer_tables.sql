-- ============================================================
-- FILE: 02_create_layer_tables.sql
-- PURPOSE: Bronze / Silver / Gold Delta table templates
--          and standard views for each layer
-- ============================================================

-- -------------------------------------------------------
-- BRONZE LAYER — raw ingestion, append-only
-- -------------------------------------------------------
USE CATALOG bronze_catalog;
CREATE SCHEMA IF NOT EXISTS bronze_catalog.raw;

-- Generic Bronze template — actual tables created dynamically by framework
-- This shows the standard column pattern every Bronze table follows

/*
CREATE TABLE IF NOT EXISTS bronze_catalog.raw.<source_table> (
    -- All source columns preserved as-is (schema evolution enabled)
    -- Plus framework metadata columns below:
    _src_system         STRING      NOT NULL,   -- source system identifier
    _src_object         STRING      NOT NULL,   -- source table/file/topic
    _ingestion_ts       TIMESTAMP   NOT NULL,   -- when record was ingested
    _run_id             STRING      NOT NULL,   -- links back to pipeline_audit
    _pipeline_id        BIGINT      NOT NULL,
    _file_name          STRING,                 -- for file-based sources
    _kafka_offset       BIGINT,                 -- for streaming sources
    _kafka_partition    INT,                    -- for streaming sources
    _is_deleted         BOOLEAN     DEFAULT FALSE  -- for CDC sources
)
USING DELTA
PARTITIONED BY (_ingestion_ts::DATE)
TBLPROPERTIES (
    'delta.enableChangeDataFeed'            = 'true',
    'delta.autoOptimize.optimizeWrite'      = 'true',
    'delta.autoOptimize.autoCompact'        = 'true',
    'delta.logRetentionDuration'            = 'interval 30 days',
    'delta.deletedFileRetentionDuration'    = 'interval 7 days'
);
*/

-- -------------------------------------------------------
-- SILVER LAYER — conformed, cleansed, merged
-- -------------------------------------------------------
USE CATALOG silver_catalog;
CREATE SCHEMA IF NOT EXISTS silver_catalog.conformed;

/*
Standard Silver table template:

CREATE TABLE IF NOT EXISTS silver_catalog.conformed.<entity_name> (
    -- Business columns (conformed, cleansed, typed)
    -- ...
    -- Framework metadata
    _src_system         STRING      NOT NULL,
    _pipeline_id        BIGINT      NOT NULL,
    _run_id             STRING      NOT NULL,
    _valid_from         TIMESTAMP   NOT NULL,   -- SCD2 start
    _valid_to           TIMESTAMP,              -- SCD2 end (NULL = current)
    _is_current         BOOLEAN     DEFAULT TRUE,
    _is_deleted         BOOLEAN     DEFAULT FALSE,
    _created_ts         TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
    _updated_ts         TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
    _checksum           STRING      -- MD5 of business key columns, used to detect changes
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
);
*/

-- -------------------------------------------------------
-- GOLD LAYER — consumption-ready aggregated tables
-- -------------------------------------------------------
USE CATALOG gold_catalog;
CREATE SCHEMA IF NOT EXISTS gold_catalog.reporting;
CREATE SCHEMA IF NOT EXISTS gold_catalog.data_science;
CREATE SCHEMA IF NOT EXISTS gold_catalog.finance;

-- -------------------------------------------------------
-- MONITORING VIEWS
-- -------------------------------------------------------
USE CATALOG framework_catalog;
USE SCHEMA framework;

-- View: Pipeline run summary dashboard
CREATE OR REPLACE VIEW framework.v_pipeline_run_summary AS
SELECT
    pc.pipeline_name,
    pc.pipeline_group,
    pc.source_type,
    pc.target_layer,
    pa.run_date,
    pa.status,
    COUNT(*)                                    AS total_runs,
    SUM(CASE WHEN pa.status = 'success' THEN 1 ELSE 0 END)  AS successful_runs,
    SUM(CASE WHEN pa.status = 'failed'  THEN 1 ELSE 0 END)  AS failed_runs,
    AVG(pa.duration_seconds)                    AS avg_duration_sec,
    MAX(pa.duration_seconds)                    AS max_duration_sec,
    SUM(pa.rows_written)                        AS total_rows_written,
    SUM(pa.rows_rejected)                       AS total_rows_rejected,
    MAX(pa.end_time)                            AS last_run_time
FROM framework.pipeline_audit pa
JOIN framework.pipeline_config pc ON pa.pipeline_id = pc.pipeline_id
GROUP BY ALL;

-- View: SLA breach detection
CREATE OR REPLACE VIEW framework.v_sla_breaches AS
SELECT
    pc.pipeline_name,
    pc.pipeline_group,
    pc.sla_minutes,
    pa.run_date,
    pa.start_time,
    pa.end_time,
    pa.duration_seconds,
    ROUND(pa.duration_seconds / 60.0, 2)        AS duration_minutes,
    pa.status,
    CASE
        WHEN pa.duration_seconds > (pc.sla_minutes * 60) THEN 'BREACHED'
        WHEN pa.duration_seconds > (pc.sla_minutes * 60 * 0.8) THEN 'AT_RISK'
        ELSE 'OK'
    END AS sla_status
FROM framework.pipeline_audit pa
JOIN framework.pipeline_config pc ON pa.pipeline_id = pc.pipeline_id
WHERE pa.run_date >= CURRENT_DATE() - INTERVAL 7 DAYS;

-- View: Data quality summary
CREATE OR REPLACE VIEW framework.v_dq_summary AS
SELECT
    pc.pipeline_name,
    dr.rule_name,
    dr.rule_type,
    dr.column_name,
    dr.run_date,
    dr.total_records,
    dr.passed_records,
    dr.failed_records,
    dr.failure_pct,
    dr.status
FROM framework.dq_results dr
JOIN framework.pipeline_config pc ON dr.pipeline_id = pc.pipeline_id
WHERE dr.run_date >= CURRENT_DATE() - INTERVAL 7 DAYS;

-- View: Active pipeline catalog (operational view)
CREATE OR REPLACE VIEW framework.v_active_pipelines AS
SELECT
    pc.pipeline_id,
    pc.pipeline_name,
    pc.pipeline_group,
    pc.source_type,
    pc.load_strategy,
    pc.target_layer,
    CONCAT(pc.target_catalog, '.', pc.target_schema, '.', pc.target_table) AS full_target_name,
    pc.schedule_cron,
    pc.sla_minutes,
    pc.watermark_value,
    pa.last_run_time,
    pa.last_status
FROM framework.pipeline_config pc
LEFT JOIN (
    SELECT
        pipeline_id,
        end_time AS last_run_time,
        status AS last_status
    FROM framework.pipeline_audit
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pipeline_id ORDER BY end_time DESC) = 1
) pa ON pc.pipeline_id = pa.pipeline_id
WHERE pc.active_flag = TRUE;

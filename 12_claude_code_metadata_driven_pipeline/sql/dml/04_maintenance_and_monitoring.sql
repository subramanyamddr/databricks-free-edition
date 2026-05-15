-- ============================================================
-- FILE: 04_maintenance_and_monitoring.sql
-- PURPOSE: Delta table maintenance, SLA monitoring, operational queries
--          Run these via Databricks SQL Warehouse or scheduled jobs
-- ============================================================

-- -------------------------------------------------------
-- DELTA TABLE MAINTENANCE
-- Run daily after main pipeline completes
-- -------------------------------------------------------

-- Optimize Bronze tables (Z-ORDER on common filter columns)
OPTIMIZE bronze_catalog.raw.crm_customers
    ZORDER BY (CustomerId, ModifiedDate);

OPTIMIZE bronze_catalog.raw.finance_gl_transactions
    ZORDER BY (TRANSACTION_ID, TRANSACTION_DATE, CUSTOMER_ID);

OPTIMIZE bronze_catalog.raw.digital_clickstream
    ZORDER BY (user_id, event_date, event_type);

-- Optimize Silver tables
OPTIMIZE silver_catalog.conformed.crm_customers
    ZORDER BY (customer_id);

OPTIMIZE silver_catalog.conformed.finance_gl_transactions
    ZORDER BY (transaction_id, transaction_date, customer_id);

-- Optimize Gold tables
OPTIMIZE gold_catalog.reporting.fact_customer_sales;
OPTIMIZE gold_catalog.finance.monthly_revenue_summary;

-- Vacuum — remove old versions (keep 7 days for time travel)
SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;

VACUUM bronze_catalog.raw.crm_customers                  RETAIN 168 HOURS;
VACUUM bronze_catalog.raw.finance_gl_transactions        RETAIN 168 HOURS;
VACUUM silver_catalog.conformed.crm_customers            RETAIN 168 HOURS;
VACUUM silver_catalog.conformed.finance_gl_transactions  RETAIN 168 HOURS;
VACUUM gold_catalog.reporting.fact_customer_sales        RETAIN 168 HOURS;

-- -------------------------------------------------------
-- OPERATIONAL MONITORING QUERIES
-- Use these in Power BI / Databricks SQL dashboards
-- -------------------------------------------------------

-- 1. Pipeline health — last 7 days
SELECT
    pipeline_name,
    pipeline_group,
    target_layer,
    run_date,
    status,
    rows_written,
    rows_rejected,
    ROUND(duration_seconds / 60.0, 1)  AS duration_min,
    watermark_end,
    CASE
        WHEN status = 'failed'    THEN '🔴 Failed'
        WHEN status = 'running'   THEN '🟡 Running'
        WHEN status = 'success'   THEN '🟢 Success'
        ELSE '⚪ Unknown'
    END                                 AS health_indicator
FROM framework_catalog.framework.v_pipeline_run_summary
WHERE run_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY run_date DESC, pipeline_name;

-- 2. SLA breach report
SELECT
    pipeline_name,
    pipeline_group,
    sla_minutes,
    run_date,
    ROUND(duration_minutes, 1)          AS actual_minutes,
    sla_status,
    ROUND(duration_minutes - sla_minutes, 1) AS over_sla_by_min
FROM framework_catalog.framework.v_sla_breaches
WHERE sla_status IN ('BREACHED', 'AT_RISK')
ORDER BY run_date DESC, over_sla_by_min DESC;

-- 3. Data quality issues
SELECT
    pipeline_name,
    rule_name,
    rule_type,
    column_name,
    run_date,
    total_records,
    failed_records,
    ROUND(failure_pct, 2)               AS failure_pct,
    status
FROM framework_catalog.framework.v_dq_summary
WHERE status IN ('fail', 'warning')
ORDER BY run_date DESC, failure_pct DESC;

-- 4. Quarantine summary — unprocessed bad records
SELECT
    pipeline_name,
    source_table,
    error_type,
    run_date,
    COUNT(*)                            AS quarantine_count,
    MIN(created_date)                   AS first_failure,
    MAX(created_date)                   AS last_failure
FROM framework_catalog.framework.quarantine
WHERE is_reprocessed = FALSE
GROUP BY pipeline_name, source_table, error_type, run_date
ORDER BY run_date DESC, quarantine_count DESC;

-- 5. Schema drift history
SELECT
    pc.pipeline_name,
    sr.source_object,
    sr.schema_version,
    sr.is_current,
    sr.change_summary,
    sr.detected_date
FROM framework_catalog.framework.schema_registry sr
JOIN framework_catalog.framework.pipeline_config pc
    ON sr.pipeline_id = pc.pipeline_id
WHERE sr.schema_version > 1     -- only show tables that have drifted at least once
ORDER BY sr.detected_date DESC;

-- 6. Watermark positions — current state of all incremental pipelines
SELECT
    pipeline_name,
    pipeline_group,
    source_type,
    source_object,
    watermark_column,
    watermark_value                     AS current_watermark,
    modified_date                       AS watermark_last_updated
FROM framework_catalog.framework.pipeline_config
WHERE load_strategy IN ('incremental', 'cdc')
  AND active_flag = TRUE
ORDER BY pipeline_group, pipeline_name;

-- 7. Row count validation — compare Bronze to Silver
WITH bronze_counts AS (
    SELECT 'crm_customers' AS table_name, COUNT(*) AS row_count
    FROM bronze_catalog.raw.crm_customers
    UNION ALL
    SELECT 'finance_gl_transactions', COUNT(*)
    FROM bronze_catalog.raw.finance_gl_transactions
),
silver_counts AS (
    SELECT 'crm_customers' AS table_name, COUNT(*) AS row_count
    FROM silver_catalog.conformed.crm_customers WHERE _is_current = TRUE
    UNION ALL
    SELECT 'finance_gl_transactions', COUNT(*)
    FROM silver_catalog.conformed.finance_gl_transactions WHERE _is_current = TRUE
)
SELECT
    b.table_name,
    b.row_count                         AS bronze_rows,
    s.row_count                         AS silver_rows,
    b.row_count - s.row_count           AS delta,
    ROUND((s.row_count / b.row_count) * 100, 2) AS silver_coverage_pct
FROM bronze_counts b
JOIN silver_counts s ON b.table_name = s.table_name;

-- 8. Delta table health check
SELECT
    table_catalog,
    table_schema,
    table_name,
    last_altered,
    table_rows
FROM system.information_schema.tables
WHERE table_catalog IN ('bronze_catalog', 'silver_catalog', 'gold_catalog')
ORDER BY table_catalog, table_schema, table_name;

-- -------------------------------------------------------
-- REPROCESSING — replay quarantined records
-- -------------------------------------------------------

-- Step 1: Review what's in quarantine
SELECT *
FROM framework_catalog.framework.quarantine
WHERE pipeline_name = 'sqlserver_crm_customers_incremental'
  AND is_reprocessed = FALSE
  AND run_date = '2024-01-15'
LIMIT 100;

-- Step 2: After fixing root cause, mark records for reprocessing
-- Then trigger the pipeline manually via Databricks Workflows API

-- Step 3: Mark as reprocessed
UPDATE framework_catalog.framework.quarantine
SET
    is_reprocessed      = TRUE,
    reprocessed_date    = CURRENT_TIMESTAMP()
WHERE pipeline_name = 'sqlserver_crm_customers_incremental'
  AND is_reprocessed  = FALSE
  AND run_date        = '2024-01-15';

-- -------------------------------------------------------
-- DELTA TIME TRAVEL — restore previous state
-- -------------------------------------------------------

-- Check Delta history
DESCRIBE HISTORY silver_catalog.conformed.crm_customers;

-- Read as of specific version
SELECT *
FROM silver_catalog.conformed.crm_customers VERSION AS OF 10
WHERE customer_id = 12345;

-- Restore table to previous version (use carefully in production)
-- RESTORE TABLE silver_catalog.conformed.crm_customers TO VERSION AS OF 10;

-- ============================================================
-- FILE: 03_seed_pipeline_config.sql
-- PURPOSE: Sample pipeline configurations for different source types
--          Insert these rows to onboard new pipelines — NO CODE CHANGES
-- ============================================================

USE CATALOG framework_catalog;
USE SCHEMA framework;

-- -------------------------------------------------------
-- SOURCE TYPE: SQL Server — Incremental load
-- -------------------------------------------------------
INSERT INTO framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_database, source_schema, source_object,
    watermark_column, watermark_value, watermark_data_type,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, partition_columns, primary_keys, write_mode,
    jdbc_fetch_size, jdbc_num_partitions, jdbc_partition_column,
    jdbc_lower_bound, jdbc_upper_bound,
    sla_minutes, schedule_cron, notification_email,
    active_flag, created_by, comments
) VALUES (
    'sqlserver_crm_customers_incremental',
    'crm',
    'sqlserver',
    'kv-secret-sqlserver-crm-connstr',  -- Key Vault secret name
    'CRM_DB',
    'dbo',
    'Customers',
    'ModifiedDate',
    '1900-01-01 00:00:00',
    'datetime',
    'incremental',
    'bronze_catalog', 'raw', 'crm_customers',
    'bronze',
    NULL,
    'CustomerId',
    'append',
    50000, 8, 'CustomerId', 1, 10000000,
    30,
    '0 6 * * *',
    'data-alerts@company.com',
    TRUE,
    'data_engineering_team',
    'CRM customer master — incremental by ModifiedDate'
);

-- -------------------------------------------------------
-- SOURCE TYPE: SQL Server — Full load
-- -------------------------------------------------------
INSERT INTO framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_database, source_schema, source_object,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, primary_keys, write_mode,
    sla_minutes, schedule_cron, notification_email,
    active_flag, created_by, comments
) VALUES (
    'sqlserver_ref_country_codes_full',
    'reference_data',
    'sqlserver',
    'kv-secret-sqlserver-ref-connstr',
    'REF_DB',
    'dbo',
    'CountryCodes',
    'full',
    'bronze_catalog', 'raw', 'ref_country_codes',
    'bronze',
    'CountryCode',
    'overwrite',
    15,
    '0 1 * * *',
    'data-alerts@company.com',
    TRUE,
    'data_engineering_team',
    'Reference country codes — small table, full reload daily'
);

-- -------------------------------------------------------
-- SOURCE TYPE: Oracle — Incremental with custom query
-- -------------------------------------------------------
INSERT INTO framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_database, source_schema, source_object,
    source_query_override,
    watermark_column, watermark_value, watermark_data_type,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, primary_keys, write_mode,
    jdbc_fetch_size, jdbc_num_partitions,
    sla_minutes, schedule_cron, notification_email,
    active_flag, created_by, comments
) VALUES (
    'oracle_finance_gl_transactions_incremental',
    'finance',
    'oracle',
    'kv-secret-oracle-finance-connstr',
    'FINDB',
    'GL_OWNER',
    'GL_TRANSACTIONS',
    'SELECT t.*, a.ACCOUNT_NAME FROM GL_OWNER.GL_TRANSACTIONS t JOIN GL_OWNER.ACCOUNTS a ON t.ACCOUNT_ID = a.ACCOUNT_ID',
    'LAST_UPDATED_TS',
    '1900-01-01 00:00:00',
    'datetime',
    'incremental',
    'bronze_catalog', 'raw', 'finance_gl_transactions',
    'bronze',
    'TRANSACTION_ID',
    'append',
    100000, 16,
    60,
    '0 */4 * * *',
    'finance-data@company.com',
    TRUE,
    'data_engineering_team',
    'GL Transactions — custom query with account name join, every 4 hours'
);

-- -------------------------------------------------------
-- SOURCE TYPE: ADLS Blob — CSV files
-- -------------------------------------------------------
INSERT INTO framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_object, file_path_pattern, file_format, file_delimiter, has_header,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, primary_keys, write_mode,
    schema_evolution,
    sla_minutes, schedule_cron, notification_email,
    active_flag, created_by, comments
) VALUES (
    'blob_hr_employee_export_csv',
    'hr',
    'blob',
    'kv-secret-adls-storage-key',
    'hr_employee_export',
    'abfss://raw@storageaccount.dfs.core.windows.net/hr/employees/yyyy=*/mm=*/dd=*/*.csv',
    'csv',
    ',',
    TRUE,
    'incremental',
    'bronze_catalog', 'raw', 'hr_employees',
    'bronze',
    'EmployeeId',
    'merge',
    TRUE,
    45,
    '30 7 * * *',
    'hr-data@company.com',
    TRUE,
    'data_engineering_team',
    'HR employee extract — daily CSV drop on ADLS'
);

-- -------------------------------------------------------
-- SOURCE TYPE: Event Hubs — Streaming
-- -------------------------------------------------------
INSERT INTO framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_object,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, primary_keys, write_mode,
    sla_minutes, notification_email,
    active_flag, created_by, comments
) VALUES (
    'eventhub_clickstream_streaming',
    'digital',
    'eventhub',
    'kv-secret-eventhub-clickstream-connstr',
    'clickstream-events',   -- Event Hub topic name
    'streaming',
    'bronze_catalog', 'raw', 'digital_clickstream',
    'bronze',
    'event_id',
    'append',
    5,
    'streaming-alerts@company.com',
    TRUE,
    'data_engineering_team',
    'Real-time clickstream from web/mobile — continuous streaming ingestion'
);

-- -------------------------------------------------------
-- DQ RULES for CRM Customers pipeline
-- -------------------------------------------------------
INSERT INTO framework.dq_rules (pipeline_id, rule_name, rule_type, column_name, rule_expression, severity, threshold_pct)
SELECT
    pc.pipeline_id,
    'customer_id_not_null',
    'not_null',
    'CustomerId',
    'CustomerId IS NOT NULL',
    'error',
    0.0
FROM framework.pipeline_config pc WHERE pc.pipeline_name = 'sqlserver_crm_customers_incremental';

INSERT INTO framework.dq_rules (pipeline_id, rule_name, rule_type, column_name, rule_expression, severity, threshold_pct)
SELECT
    pc.pipeline_id,
    'customer_email_format',
    'regex',
    'Email',
    "Email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
    'warning',
    5.0   -- allow up to 5% failure before alerting
FROM framework.pipeline_config pc WHERE pc.pipeline_name = 'sqlserver_crm_customers_incremental';

INSERT INTO framework.dq_rules (pipeline_id, rule_name, rule_type, column_name, rule_expression, severity, threshold_pct)
SELECT
    pc.pipeline_id,
    'customer_status_valid_values',
    'range',
    'Status',
    "Status IN ('Active','Inactive','Pending','Suspended')",
    'error',
    0.0
FROM framework.pipeline_config pc WHERE pc.pipeline_name = 'sqlserver_crm_customers_incremental';

-- -------------------------------------------------------
-- Pipeline dependencies (CRM → Finance flow)
-- -------------------------------------------------------
INSERT INTO framework.pipeline_dependency (pipeline_id, depends_on_id, dependency_type)
SELECT
    fin.pipeline_id,
    crm.pipeline_id,
    'hard'
FROM framework.pipeline_config crm
JOIN framework.pipeline_config fin
    ON crm.pipeline_name = 'sqlserver_crm_customers_incremental'
   AND fin.pipeline_name = 'oracle_finance_gl_transactions_incremental';

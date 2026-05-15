# Azure Databricks — Metadata-Driven Pipeline Framework
## Production Grade | Version 1.0

---

## Project Structure

```
databricks_framework/
│
├── sql/
│   ├── ddl/
│   │   ├── 01_create_control_tables.sql      # Framework metadata tables (run once)
│   │   └── 02_create_layer_tables.sql        # Bronze/Silver/Gold DDL templates + views
│   └── dml/
│       ├── 03_seed_pipeline_config.sql       # Onboard new pipelines (insert rows only)
│       └── 04_maintenance_and_monitoring.sql # OPTIMIZE, VACUUM, monitoring queries
│
├── notebooks/
│   ├── utils/
│   │   ├── framework_utils.py               # Core: secrets, audit, schema registry, notifications
│   │   └── dq_engine.py                     # Data quality rules engine
│   ├── ingestion/
│   │   └── bronze_ingestion_orchestrator.py # Handles SQL Server, Oracle, Blob, Event Hubs
│   └── transformation/
│       ├── silver_transformation_engine.py  # SCD1/SCD2 MERGE, cleansing, business rules
│       └── gold_aggregation_engine.py       # Aggregations, star schema, BI/DS/Finance tables
│
├── orchestration/
│   └── databricks_workflow.json             # Databricks Workflow DAG definition
│
└── README.md
```

---

## How to Onboard a New Source (Zero Code Changes)

Just insert one row into `framework.pipeline_config`:

```sql
INSERT INTO framework_catalog.framework.pipeline_config (
    pipeline_name, pipeline_group, source_type, source_connection_key,
    source_database, source_schema, source_object,
    watermark_column, watermark_value, watermark_data_type,
    load_strategy, target_catalog, target_schema, target_table,
    target_layer, primary_keys, write_mode, active_flag, created_by
) VALUES (
    'sqlserver_new_source_incremental', 'new_domain', 'sqlserver',
    'kv-secret-newsource-connstr', 'NEW_DB', 'dbo', 'NewTable',
    'UpdatedAt', '1900-01-01', 'datetime',
    'incremental', 'bronze_catalog', 'raw', 'new_source_table',
    'bronze', 'Id', 'append', TRUE, 'your_name'
);
```

That's it. The framework will ingest it on the next run.

---

## Architecture Decisions

| Decision | Choice | Reason |
|---|---|---|
| Metadata store | Delta table | ACID, versioned, queryable |
| Secret management | Azure Key Vault via Databricks secret scope | Zero hardcoded credentials |
| Schema evolution | mergeSchema=true at Bronze | Absorbs source changes gracefully |
| Idempotency | MERGE at Silver/Gold | Safe reruns, no duplicates |
| Streaming | Structured Streaming + checkpoints | Exactly-once semantics |
| File ingestion | Auto Loader | Efficient incremental file detection |
| DQ bad records | Quarantine Delta table | Review and reprocess without pipeline changes |
| Notifications | Teams webhook + email | Real-time alerting |
| Audit | pipeline_audit Delta table | Full lineage and SLA tracking |

---

## Supported Source Types

| source_type | Technology | Load Strategies |
|---|---|---|
| sqlserver | SQL Server (JDBC) | full, incremental |
| oracle | Oracle (JDBC) | full, incremental |
| blob | ADLS Gen2 (CSV/Parquet/JSON) | full, incremental (Auto Loader) |
| eventhub | Azure Event Hubs | streaming |
| delta | Delta table | full, incremental (CDF) |

---

## Key Vault Secrets Required

| Secret Name Pattern | Contains |
|---|---|
| kv-secret-{system}-connstr | JDBC connection string |
| kv-secret-adls-storage-key | Storage account key |
| kv-secret-eventhub-{topic}-connstr | Event Hub connection string |
| kv-secret-teams-webhook | Teams notification webhook URL |

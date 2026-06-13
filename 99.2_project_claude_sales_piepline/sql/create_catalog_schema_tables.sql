-- =============================================================================
-- sql/create_catalog_schema_tables.sql
-- Run once per environment via Databricks SQL editor or a setup notebook.
-- Replace ${catalog} with: dev_catalog | qa_catalog | prod_catalog
-- =============================================================================

-- ── 1. Catalog ────────────────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS ${catalog}
COMMENT 'Sales pipeline catalog — ${env}';

-- ── 2. Schemas ────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
  COMMENT 'Raw ingested data — no transformations';

CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
  COMMENT 'Cleaned, typed, deduplicated data';

CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
  COMMENT 'Aggregated analytics layer';

CREATE SCHEMA IF NOT EXISTS ${catalog}.quarantine
  COMMENT 'Rows that failed DQX data-quality checks';

-- ── 3. Bronze table ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.sales_raw (
  order_id      STRING        COMMENT 'Raw order ID (unvalidated)',
  customer_id   STRING        COMMENT 'Raw customer ID',
  product       STRING        COMMENT 'Product name',
  quantity      STRING        COMMENT 'Quantity ordered (raw string)',
  unit_price    STRING        COMMENT 'Unit price (raw string)',
  order_date    STRING        COMMENT 'Order date string (yyyy-MM-dd)',
  region        STRING        COMMENT 'Sales region',
  _ingested_at  TIMESTAMP     COMMENT 'Pipeline ingest timestamp (UTC)',
  _source_file  STRING        COMMENT 'Source ADLS file path',
  _pipeline_run STRING        COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Raw sales data from CSV — Bronze layer'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'              = 'true',
  'delta.autoOptimize.optimizeWrite'        = 'true',
  'delta.autoOptimize.autoCompact'          = 'true'
);

-- ── 4. Silver table ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.silver.sales_cleaned (
  order_id      INT           NOT NULL  COMMENT 'Validated order ID',
  customer_id   STRING        NOT NULL  COMMENT 'Normalised customer ID (UPPER TRIM)',
  product       STRING                  COMMENT 'Product name',
  quantity      INT                     COMMENT 'Quantity ordered',
  unit_price    DECIMAL(10,2)           COMMENT 'Unit price in USD',
  order_date    DATE          NOT NULL  COMMENT 'Parsed order date',
  region        STRING                  COMMENT 'Sales region',
  revenue       DECIMAL(10,2)           COMMENT 'quantity × unit_price',
  _ingested_at  TIMESTAMP               COMMENT 'Ingest timestamp from Bronze',
  _updated_at   TIMESTAMP               COMMENT 'Silver upsert timestamp (UTC)',
  _pipeline_run STRING                  COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Cleaned sales data — Silver layer'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'              = 'true',
  'delta.autoOptimize.optimizeWrite'        = 'true',
  'delta.autoOptimize.autoCompact'          = 'true'
);

-- ── 5. Gold table ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.gold.sales_summary (
  summary_date    DATE          NOT NULL  COMMENT 'Aggregation date',
  region          STRING        NOT NULL  COMMENT 'Sales region',
  product         STRING        NOT NULL  COMMENT 'Product name',
  total_orders    INT                     COMMENT 'Count of orders',
  total_quantity  INT                     COMMENT 'Sum of units ordered',
  total_revenue   DECIMAL(12,2)           COMMENT 'Sum of revenue',
  avg_order_value DECIMAL(10,2)           COMMENT 'Average revenue per order',
  _updated_at     TIMESTAMP               COMMENT 'Last refresh timestamp (UTC)',
  _pipeline_run   STRING                  COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Daily sales aggregates — Gold layer'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'              = 'true',
  'delta.autoOptimize.optimizeWrite'        = 'true',
  'delta.autoOptimize.autoCompact'          = 'true'
);

-- ── 6. Quarantine table ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.quarantine.sales_quarantine (
  -- All original columns carried forward
  order_id        STRING,
  customer_id     STRING,
  product         STRING,
  quantity        STRING,
  unit_price      STRING,
  order_date      STRING,
  region          STRING,
  -- DQX failure columns (one per check, nullable)
  _dq_order_id_not_null       STRING,
  _dq_customer_id_not_null    STRING,
  _dq_order_date_not_null     STRING,
  _dq_quantity_positive       STRING,
  _dq_unit_price_positive     STRING,
  _dq_region_valid            STRING,
  -- Quarantine audit
  _quarantine_layer     STRING    COMMENT 'silver | gold',
  _quarantine_ts        TIMESTAMP COMMENT 'When the row was quarantined',
  _pipeline_run         STRING    COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'DQX-failed rows for investigation and reprocessing'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

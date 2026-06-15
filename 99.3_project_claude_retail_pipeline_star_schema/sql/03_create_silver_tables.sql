-- =============================================================================
-- sql/03_create_silver_tables.sql
-- Silver layer — cleaned, typed sales transactions (append-only, deduped).
-- Loaded daily by notebooks/02_silver_load.py after DQX validation.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.silver.sales_cleaned (
  order_id          BIGINT        NOT NULL  COMMENT 'Validated order ID (unique)',
  order_date        DATE          NOT NULL  COMMENT 'Parsed order date',
  customer_id       STRING        NOT NULL  COMMENT 'Normalised customer ID (UPPER/TRIM)',
  customer_name     STRING                  COMMENT 'Customer full name',
  customer_segment  STRING                  COMMENT 'Consumer | Corporate | Home Office',
  customer_city     STRING                  COMMENT 'Customer city',
  customer_state    STRING                  COMMENT 'Customer state code',
  product_id        STRING        NOT NULL  COMMENT 'Normalised product ID',
  product_name      STRING                  COMMENT 'Product name',
  category          STRING                  COMMENT 'Product category',
  sub_category      STRING                  COMMENT 'Product sub-category',
  store_id          STRING        NOT NULL  COMMENT 'Normalised store ID',
  store_name        STRING                  COMMENT 'Store name',
  region            STRING                  COMMENT 'Sales region',
  quantity          INT                     COMMENT 'Quantity ordered (> 0)',
  unit_price        DECIMAL(10,2)           COMMENT 'Unit price in USD (> 0)',
  discount_pct      DECIMAL(5,2)            COMMENT 'Discount percentage (0-100)',
  gross_amount      DECIMAL(12,2)           COMMENT 'quantity * unit_price',
  net_amount        DECIMAL(12,2)           COMMENT 'gross_amount after discount',
  _ingested_at      TIMESTAMP               COMMENT 'Ingest timestamp from Bronze',
  _updated_at       TIMESTAMP               COMMENT 'Silver load timestamp (UTC)',
  _pipeline_run     STRING                  COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Cleaned sales transactions — Silver layer'
PARTITIONED BY (order_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =============================================================================
-- Quarantine table — shared across Silver and Gold DQX failures.
-- Stores the offending row plus DQX result columns and metadata.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.quarantine.sales_quarantine (
  order_id          STRING,
  order_date        STRING,
  customer_id       STRING,
  customer_name     STRING,
  customer_segment  STRING,
  customer_city     STRING,
  customer_state    STRING,
  product_id        STRING,
  product_name      STRING,
  category          STRING,
  sub_category      STRING,
  store_id          STRING,
  store_name        STRING,
  region            STRING,
  quantity          STRING,
  unit_price        STRING,
  discount_pct      STRING,
  gross_amount      STRING,
  net_amount        STRING,
  -- DQX result columns are appended dynamically (errors/warnings as MAP/ARRAY)
  _errors           STRING    COMMENT 'DQX error check results (JSON)',
  _warnings         STRING    COMMENT 'DQX warning check results (JSON)',
  _quarantine_layer STRING    COMMENT 'silver | gold_fact | gold_dim',
  _quarantine_ts    TIMESTAMP COMMENT 'When the row was quarantined',
  _pipeline_run     STRING    COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'DQX-failed rows from Silver and Gold layers'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true'
);

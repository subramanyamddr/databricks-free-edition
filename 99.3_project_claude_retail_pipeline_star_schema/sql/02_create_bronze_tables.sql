-- =============================================================================
-- sql/02_create_bronze_tables.sql
-- Bronze layer table — raw sales data, all columns kept as STRING.
-- Loaded daily by notebooks/01_bronze_ingest.py (append mode).
-- =============================================================================

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.sales_raw (
  order_id          STRING  COMMENT 'Raw order ID (unvalidated)',
  order_date        STRING  COMMENT 'Order date string (yyyy-MM-dd)',
  customer_id       STRING  COMMENT 'Raw customer ID',
  customer_name     STRING  COMMENT 'Customer full name',
  customer_segment  STRING  COMMENT 'Consumer | Corporate | Home Office',
  customer_city     STRING  COMMENT 'Customer city',
  customer_state    STRING  COMMENT 'Customer state code',
  product_id        STRING  COMMENT 'Raw product ID',
  product_name      STRING  COMMENT 'Product name',
  category          STRING  COMMENT 'Product category',
  sub_category      STRING  COMMENT 'Product sub-category',
  store_id          STRING  COMMENT 'Raw store ID',
  store_name        STRING  COMMENT 'Store name',
  region            STRING  COMMENT 'Sales region',
  quantity          STRING  COMMENT 'Quantity ordered (raw string)',
  unit_price        STRING  COMMENT 'Unit price (raw string)',
  discount_pct      STRING  COMMENT 'Discount percentage (raw string, 0-100)',
  _ingested_at      TIMESTAMP COMMENT 'Pipeline ingest timestamp (UTC)',
  _source_file      STRING    COMMENT 'Source ADLS file path',
  _ingest_date      DATE      COMMENT 'Business/process date for this load',
  _pipeline_run     STRING    COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Raw daily sales feed — Bronze layer'
PARTITIONED BY (_ingest_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

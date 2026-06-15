-- =============================================================================
-- sql/04_create_gold_tables.sql
-- Gold layer — star schema dimensional model for BI / reporting.
--
--   dim_date      : pre-populated static date dimension
--   dim_customer  : SCD Type 1, surrogate key via IDENTITY
--   dim_product   : SCD Type 1, surrogate key via IDENTITY
--   dim_store     : SCD Type 1, surrogate key via IDENTITY
--   fact_sales    : one row per order line, FKs to all dimensions
-- =============================================================================

-- ── dim_date ────────────────────────────────────────────────────────────────
-- date_key is deterministic (yyyyMMdd as INT) so fact rows can compute it
-- directly without a dimension lookup. Populated once by
-- notebooks/00_setup_dim_date.py for a wide date range.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_date (
  date_key      INT     NOT NULL  COMMENT 'Surrogate key, format yyyyMMdd',
  full_date     DATE    NOT NULL  COMMENT 'Calendar date',
  year          INT               COMMENT 'Calendar year',
  quarter       INT               COMMENT 'Calendar quarter (1-4)',
  month         INT               COMMENT 'Calendar month (1-12)',
  month_name    STRING            COMMENT 'Month name, e.g. January',
  day_of_month  INT               COMMENT 'Day of month (1-31)',
  day_of_week   INT               COMMENT 'ISO day of week (1=Mon..7=Sun)',
  day_name      STRING            COMMENT 'Day name, e.g. Monday',
  week_of_year  INT               COMMENT 'ISO week number',
  is_weekend    BOOLEAN           COMMENT 'True for Sat/Sun'
)
USING DELTA
COMMENT 'Date dimension — Gold star schema'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ── dim_customer ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_customer (
  customer_key      BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  customer_id       STRING        NOT NULL COMMENT 'Natural/business key',
  customer_name     STRING                 COMMENT 'Customer full name',
  customer_segment  STRING                 COMMENT 'Consumer | Corporate | Home Office',
  customer_city     STRING                 COMMENT 'Customer city',
  customer_state    STRING                 COMMENT 'Customer state code',
  _updated_at       TIMESTAMP              COMMENT 'Last SCD1 update timestamp',
  _pipeline_run     STRING                 COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Customer dimension (SCD Type 1) — Gold star schema'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ── dim_product ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_product (
  product_key   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  product_id    STRING        NOT NULL COMMENT 'Natural/business key',
  product_name  STRING                 COMMENT 'Product name',
  category      STRING                 COMMENT 'Product category',
  sub_category  STRING                 COMMENT 'Product sub-category',
  _updated_at   TIMESTAMP              COMMENT 'Last SCD1 update timestamp',
  _pipeline_run STRING                 COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Product dimension (SCD Type 1) — Gold star schema'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ── dim_store ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_store (
  store_key     BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
  store_id      STRING        NOT NULL COMMENT 'Natural/business key',
  store_name    STRING                 COMMENT 'Store name',
  region        STRING                 COMMENT 'Sales region',
  _updated_at   TIMESTAMP              COMMENT 'Last SCD1 update timestamp',
  _pipeline_run STRING                 COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Store dimension (SCD Type 1) — Gold star schema'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- ── fact_sales ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fact_sales (
  order_id        BIGINT        NOT NULL COMMENT 'Order line natural key (unique)',
  date_key        INT           NOT NULL COMMENT 'FK -> dim_date.date_key',
  customer_key    BIGINT        NOT NULL COMMENT 'FK -> dim_customer.customer_key',
  product_key     BIGINT        NOT NULL COMMENT 'FK -> dim_product.product_key',
  store_key       BIGINT        NOT NULL COMMENT 'FK -> dim_store.store_key',
  quantity        INT                    COMMENT 'Units sold',
  unit_price      DECIMAL(10,2)          COMMENT 'Unit price in USD',
  discount_pct    DECIMAL(5,2)           COMMENT 'Discount percentage applied',
  gross_amount    DECIMAL(12,2)          COMMENT 'quantity * unit_price',
  net_amount      DECIMAL(12,2)          COMMENT 'gross_amount after discount',
  _updated_at     TIMESTAMP              COMMENT 'Last upsert timestamp (UTC)',
  _pipeline_run   STRING                 COMMENT 'Databricks job run ID'
)
USING DELTA
COMMENT 'Sales fact table, grain = one row per order line — Gold star schema'
PARTITIONED BY (date_key)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- =============================================================================
-- sql/01_create_catalog_schemas.sql
-- Creates the Unity Catalog and all schemas (databases) for the retail
-- sales pipeline. Run once per environment.
--
-- Usage (Databricks SQL editor):
--   1. Set the catalog widget/variable for your environment:
--        dev_catalog | qa_catalog | prod_catalog
--   2. Replace ${catalog} below with that value, or run via the
--      00_setup_catalog notebook which substitutes it automatically.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog}
COMMENT 'Retail sales medallion pipeline catalog';

-- Bronze: raw ingested data, append-only, schema mirrors source CSV
CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze
COMMENT 'Raw ingested sales data — no transformations applied';

-- Silver: cleaned, typed, deduplicated, append-only transaction grain
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver
COMMENT 'Cleaned and validated sales transactions';

-- Gold: star schema — dimension and fact tables for reporting
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold
COMMENT 'Star schema dimensional model for BI / reporting';

-- Quarantine: rows that fail DQX data-quality checks in Silver/Gold
CREATE SCHEMA IF NOT EXISTS ${catalog}.quarantine
COMMENT 'Rows that failed DQX checks, held for investigation';

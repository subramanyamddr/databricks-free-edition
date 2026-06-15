# Retail Sales Pipeline — Production Databricks Star Schema

End-to-end medallion pipeline: **daily CSV → Bronze (append) → Silver (append, validated) → Gold (star schema, dims + fact)**

Environment-independent · Databricks Labs **DQX** validation in Silver & Gold · Structured JSON logging to ADLS Gen2 · Deploys to QA via **Databricks Asset Bundles (DAB)**

---

## 1. Star schema design

```
                  ┌────────────────┐
                  │   dim_date      │
                  │ date_key (PK)   │
                  └────────┬────────┘
                           │
┌────────────────┐        │        ┌────────────────┐
│  dim_customer   │        │        │  dim_product    │
│ customer_key(PK)│◄───────┼───────►│ product_key (PK)│
└────────┬────────┘        │        └────────┬────────┘
         │                  │                 │
         │           ┌──────▼───────┐         │
         └──────────►│  fact_sales   │◄────────┘
                      │ order_id      │
                      │ date_key (FK) │
                      │ customer_key  │
                      │ product_key   │
                      │ store_key     │
                      │ quantity      │
                      │ gross_amount  │
                      │ net_amount    │
                      └──────┬────────┘
                             │
                      ┌──────▼────────┐
                      │  dim_store     │
                      │ store_key (PK) │
                      └────────────────┘
```

- **dim_date** — static, pre-populated once for a wide date range (2023–2026 by default)
- **dim_customer / dim_product / dim_store** — SCD Type 1 (last value wins), surrogate keys via `GENERATED ALWAYS AS IDENTITY`
- **fact_sales** — grain = one row per order line, partitioned by `date_key`

---

## 2. Project structure

```
retail_pipeline/
├── databricks.yml                          # DAB root config (dev/qa/prod targets)
├── data/                                    # Sample input — 3 days of CSVs
│   ├── 2024-01-15/sales_20240115.csv
│   ├── 2024-01-16/sales_20240116.csv
│   └── 2024-01-17/sales_20240117.csv
├── conf/
│   ├── base.json                           # Shared defaults
│   ├── dev.json / qa.json / prod.json      # Env overrides
├── notebooks/
│   ├── setup_run_ddl.py                    # ONE-TIME: run all SQL DDL
│   ├── 00_setup_dim_date.py                # ONE-TIME: populate dim_date
│   ├── 01_bronze_ingest.py                 # Daily: CSV -> Bronze
│   ├── 02_silver_load.py                   # Daily: Bronze -> Silver (+ DQX)
│   └── 03_gold_star_schema.py              # Daily: Silver -> Gold star schema (+ DQX)
├── sql/
│   ├── 01_create_catalog_schemas.sql
│   ├── 02_create_bronze_tables.sql
│   ├── 03_create_silver_tables.sql
│   └── 04_create_gold_tables.sql
├── utils/
│   ├── config_loader.py
│   ├── pipeline_logger.py                  # JSON logs -> ADLS Gen2
│   └── dq_checks.py                        # DQX check definitions + runner
├── resources/
│   └── retail_pipeline_job.yml             # Databricks Jobs (daily + setup)
├── init/
│   └── install_dqx.sh                      # Cluster init: installs DQX
├── tests/
│   └── test_pipeline.py                    # pytest unit tests
└── .github/workflows/deploy.yml            # CI/CD: test -> QA -> prod
```

---

## 3. Sample data (3 days, for testing)

| Date | Orders | Designed-in issues |
|------|--------|---------------------|
| 2024-01-15 | 100001–100012 | null `customer_id` (order 100011, error), negative `quantity` (order 100012, error) |
| 2024-01-16 | 100013–100024 | new customers C009/C010, new products P011/P012, `discount_pct=150` (order 100023, error), `unit_price=0` (order 100024, error) |
| 2024-01-17 | 100025–100036 | new customer C011, new product P013, null `quantity` (order 100035, error), future-dated order `2099-12-31` (order 100036, warn) |

This lets you verify: dimension growth (SCD1 inserts on day 2/3), DQX quarantine routing, and the pass-rate threshold logic.

---

## 4. One-time setup (per environment)

### 4.1 Upload sample CSVs to ADLS Gen2

Folder layout the pipeline expects: `<container>/<source_path>/<yyyy-MM-dd>/*.csv`

```bash
az storage blob upload-batch \
  --account-name <storage_account> \
  --destination raw-data/sales \
  --source data/
```

This uploads `data/2024-01-15/...` → `raw-data/sales/2024-01-15/...`, etc.

### 4.2 Upload the init script

```bash
databricks workspace import \
  init/install_dqx.sh \
  /Repos/retail_pipeline/init/install_dqx.sh
```

### 4.3 Deploy the bundle (creates jobs, syncs notebooks)

```bash
databricks bundle deploy --target qa
```

### 4.4 Run DDL setup (catalog, schemas, all tables)

```bash
databricks bundle run setup_run_ddl --target qa
```
> If `setup_run_ddl` isn't registered as a job, run `notebooks/setup_run_ddl.py` directly from the Databricks workspace UI with widget `env=qa`.

### 4.5 Populate dim_date (one time)

```bash
databricks bundle run setup_dim_date --target qa
```

---

## 5. Run the daily pipeline

### Automatic
The job `retail_pipeline_daily_<env>` runs daily at 06:00 IST via cron.

### Manual / backfill (for the 3 sample days)

```bash
databricks bundle run retail_pipeline_daily --target qa \
  --params process_date=2024-01-15

databricks bundle run retail_pipeline_daily --target qa \
  --params process_date=2024-01-16

databricks bundle run retail_pipeline_daily --target qa \
  --params process_date=2024-01-17
```

Each task chain: `bronze_ingest → silver_load → gold_star_schema`.

---

## 6. DQX validation

Implemented via `databricks-labs-dqx` (`DQEngine.apply_checks_by_metadata_and_split`).
Checks are declared in `utils/dq_checks.py`. Rows failing any `error`-criticality
check are written to `<catalog>.quarantine.sales_quarantine` with `_errors` /
`_warnings` JSON columns describing the failure; `warn`-criticality issues are
flagged but the row still proceeds.

### Silver checks (`SILVER_CHECKS`)

| Check | Criticality | On expected sample data |
|-------|-------------|--------------------------|
| `order_id_is_not_null` | error | — |
| `order_date_is_not_null` | error | — |
| `customer_id_is_not_null` | error | catches order 100011 (day 1) |
| `product_id_is_not_null` | error | — |
| `store_id_is_not_null` | error | — |
| `quantity_is_not_null` | error | catches order 100035 (day 3) |
| `quantity_in_valid_range` (1–10000) | error | catches order 100012 (day 1, qty = -2) |
| `unit_price_in_valid_range` (0.01–999999) | error | catches order 100024 (day 2, price = 0) |
| `discount_pct_in_valid_range` (0–100) | error | catches order 100023 (day 2, 150%) |
| `customer_segment_is_valid` | warn | — |
| `order_date_not_in_future` | warn | flags order 100036 (day 3, 2099) |

### Gold checks

- **`GOLD_DIM_CUSTOMER_CHECKS` / `GOLD_DIM_PRODUCT_CHECKS` / `GOLD_DIM_STORE_CHECKS`** — natural key not null + unique, applied to staged dimension rows before MERGE.
- **`GOLD_FACT_CHECKS`** — all four surrogate FKs not null (catches failed dimension lookups), `quantity` in range, `net_amount` ≥ 0 (error), `gross_amount` ≥ 0 (warn).

### Pass-rate threshold

If the pass rate for `error`-criticality checks drops below `dq_min_pass_rate_pct`
(dev 50%, QA 75%, prod 90%), the notebook raises and the job task fails —
halting downstream tasks.

Investigate quarantined rows:
```sql
SELECT * FROM qa_catalog.quarantine.sales_quarantine
ORDER BY _quarantine_ts DESC;
```

---

## 7. Logging (ADLS Gen2)

Every notebook run writes structured JSON logs to:
```
abfss://pipeline-logs@<adls_account>.dfs.core.windows.net/
  <ENV>/retail_pipeline/<LAYER>/<yyyy>/<mm>/<dd>/run_<job_run_id>.jsonl
```

Each record includes timestamp, level, env, layer, run_id, message, and an
`extra` payload (DQ results, row counts, table names).

---

## 8. Deploy to QA (Databricks Asset Bundles)

```bash
pip install databricks-cli
databricks configure --token        # use QA workspace host + PAT/service principal

cd retail_pipeline
databricks bundle validate --target qa
databricks bundle deploy --target qa
databricks bundle run setup_run_ddl --target qa     # one-time
databricks bundle run setup_dim_date --target qa    # one-time
databricks bundle run retail_pipeline_daily --target qa --params process_date=2024-01-15
```

### Automated via GitHub Actions
Push to `main` → unit tests run → on success, bundle is validated and deployed
to QA, `setup_dim_date` runs (idempotent overwrite), and a smoke-test run of
`retail_pipeline_daily` is triggered.

Required GitHub secrets: `DATABRICKS_QA_HOST`, `DATABRICKS_QA_TOKEN`
(and `DATABRICKS_PROD_HOST` / `DATABRICKS_PROD_TOKEN` for releases).

---

## 9. Config reference (`conf/*.json`)

| Key | Default | Description |
|-----|---------|-------------|
| `adls_account` | — | ADLS Gen2 storage account (required) |
| `catalog_name` | — | Unity Catalog name (required) |
| `source_container` | `raw-data` | ADLS container for source CSVs |
| `source_path` | `sales/` | Path prefix; daily folder = `<source_path>/<process_date>/` |
| `silver_watermark_days` | `1` | Lookback window (unused when `process_date` is explicit) |
| `dq_min_pass_rate_pct` | dev 50 / qa 75 / prod 90 | Min DQX pass rate before halting |
| `optimize_on_write` | `false` | Run `OPTIMIZE ... ZORDER` after writes |
| `dim_date_start` / `dim_date_end` | `2023-01-01` / `2026-12-31` | Range for `dim_date` |
| `log_level` | `INFO` (dev: `DEBUG`) | Logger verbosity |

---

## 10. Local testing

```bash
pip install pyspark==3.5.0 pytest pytest-cov delta-spark==3.1.0
cd retail_pipeline
export REPO_ROOT=$(pwd)
export PYTHONPATH=$(pwd)
pytest tests/ -v
```

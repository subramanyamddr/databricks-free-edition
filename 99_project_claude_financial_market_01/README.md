# fin-platform – Databricks Medallion Architecture
## Production-Grade Financial Data Pipeline

---

## Architecture Overview

```
Source Files (CSV)
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  Unity Catalog Volume  /Volumes/<catalog>/landing/raw_ingest/   │
│  ├── trade_executions/file_date=YYYY-MM-DD/*.csv                │
│  └── market_prices/file_date=YYYY-MM-DD/*.csv                   │
└─────────────────────────────┬───────────────────────────────────┘
                              │  Auto Loader (cloudFiles)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE  –  fin_platform_<env>.bronze                           │
│  ├── trade_executions   (all STRING, partitioned by file_date)  │
│  └── market_prices      (all STRING, partitioned by file_date)  │
│  • Append-only  • MD5 row hash  • Source file tracked           │
└─────────────────────────────┬───────────────────────────────────┘
                              │  Cast + Cleanse + MERGE upsert
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER  –  fin_platform_<env>.silver                           │
│  ├── trade_executions   (typed, net_value derived, quarantine)  │
│  ├── market_prices      (typed, daily_return_pct, 52w range)    │
│  ├── quarantine_trade_executions                                │
│  └── quarantine_market_prices                                   │
│  • Upsert on business key  • CDF enabled  • Idempotent          │
└─────────────────────────────┬───────────────────────────────────┘
                              │  Aggregate + Enrich + Join
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD  –  fin_platform_<env>.gold                               │
│  ├── daily_trade_summary        (by instrument / asset class)   │
│  ├── portfolio_daily_pnl        (unrealised + realised P&L)     │
│  └── instrument_market_snapshot (52-week rolling high/low)      │
│  • replaceWhere partition  • Business-ready for analysts        │
└─────────────────────────────────────────────────────────────────┘

AUDIT  –  fin_platform_<env>.audit
├── pipeline_runs     (one row per notebook execution)
└── dq_results        (one row per DQ rule per run)
```

---

## Repository Structure

```
databricks_medallion/
├── 00_setup/
│   ├── 00_setup_catalog_and_schemas.sql   ← Step 1: Run once (admin)
│   ├── 00_setup_volumes.sql               ← Step 2: Run once (admin)
│   └── 00_setup_tables.sql               ← Step 3: Run once (admin)
│
├── 01_bronze/
│   ├── 01_bronze_trade_executions.py      ← Auto Loader ingest
│   └── 01_bronze_market_prices.py
│
├── 02_silver/
│   ├── 02_silver_trade_executions.py      ← Cast, cleanse, MERGE
│   └── 02_silver_market_prices.py
│
├── 03_gold/
│   ├── 03_gold_daily_trade_summary.py     ← Buy/sell aggregation
│   ├── 03_gold_portfolio_pnl.py           ← P&L calculation
│   └── 03_gold_instrument_market_snapshot.py  ← 52-week stats
│
├── 04_orchestration/
│   ├── 04_orchestrate_daily_pipeline.py   ← Master orchestrator
│   ├── job_definition_template.json       ← Databricks Job JSON
│   ├── deploy.sh                          ← Deployment script
│   └── deploy_pipeline.yml               ← GitHub Actions CI/CD
│
└── 05_utils/
    ├── env_config.py                      ← Environment resolver
    ├── logger_utils.py                    ← JSON structured logging
    └── dq_utils.py                        ← Data quality framework
```

---

## Step-by-Step Setup

### Prerequisites
- Databricks workspace (Unity Catalog enabled)
- Azure Data Lake Storage Gen2 (or S3/GCS)
- Databricks CLI v0.18+ installed locally
- Service principal per environment
- GitHub (or Azure DevOps) for CI/CD

---

### Step 1 – Create Unity Catalog Structure

Run as a **Metastore Admin** in the Databricks SQL Editor, substituting `dev`, `uat`, or `prod`:

```sql
-- In Databricks SQL Editor:
SET VARIABLE env = 'dev';   -- change per environment
```

Then execute in order:
1. `00_setup/00_setup_catalog_and_schemas.sql`
2. `00_setup/00_setup_volumes.sql`
3. `00_setup/00_setup_tables.sql`

**What gets created:**
| Object | Name Pattern |
|--------|-------------|
| Catalog | `fin_platform_dev` / `_uat` / `_prod` |
| Schemas | `bronze`, `silver`, `gold`, `audit`, `landing` |
| Volume (landing) | `/Volumes/fin_platform_dev/landing/raw_ingest/` |
| Volume (checkpoints) | `/Volumes/fin_platform_dev/landing/checkpoints/` |
| Bronze tables | `bronze.trade_executions`, `bronze.market_prices` |
| Silver tables | `silver.trade_executions`, `silver.market_prices` |
| Gold tables | `gold.daily_trade_summary`, `gold.portfolio_daily_pnl`, `gold.instrument_market_snapshot` |
| Audit tables | `audit.pipeline_runs`, `audit.dq_results` |

---

### Step 2 – Import Code into Databricks Repos

```bash
# In Databricks UI:
# Workspace → Repos → Add Repo → paste your Git repo URL
# Branch: develop (for dev/uat) | main (for prod)

# OR via CLI:
databricks repos create \
  --url https://github.com/yourorg/fin-platform \
  --provider github \
  --path /Repos/fin-platform/databricks_medallion
```

---

### Step 3 – Configure Databricks Secrets

Store credentials so notebooks never contain plain-text secrets:

```bash
# Create secret scope per environment
databricks secrets create-scope --scope fin-platform-dev

# Store service principal credentials
databricks secrets put --scope fin-platform-dev \
  --key storage-account-key --string-value "<key>"

databricks secrets put --scope fin-platform-dev \
  --key sp-client-secret --string-value "<secret>"
```

Reference in notebooks:
```python
storage_key = dbutils.secrets.get("fin-platform-dev", "storage-account-key")
```

---

### Step 4 – Drop Source Files into Landing Volume

Files must land here before the pipeline runs:

```
/Volumes/fin_platform_dev/landing/raw_ingest/
├── trade_executions/
│   └── file_date=2025-01-13/
│       └── 01_trade_executions_2025-01-13.csv
└── market_prices/
    └── file_date=2025-01-13/
        └── 02_market_prices_2025-01-13.csv
```

Auto Loader tracks processed files in the checkpoint volume – re-dropping the same file does **not** cause duplicates.

---

### Step 5 – Create the Databricks Job

```bash
export DATABRICKS_HOST="https://adb-XXXXX.azuredatabricks.net"
export DATABRICKS_TOKEN="dapiXXXXXXXXXX"
export ENV="dev"

# Substitute env placeholder
sed "s/\${env}/$ENV/g" \
  databricks_medallion/04_orchestration/job_definition_template.json \
  > /tmp/job_dev.json

databricks jobs create --json-file /tmp/job_dev.json
```

Or use the deploy script:
```bash
./databricks_medallion/04_orchestration/deploy.sh --env dev
```

---

### Step 6 – Run the Pipeline

**Manually (one-off backfill):**
```bash
databricks jobs run-now \
  --job-name "fin-platform-daily-pipeline-dev" \
  --job-parameters '{"env":"dev","file_date":"2025-01-13"}'
```

**Scheduled:** The job is configured to run daily at 09:00 UTC via the Quartz cron in the job JSON.

---

### Step 7 – Monitor

**Pipeline audit query:**
```sql
SELECT *
FROM fin_platform_dev.audit.pipeline_runs
ORDER BY started_at DESC
LIMIT 50;
```

**DQ results query:**
```sql
SELECT table_name, rule_name, severity, passed, details, run_date
FROM fin_platform_dev.audit.dq_results
WHERE passed = false
ORDER BY run_date DESC;
```

**Structured logs** (JSON) are captured in Databricks cluster logs and can be forwarded to Splunk / Datadog / Azure Monitor via the log delivery configuration in the workspace.

---

## Environment Promotion Path

```
feature/* branch
    │  (PR + code review)
    ▼
develop branch  ──── GitHub Actions ────▶  DEV workspace
    │                                      • smoke test auto-runs
    │  (PR + release tag)
    ▼
release/* branch ─── GitHub Actions ────▶  UAT workspace
    │                                      • integration tests
    │  (PR to main + MANUAL APPROVAL)
    ▼
main branch ──────── GitHub Actions ────▶  PROD workspace
                                           • PagerDuty alert on failure
                                           • Slack notification on success
```

### Secret Configuration per GitHub Environment
In GitHub Settings → Environments, create `dev`, `uat`, `prod` with:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`  (service principal PAT)
- `SQL_WAREHOUSE_ID`
- `PAGERDUTY_WEBHOOK_ID`  (prod only)
- `SLACK_WEBHOOK_URL`

For **prod**, set "Required reviewers" to enforce the approval gate.

---

## Idempotency & Re-run Safety

| Layer | Mechanism | Re-run safe? |
|-------|-----------|-------------|
| Bronze | Auto Loader checkpoint + `availableNow` trigger | ✅ Same files never re-ingested |
| Silver | MERGE on business key + `_row_hash` guard | ✅ Unchanged rows not re-written |
| Gold | `replaceWhere` on date partition | ✅ Partition fully replaced atomically |

---

## Data Quality Framework

Rules evaluated per run, results written to `audit.dq_results`:

| Severity | Behaviour in PROD | Behaviour in DEV/UAT |
|----------|-------------------|----------------------|
| CRITICAL | Pipeline halts with exception | Logged as ERROR, continues |
| WARNING | Logged as WARNING, continues | Logged as WARNING, continues |

---

## Logging

Every notebook emits **structured JSON logs** to stdout:
```json
{
  "timestamp": "2025-01-13T09:05:12.345Z",
  "level": "INFO",
  "logger": "silver.trade_executions",
  "message": "Stage completed",
  "stage": "silver_merge",
  "event": "end",
  "elapsed_s": 4.321,
  "pipeline": "silver_trade_executions",
  "env": "prod",
  "pid": 12345,
  "host": "10.0.0.5"
}
```

Forward to your SIEM/observability platform via Databricks **Log Delivery** (workspace settings).

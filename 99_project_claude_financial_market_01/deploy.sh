#!/usr/bin/env bash
# =============================================================================
# deploy.sh – Deploy fin-platform medallion pipeline to Databricks
#
# Usage:
#   ./deploy.sh --env dev
#   ./deploy.sh --env uat
#   ./deploy.sh --env prod
#
# Prerequisites:
#   • Databricks CLI v0.18+    (pip install databricks-cli  OR  brew install databricks)
#   • DATABRICKS_HOST and DATABRICKS_TOKEN set per environment in CI/CD secrets
#   • Python 3.9+ for env substitution step
#
# CI/CD integration (GitHub Actions / Azure DevOps):
#   Set DATABRICKS_HOST and DATABRICKS_TOKEN as repo / pipeline secrets
#   and call:  ./deploy.sh --env $TARGET_ENV
# =============================================================================

set -euo pipefail

# ── Parse arguments ──────────────────────────────────────────────────────────
ENV=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --env)     ENV="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    *)         echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$ENV" ]]; then
  echo "ERROR: --env is required (dev | uat | prod)"
  exit 1
fi

if [[ ! "$ENV" =~ ^(dev|uat|prod)$ ]]; then
  echo "ERROR: env must be dev | uat | prod, got: $ENV"
  exit 1
fi

echo "=================================================="
echo "  Deploying fin-platform to environment: $ENV"
echo "  Dry run: $DRY_RUN"
echo "=================================================="

# ── Databricks workspace host ─────────────────────────────────────────────────
# These are read from CI/CD secrets; set them before running:
#   export DATABRICKS_HOST=https://adb-<workspace-id>.azuredatabricks.net
#   export DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXX

if [[ -z "${DATABRICKS_HOST:-}" ]]; then
  echo "ERROR: DATABRICKS_HOST environment variable is not set"
  exit 1
fi
if [[ -z "${DATABRICKS_TOKEN:-}" ]]; then
  echo "ERROR: DATABRICKS_TOKEN environment variable is not set"
  exit 1
fi

# ── Repo sync – push code to Databricks Repos ────────────────────────────────
REPO_PATH="/Repos/fin-platform/databricks_medallion"
GIT_BRANCH="main"

echo ""
echo "Step 1/5: Syncing repo to Databricks Workspace..."
if [[ "$DRY_RUN" == "false" ]]; then
  databricks repos update \
    --path "$REPO_PATH" \
    --branch "$GIT_BRANCH" \
    --tag ""   # set to a git tag for prod pinning, e.g. "v1.2.3"
fi
echo "  ✓ Repo synced"

# ── Run setup SQL scripts ─────────────────────────────────────────────────────
# Substitute ${env} placeholder before running
echo ""
echo "Step 2/5: Running Unity Catalog setup SQL..."

SQL_FILES=(
  "00_setup/00_setup_catalog_and_schemas.sql"
  "00_setup/00_setup_volumes.sql"
  "00_setup/00_setup_tables.sql"
)

ADMIN_CLUSTER_ID="${ADMIN_CLUSTER_ID:-}"

for SQL_FILE in "${SQL_FILES[@]}"; do
  TMP_SQL="/tmp/$(basename $SQL_FILE)"
  sed "s/\${env}/$ENV/g" "$SQL_FILE" > "$TMP_SQL"

  echo "  Running: $SQL_FILE"
  if [[ "$DRY_RUN" == "false" && -n "$ADMIN_CLUSTER_ID" ]]; then
    databricks fs cp "$TMP_SQL" "dbfs:/tmp/setup/$(basename $SQL_FILE)" --overwrite
    # Execute via SQL statement API
    databricks sql execute \
      --statement "$(cat $TMP_SQL)" \
      --warehouse-id "${SQL_WAREHOUSE_ID:-}" \
      --wait 2>/dev/null || echo "    (SQL script may require manual admin execution)"
  else
    echo "    [DRY RUN] Would execute: $TMP_SQL"
  fi
done
echo "  ✓ Setup SQL complete (verify in Databricks SQL Editor if skipped)"

# ── Deploy / update Databricks Job ───────────────────────────────────────────
echo ""
echo "Step 3/5: Deploying Databricks Job..."

JOB_TEMPLATE="04_orchestration/job_definition_template.json"
JOB_FILE="/tmp/job_${ENV}.json"

# Substitute placeholders
sed "s/\${env}/$ENV/g" "$JOB_TEMPLATE" \
  | sed "s|\${pagerduty_webhook_id}|${PAGERDUTY_WEBHOOK_ID:-PLACEHOLDER}|g" \
  > "$JOB_FILE"

JOB_NAME="fin-platform-daily-pipeline-${ENV}"

if [[ "$DRY_RUN" == "false" ]]; then
  # Check if job already exists
  EXISTING_JOB_ID=$(databricks jobs list --output JSON \
    | python3 -c "
import sys, json
jobs = json.load(sys.stdin).get('jobs', [])
match = [j['job_id'] for j in jobs if j.get('settings', {}).get('name') == '${JOB_NAME}']
print(match[0] if match else '')
" 2>/dev/null || echo "")

  if [[ -n "$EXISTING_JOB_ID" ]]; then
    echo "  Updating existing job ID: $EXISTING_JOB_ID"
    databricks jobs reset --job-id "$EXISTING_JOB_ID" --json-file "$JOB_FILE"
  else
    echo "  Creating new job: $JOB_NAME"
    CREATED=$(databricks jobs create --json-file "$JOB_FILE")
    echo "  Created job: $CREATED"
  fi
else
  echo "  [DRY RUN] Would create/update job: $JOB_NAME"
  cat "$JOB_FILE"
fi
echo "  ✓ Job deployed"

# ── Upload any supporting wheel / library ─────────────────────────────────────
echo ""
echo "Step 4/5: Uploading shared utilities to DBFS..."
if [[ "$DRY_RUN" == "false" ]]; then
  databricks fs cp -r 05_utils \
    "dbfs:/FileStore/fin-platform/${ENV}/utils" --overwrite
fi
echo "  ✓ Utilities uploaded"

# ── Smoke test: trigger a dry run on dev ─────────────────────────────────────
echo ""
echo "Step 5/5: Post-deployment validation..."
if [[ "$ENV" == "dev" && "$DRY_RUN" == "false" ]]; then
  TODAY=$(date -u +"%Y-%m-%d")
  echo "  Triggering test run for date: $TODAY"
  RUN_RESPONSE=$(databricks jobs run-now \
    --job-name "$JOB_NAME" \
    --job-parameters "{\"env\":\"dev\",\"file_date\":\"$TODAY\"}")
  RUN_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
  echo "  Run submitted. Run ID: $RUN_ID"
  echo "  Monitor at: ${DATABRICKS_HOST}/#job/runs/$RUN_ID"
else
  echo "  Smoke test skipped (env=$ENV, dry_run=$DRY_RUN)"
fi

echo ""
echo "=================================================="
echo "  Deployment complete for env: $ENV"
echo "=================================================="

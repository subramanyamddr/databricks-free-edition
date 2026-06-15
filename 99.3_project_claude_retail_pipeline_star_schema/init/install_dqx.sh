#!/bin/bash
# =============================================================================
# init/install_dqx.sh
# Cluster init script — installs databricks-labs-dqx on every node.
# Upload to: /Repos/retail_pipeline/init/install_dqx.sh and reference it in
# the job_cluster definition (resources/retail_pipeline_job.yml).
# =============================================================================

set -euo pipefail

echo "[init] Installing databricks-labs-dqx..."
pip install --quiet databricks-labs-dqx==0.14.0

echo "[init] DQX installation complete"

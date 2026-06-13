#!/bin/bash
# =============================================================================
# init/install_dqx.sh
# Databricks cluster init script
# Installs databricks-labs-dqx on every node at cluster startup
# Upload this file to: /Repos/sales_pipeline/init/install_dqx.sh
# =============================================================================

set -euo pipefail

echo "[init] Installing databricks-labs-dqx..."
pip install --quiet databricks-labs-dqx==0.2.0

echo "[init] DQX installation complete"

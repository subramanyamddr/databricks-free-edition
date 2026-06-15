# =============================================================================
# tests/conftest.py
# Shared pytest fixtures.
#
# databricks-labs-dqx's DQEngine verifies its WorkspaceClient by calling
# ws.clusters.select_spark_version() at construction time. On a real
# Databricks cluster this succeeds automatically via the attached identity.
# For local/CI runs (no workspace credentials), we patch
# databricks.sdk.WorkspaceClient with a MagicMock so DQEngine can be
# constructed and the check-application logic (pure DataFrame
# transformations — no actual API calls) can be exercised end-to-end.
# =============================================================================

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def mock_dqx_workspace_client(monkeypatch):
    import databricks.sdk as dbsdk
    monkeypatch.setattr(dbsdk, "WorkspaceClient", lambda *a, **k: MagicMock())

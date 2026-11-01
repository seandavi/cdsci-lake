"""Every test runs against a local lake, whatever the shell exports.

`Settings` reads `CU_OPENALEX_LAKE_BACKEND` from the environment, and the source tests build
`Settings(storage_base_uri=...)` without naming a backend. With `postgres` exported (as the
release units and ad-hoc ingests do), a test's curate + prune would write into, and delete
from, the shared lake. Pin it here, once, for the whole session.
"""

import pytest


@pytest.fixture(autouse=True)
def _local_lake_only(monkeypatch):
    monkeypatch.setenv("CU_OPENALEX_LAKE_BACKEND", "local")


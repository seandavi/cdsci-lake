"""Guards for the SQLMesh transform config (ADR-0019, issue #83).

The `project:` assertion is the load-bearing one: SQLMesh only preserves another
project's models when planning a subset if the planning project names itself
(`any(self._projects)` in sqlmesh/core/context.py). An unset name is "", which
is falsy — so a nameless config silently disables the guard and a plan can drop
another producer's virtual layer. Cheap check, expensive failure.
"""

from __future__ import annotations

import re
from pathlib import Path

CONFIG = Path(__file__).resolve().parents[1] / "transform" / "config.py"


def test_transform_config_sets_a_project_name() -> None:
    assert re.search(r'^\s*project="[^"]+"', CONFIG.read_text(), re.M), (
        "transform/config.py must set a non-empty project= on Config(...)"
    )


def test_transform_config_does_not_default_to_prod() -> None:
    """A bare `sqlmesh plan` must target our own environment, never prod."""
    text = CONFIG.read_text()
    m = re.search(r'default_target_environment="([^"]+)"', text)
    assert m, "transform/config.py must pin default_target_environment"
    assert m.group(1) != "prod", "default_target_environment must not be prod"

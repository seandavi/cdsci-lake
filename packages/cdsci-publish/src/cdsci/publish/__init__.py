"""cdsci.publish -- shared release-bundle mechanics (ADR-0027).

Parquet + DuckDB views + manifest.json, the format bioc-on-ice (ADR-0014) and
omicidx (ADR-0004) already publish. Domain meaning -- table lists, column docs,
provenance, base URL -- stays with the product; this package owns the mechanics.
"""

import json
from importlib import resources

from .bundle import MANIFEST_VERSION, build_db, publish, restore_sql, sql_literal, views_sql
from .concordance import concordance

__all__ = ["MANIFEST_VERSION", "build_db", "concordance", "manifest_schema", "publish",
           "restore_sql", "sql_literal", "views_sql"]


def manifest_schema() -> dict:
    """The manifest.json JSON Schema, loaded from the packaged `manifest.schema.json`."""
    return json.loads(resources.files(__package__).joinpath("manifest.schema.json").read_text())

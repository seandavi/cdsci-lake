"""Flag lake columns named after a bioregistry *synonym* instead of the
canonical prefix (e.g. ``ncbi_taxon_id`` instead of ``ncbitaxon_id``).

The 2026-08-10 naming-convention session established the rule by hand:
external, citable identifiers get the bioregistry canonical prefix
(``ncbitaxon_id``, not ``ncbi_taxon_id``); internally-synthesized join keys
(``study_id``, ``experiment_id``, ``bsdb_id``) aren't bioregistry-namespaced
at all and should never be flagged; a handful of domain-standard names
(``pmid``) are deliberate, reasoned exceptions to "always use the canonical
prefix" (renaming every ``pmid`` to ``pubmed_id`` would make this lake less
recognizable to anyone who's touched PubMed data, not more consistent).

Only flags a column whose ``<prefix>_id[s]`` prefix matches a bioregistry
*synonym* pointing at a **different** canonical prefix. A prefix that
matches nothing in bioregistry at all (``study_id``, ``run_id``, ``bsdb_id``)
is silently skipped — it's not an ontology/database identifier, not a
naming violation. Read-only; reports, doesn't fix.
"""

from __future__ import annotations

import re
import sys

from cdsci.lake.connect import lake_connect

# Deliberate, reasoned exceptions -- see module docstring. Add to this list
# only with the same kind of reasoning already applied to `pmid`, not to
# silence a finding you haven't actually thought about.
ALLOWLIST = {"pmid"}

_ID_COLUMN = re.compile(r"^(.+)_ids?$")


def _synonym_map(con) -> dict[str, str]:
    """``{lowercased synonym: canonical prefix}`` from lake.ref.bioregistry."""
    out: dict[str, str] = {}
    for prefix, synonyms in con.sql(
        "SELECT prefix, synonyms FROM lake.ref.bioregistry WHERE synonyms IS NOT NULL"
    ).fetchall():
        for syn in synonyms.split("|"):
            syn = syn.strip().lower().replace(" ", "_").replace("-", "_")
            if syn and syn != prefix:
                out.setdefault(syn, prefix)
    return out


def main() -> int:
    con = lake_connect(read_only=True)
    synonyms = _synonym_map(con)

    findings = []
    for _db, schema, table, column, _oid in con.sql(
        "SELECT database_name, schema_name, table_name, column_name, table_oid "
        "FROM duckdb_columns() WHERE database_name = 'lake'"
    ).fetchall():
        m = _ID_COLUMN.match(column)
        if not m:
            continue
        candidate = m.group(1).lower()
        if candidate in ALLOWLIST:
            continue
        canonical = synonyms.get(candidate)
        if canonical:
            findings.append((f"{schema}.{table}", column, candidate, canonical))

    if not findings:
        print("no findings -- every _id/_ids column either matches its bioregistry "
              "canonical prefix, isn't bioregistry-namespaced, or is allowlisted")
        return 0

    print(f"{len(findings)} finding(s):\n")
    for table, column, candidate, canonical in sorted(findings):
        print(f"  {table}.{column} -- {candidate!r} is a bioregistry synonym of "
              f"canonical prefix {canonical!r}; consider renaming to {canonical}_id")
    return 1


if __name__ == "__main__":
    sys.exit(main())

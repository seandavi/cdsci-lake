"""One-off smoke test for targets.py's iceberg adapter against the real,
live bioc-on-ice icegate instance. NOT a pytest test (needs a real secret +
network) -- run manually:

    uv run python scripts/smoketest_icegate.py

Writes a scratch table to the `annotation` namespace (write-scoped there per
bioc-on-ice's icegate.yaml), reads it back, then drops it. Prints PASS/FAIL
only -- the token never gets printed or written to disk.
"""

from __future__ import annotations

import subprocess
import sys

from cdsci.lake.connect import lake_connect
from cdsci.lake.transform.targets import Target, publish

CATALOG = "bioconice"
ENDPOINT = "https://icegate-bioconice.seandavi.workers.dev"
NAMESPACE = "annotation"
TABLE = "_cdsci_lake_smoketest"


def main() -> int:
    token = subprocess.run(
        ["gcloud", "secrets", "versions", "access", "latest",
         "--secret=bioconice-icegate-key-seandavi", "--project=cdsci-infra"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    con = lake_connect()
    try:
        con.execute(
            "CREATE SCHEMA IF NOT EXISTS lake.smoketest; "
            "CREATE OR REPLACE TABLE lake.smoketest.genes AS "
            "SELECT * FROM (VALUES "
            "  ('CDSCI0001', 'faux-gene-a', 'chr_test'), "
            "  ('CDSCI0002', 'faux-gene-b', 'chr_test') "
            ") v(gene_id, symbol, chrom)"
        )
        target = Target(
            "iceberg",
            {
                "token": token,
                "catalog": CATALOG,
                "endpoint": ENDPOINT,
                "namespace": NAMESPACE,
                "table": TABLE,
            },
        )
        publish(con, "lake.smoketest.genes", target, date="2026-08-07")
        # publish() detaches its own catalog handle on exit (adapter contract:
        # no lingering attachment) -- re-attach here to verify + clean up.
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute(
            "CREATE OR REPLACE SECRET _verify_ice (TYPE ICEBERG, TOKEN ?);", [token]
        )
        con.execute(
            f"ATTACH '{CATALOG}' AS _verify_cat "
            f"(TYPE ICEBERG, ENDPOINT '{ENDPOINT}', SECRET _verify_ice);"
        )
        try:
            rows = con.execute(
                f"SELECT * FROM _verify_cat.{NAMESPACE}.{TABLE} ORDER BY gene_id"
            ).fetchall()
            expected = [
                ("CDSCI0001", "faux-gene-a", "chr_test"),
                ("CDSCI0002", "faux-gene-b", "chr_test"),
            ]
            if rows != expected:
                print(f"FAIL: read back {rows!r}, expected {expected!r}")
                return 1
            print(f"PASS: wrote + read back {len(rows)} rows via icegate ({CATALOG}.{NAMESPACE}.{TABLE})")

            # Clean up -- this is a smoke test table, not real bioc-on-ice data.
            con.execute(f"DROP TABLE _verify_cat.{NAMESPACE}.{TABLE};")
            print("cleaned up: dropped the scratch table")
            return 0
        finally:
            con.execute("DETACH _verify_cat;")
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())

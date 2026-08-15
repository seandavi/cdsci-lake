#!/usr/bin/env bash
# Interactive DuckDB attached to the sandbox DuckLake (see README.md).
#   ./connect.sh              -> interactive shell, already USEing `lake`
#   ./connect.sh "SELECT 1"   -> run one statement and exit
#
# DATA_PATH must match what the catalog stored at init, hence the absolute path.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
attach="ATTACH 'ducklake:$here/catalog.ducklake' AS lake (DATA_PATH '$here/lake_data'); USE lake;"

if [ $# -gt 0 ]; then
  exec duckdb -c "$attach $*"
fi
exec duckdb -cmd "$attach"

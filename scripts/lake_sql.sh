# Source this (e.g. from ~/.bashrc: `source /path/to/cdsci-lake/scripts/lake_sql.sh`)
# to get a `lake-sql` command: fresh-from-GSM ad-hoc `duckdb` CLI access to the
# prod lake, with real tab-completion against the live catalog -- see
# monode/infrastructure's DUCKLAKE.md and this repo's lake_sql_init.py for why
# this isn't just `duckdb -c "ATTACH ..."` by hand.
#
# Deliberately does NOT use this box's existing persisted `lake`/`pg_main`/`r2`
# secrets (see duckdb_secrets()) -- those are a static, possibly-stale copy;
# this pulls current credentials from GSM every invocation instead, same
# "secrets always from GSM" rule as everything else in this session.
lake-sql() {
  local repo="/home/davsean/Documents/git/cdsci-lake"
  local tmp_dir tmp_sql
  tmp_dir=$(mktemp -d) || return 1
  tmp_sql="$tmp_dir/init.sql"
  # shellcheck disable=SC2064 -- intentionally expand tmp_dir now
  trap "rm -rf '$tmp_dir'; unset PGPASSWORD" RETURN

  mkdir -p /tmp/cdsci-lake-sql-secrets && chmod 700 /tmp/cdsci-lake-sql-secrets

  if ! (cd "$repo" && CU_OPENALEX_LAKE_BACKEND=postgres uv run python scripts/lake_sql_init.py) > "$tmp_sql"; then
    echo "lake-sql: failed to generate init script (see error above)" >&2
    return 1
  fi
  chmod 600 "$tmp_sql"

  if ! PGPASSWORD=$(cd "$repo" && CU_OPENALEX_LAKE_BACKEND=postgres uv run python scripts/lake_sql_init.py --pgpassword); then
    echo "lake-sql: failed to fetch PGPASSWORD" >&2
    return 1
  fi
  export PGPASSWORD

  duckdb -init "$tmp_sql" "$@"
}

# cdsci-lake

The shared **research-data lake** platform for cancerdatasci: a single DuckLake
(Postgres catalog + Cloudflare R2 data) holding canonical, source-faithful
biomedical reference corpora that internal projects consume instead of
re-fetching from APIs.

This repo is a **lake publisher + read client**, a sibling to
[omicidx](https://github.com/seandavi/omicidx) (which publishes pubmed/SRA/GEO
into the same lake). Consuming projects depend on the *data contract*, not this
code — they "assume the data exists".

```
publishers            →   the lake (DuckLake)   →   consumers
  omicidx (public)          Postgres `lake`          research projects
  cdsci-lake (this)         R2  `cdsci-lake`           (read-only)
```

## Two surfaces

```bash
pip install cdsci-lake            # READ CLIENT only (duckdb + pydantic)
pip install cdsci-lake[ingest]    # + the bulk source ingestors (platform side)
```

**Read client** — what consumers use:

```python
from cdsci.lake import lake_connect

con = lake_connect(read_only=True)          # attaches the shared lake
con.execute("SELECT count(*) FROM lake.icite WHERE rcr > 2").fetchall()
con.execute("""
  SELECT p.title FROM lake.omicidx.pubmed_article p
  SEMI JOIN my_cohort c USING (pmid)
""")
```

Credentials come from **Google Secret Manager** (project `cdsci-infra`) via the
`gcloud` CLI — not `.env`. `CU_OPENALEX_LAKE_BACKEND` selects `postgres` (the
shared lake) or `local` (a single-file catalog for dev/test).

**Ingest** — platform-side bulk importers, each writing one source schema via
time-travel-friendly MERGE-upserts:

```bash
python -m cdsci.lake.sources.reporter run --year 2023 --schema _dev
python -m cdsci.lake.sources.icite   run            # monthly figshare snapshot
```

## Layout

```
src/cdsci/lake/
  connect.py            # lake_connect(), upsert(), schema/snapshot helpers
  config.py  secrets.py  download.py
  sources/<src>/        # one canonical corpus each -> a lake schema
docs/adr/               # architecture decisions
docs/design/            # source mapping + import design
```

See `docs/adr/` for the platform decisions (catalog topology, per-source-silver
medallion, MERGE-upsert, credentials, orchestration).

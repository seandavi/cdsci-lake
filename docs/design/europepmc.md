# Europe PMC text-mined annotations (`europepmc`) source

Europe PMC runs text-mining over the PMC full-text corpus and publishes the
extracted entity mentions as a directory of CSVs at
[`europepmc.org/pub/databases/pmc/TextMinedTerms/`](https://europepmc.org/pub/databases/pmc/TextMinedTerms/).
The `europepmc` source loads them into **one tidy lake table**,
`lake.europepmc.annotations`.

## Source = one CSV per annotated database, all the same shape

The directory has ~60 files — one per annotated resource/ontology (`uniprot.csv`,
`chebi.csv`, `nct.csv`, `gen.csv`, `geo.csv`, `pdb.csv`, `refsnp.csv`, …) plus a
`PRIVACY-NOTICE.txt`. Every CSV has the identical four-column shape, the first
column's *header* being the database name:

```
<database>,PMCID,EXTID,SOURCE
"MINT-1777462",PMC3340672,22553621,MED
```

| field | meaning |
|-------|---------|
| col 1 (`accession`) | the term / accession id; header = the database name |
| `PMCID` | the PMC article the term was mined from |
| `EXTID` | the article's id in `SOURCE`'s namespace |
| `SOURCE` | that namespace — `MED` ⟹ `EXTID` is a **PubMed id** |

Because the files share a schema, they collapse into one table; the **database is
promoted to a column** (from the file stem) so all resources live together and a
query can filter `WHERE database = 'nct'` or join across all of them at once.

## One tidy table → `europepmc.annotations`

| column | type | note |
|--------|------|------|
| `database` | VARCHAR | annotated resource (file stem): `uniprot`, `chebi`, `nct`, … |
| `accession` | VARCHAR | the term/accession id within that database |
| `pmcid` | VARCHAR | PMC article id — joins `pmc.documents` |
| `pmid` | BIGINT | the MED `EXTID` (PubMed id); NULL for non-MED annotations — joins `icite`, `reporter.publink`, omicidx |
| `snapshot_version` | VARCHAR | load tag (date), excluded from change-detection |

**MERGE key:** `(database, accession, pmcid)` — one row per (term, article).
Curation reads each file **positionally** (`header=false, skip=1`) so the varying
first-column header is irrelevant, drops rows missing `accession`/`pmcid`, and
**groups by the key** so a file repeating a term for an article yields one row
(`pmid` is the MED external id via a filtered aggregate). Each file MERGE-upserts
into the shared table; an unchanged monthly reload writes nothing (ADR-0003).

## Loading

`list_databases()` scrapes the directory index for `*.csv` stems (so a resource
added upstream is picked up automatically). `python -m cdsci.lake.sources.europepmc`:

- `databases` — list the available annotation databases.
- `run` — download (resumably, into the raw layer) and MERGE every database into
  `lake.europepmc.annotations`. `--database <stem>` loads just one; `--file <path>`
  loads a local CSV; `--version` overrides the snapshot label (default: load date);
  `--limit` caps rows per file for a smoke test.

The run is recorded in the operational ledger via `ops.run` (ADR-0006). Total
bulk is a few hundred MB across the ~60 files (largest: `gen` ~116 MB, `nct`
~43 MB, `refsnp` ~39 MB); files stream to the raw layer and curate one at a time.

## Why it's useful

`pmc.documents`/`pmc.passages` give us the full text; `europepmc.annotations`
gives us **what entities that text mentions**, pre-extracted and normalized to
stable accessions — accession/software/gene/chemical/trial mentions per article
without running our own NER. It is the annotation companion to the PMC corpus
(ADR-0002): e.g. `nct` annotations are a second PMCID↔trial bridge alongside
`ctgov.references`, and `uniprot`/`chebi`/`gen`/`refsnp` support entity-level
literature mining and CFDE-style accession search.

## Open items

- **License** — recorded as `europepmc-terms` in the source registry pending
  confirmation of the exact reuse terms before republishing in the shared lake.
- **Versioning** — the directory has no explicit version; we tag with the load
  date. If Europe PMC adds a dated release, switch `snapshot_version` to it.
- **`pmid` for non-MED rows** — left NULL; `pmcid` remains the reliable link.

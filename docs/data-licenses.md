# Data licenses — per-source terms (carry-forward)

Source data in the lake carries its own license. The machine-readable record is
the `lake_ops.source` registry — `SELECT name, license FROM ops.lake_ops.source` —
which every ingestor seeds. This file is the human-facing summary; **check it
before exporting or sharing any data or derived artifact.**

The lake is **internal and private** (Postgres catalog + private R2); loading data
into it is internal use, not redistribution.

| source | license | redistribute? | notes |
|--------|---------|---------------|-------|
| reporter, icite, ctgov, scp, census_geo | US public domain | yes | US-government works |
| openalex | CC0 | yes | public domain dedication |
| retractionwatch | CC0 (effectively) | yes | attribution appreciated |
| pmc | mixed OA | per-article | BioC-PMC open-access subset; honor per-article terms |
| europepmc | Europe PMC terms | verify | confirm reuse terms before any external sharing |
| **reliance** | **CC BY-NC 4.0** | **NO** | **non-commercial only; do NOT redistribute; attribute Marx** |

## ⚠️ Non-commercial / non-redistributable sources

**Reliance on Science (`reliance.*`) is CC BY-NC 4.0.** Internal non-commercial
research use only (a state university qualifies). **Never** include `reliance.*` —
or any view/extract/join output derived from it — in a public dataset, a shared
download, a published figure's underlying data, or any commercial use. Attribute:
Marx, M., *Reliance on Science* (Zenodo). See `docs/design/reliance.md`.

Practical rule for consumers and export tooling: if a query touches `reliance.*`,
its output inherits CC BY-NC — treat it as non-redistributable. When in doubt,
read the `license` column for every source a query reads.

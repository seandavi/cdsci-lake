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
| hgnc | CC0 | yes | genenames.org/about/license (2026-11-01): "all data is released under the Creative Commons Public Domain (CC0) License" |
| complexportal | CC0 | yes | CC0 per bioc-on-ice's declared table docs (`schemas.py`); EBI's about page was not re-checked 2026-11-01 |
| gwas_catalog | EMBL-EBI Terms of Use | yes, attributed | ebi.ac.uk/gwas/docs/about (2026-11-01): "The NHGRI-EBI GWAS Catalog and all its contents are available under the general Terms of Use for EMBL-EBI Services"; ebi.ac.uk/about/terms-of-use (2026-11-01): "EMBL-EBI itself places no additional restrictions on the use or redistribution of the data available via its Data Resources and Tools other than those provided by the original data owners". Not a formal open licence; cite the Catalog |
| mane | NCBI: no restrictions | yes, attributed | No licence file in ftp.ncbi.nlm.nih.gov/refseq/MANE/. ncbi.nlm.nih.gov/home/about/policies (2026-11-01), Molecular Data Usage: "NCBI itself places no restrictions on the use or distribution of the data contained therein"; "NCBI's Disclaimer and Copyright notice must be evident to users of your service". A joint NCBI / EMBL-EBI product; cite MANE |
| wikipathways | CC0 | yes | wikipathways.org/terms.html (2026-11-01): "WikiPathways content is covered by the Creative Commons CC0 Waiver" |
| reactome | CC0 | yes | reactome.org/license §1(c) covers the *data*; §1(a) CC BY 4.0 covers illustrations/icons only |
| ror | CC0 | yes | ror.readme.io/docs/data-dump: "All ROR IDs and metadata in the data dump are provided under the Creative Commons CC0 1.0 Universal Public Domain Dedication"; Zenodo record carries `license: cc-zero` |
| orcid | CC0 | yes | info.orcid.org/annual-data-files: "ORCID releases the Public Data File under a CC0 1.0 Public Domain Dedication". Same terms for the public API. Only researcher-*public* fields are returned; ORCID's Public Data File Use Policy adds community norms (cite ORCID, honor deletions on refresh), not license conditions |
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

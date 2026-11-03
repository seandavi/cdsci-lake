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
| cellosaurus | CC BY 4.0 | yes, attributed | ftp.expasy.org/databases/cellosaurus/README (2026-11-01): "we have chosen to apply the Creative Commons Attribution 4.0 International (CC BY 4.0) license to Cellosaurus"; "You must give appropriate credit, provide a link to the license, and indicate if changes were made." Credit the CALIPHO group, SIB |
| eqtl_catalogue | CC BY 4.0 | yes, attributed | ebi.ac.uk/eqtl/License (2026-11-03): "All eQTL Catalogue code is distributed under Apache 2.0 license and all its data is distributed under Creative Commons Attribution 4.0 International License." Cite Kerimov et al. Nat Genet 2021 |
| cellxgene | CC BY 4.0 | yes, attributed | cellxgene.cziscience.com/docs/032__Contribute and Publish Data (2026-11-03), the submitter's agreement: "anyone will be able to access it subject to a CC-BY 4.0 license, meaning they can download, share, and use the data without restriction beyond providing attribution to the original data contributor(s)". Attribute each dataset's contributors (its `citation`) |
| encode | ENCODE: no restrictions | yes, attributed | encodeproject.org/help/citing-encode/ (2026-11-03), Data Use Policy for External Users: "External data users may freely download, analyze and publish results based on any ENCODE data without restrictions."; "We request that researchers who use ENCODE datasets (published or unpublished) in publications and presentations cite the ENCODE Consortium". No licence instrument named; cite the ENCODE Consortium as that page asks |
| bedbase | per-record DUO code; all DUO:0000042 (general research use) | yes, attributed | No site-wide data licence found on bedbase.org or docs.bedbase.org (2026-11-03); the software (github.com/databio/bedhost) is BSD-2-Clause. Each record carries `license_id`, a Data Use Ontology code, and all 663,721 records of the 2026-09-01 snapshot are DUO:0000042, "general research use". docs.bedbase.org/citations (2026-11-03): "If you use BEDbase, geniml, or their components in your research, here are papers you can cite." Only the catalog lands; the BED files are referenced, never redistributed |
| biogrid | MIT | yes, with the notice | wiki.thebiogrid.org/doku.php/terms_and_conditions (2026-11-03), Terms and Conditions: "Copyright 2021 Mike Tyers Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software"; "The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software." The page carries the MIT License text without naming it; keep the notice with any copy |
| intact | CC BY 4.0 | yes, attributed | ebi.ac.uk/intact/about (License and Privacy; source github.com/Intact-Portal/intact-portal-documentation about/license-privacy.md, 2026-11-03): "The terms of the Creative Commons Attribution 4.0 International (CC BY 4.0) License, meaning you are free to use, reproduce and adapt our data providing you give appropriate credit, apply to all IntAct data, e.g. PSI-MI XML, MI-JSON and MITAB files"; "For attribution please go to our Publications page." Attribute "IntAct (EMBL-EBI), CC BY 4.0; del Toro et al. NAR 2022" |
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

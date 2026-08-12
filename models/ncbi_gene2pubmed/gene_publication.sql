-- description: Gene↔publication edge from NCBI gene2pubmed — one row per (gene, PMID), all taxa (cdsci-lake#38).
-- license: us-public-domain
-- column gene_id: NCBI Gene identifier (bioregistry prefix `ncbigene`). Bare local id, no embedded prefix.
-- column pmid: PubMed identifier (bioregistry prefix `pubmed`). Join key to `ref.id_crosswalk` for doi/pmcid/grant/nct.
-- column taxon_id: NCBI Taxonomy id (bioregistry prefix `ncbitaxon`), from each row's own tax_id.
-- ncbi_gene2pubmed.gene_publication: the tidy projection of the raw gene2pubmed
-- landing table. Deliberately the lightest model in the NCBI family — the raw
-- file is already the edge, so there is nothing to derive beyond dropping
-- incomplete rows and collapsing duplicates.
--
-- NOT part of ref.id_crosswalk, and that is a decision, not an oversight:
-- id_crosswalk's grain is one row per PMID (doi/pmcid/grant/nct are all
-- functionally dependent on the pmid). This is a many-to-many edge — one PMID
-- cites thousands of genes, one gene is cited by many PMIDs — so folding it in
-- would multiply that table's grain and break every consumer relying on it.
-- Join on `pmid` when you want both.
--
-- Not taxon-scoped either: raw is landed whole, and per-species scoping is a
-- WHERE clause a consumer writes (same rule as ncbi_gene.gene).
--
-- ponytail: SELECT DISTINCT, no gene/publication existence join. The raw dump
-- can reference a GeneID that gene_info has since retired; add a semi-join to
-- lake.ncbi_gene.gene if a consumer needs referential integrity — it would
-- couple this model to a source that is a separate ingest today.
SELECT DISTINCT
    gene_id,
    pmid,
    taxon_id
FROM lake.ncbi_gene2pubmed.gene2pubmed
WHERE gene_id IS NOT NULL AND pmid IS NOT NULL

MODEL (
  name ncbi_gene2pubmed.gene_publication,
  kind FULL,
  cron '@daily',
  tags ('license:us-public-domain'),
  description 'Gene↔publication edge from NCBI gene2pubmed — one row per (gene, PMID), all taxa (cdsci-lake#38).',
  column_descriptions (
    gene_id = 'NCBI Gene identifier (bioregistry prefix `ncbigene`). Bare local id, no embedded prefix.',
    pmid = 'PubMed identifier (bioregistry prefix `pubmed`). Join key to `icite.metadata` (doi), `pmc.documents` (pmcid), `reporter.publink` (grants), `ctgov.references` (trials).',
    taxon_id = 'NCBI Taxonomy id (bioregistry prefix `ncbitaxon`), from each row''s own tax_id.'
  ),
  audits (ncbi_gene2pubmed_gene_publication_gene_id_pmid_is_unique_the, ncbi_gene2pubmed_gene_publication_no_incomplete_edges)
);

-- ncbi_gene2pubmed.gene_publication: the tidy projection of the raw gene2pubmed
-- landing table. Deliberately the lightest model in the NCBI family — the raw
-- file is already the edge, so there is nothing to derive beyond dropping
-- incomplete rows and collapsing duplicates.
--
-- NOT folded into a per-PMID alias table, and that is a decision, not an
-- oversight: that grain is one row per PMID (doi/pmcid/grant/nct are all
-- functionally dependent on the pmid). This is a many-to-many edge — one PMID
-- cites thousands of genes, one gene is cited by many PMIDs — so folding it in
-- would multiply that grain. (The argument was originally made against
-- `ref.id_crosswalk`, retired unused 2026-08-15; it holds without it.)
-- Join on `pmid` to whichever source carries the identifier you want.
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

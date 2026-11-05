-- description: GWAS Catalog association->variant bridge: one row per variant named in an association's SNPS cell (cdsci-lake#169).
-- license: embl-ebi-terms-of-use
-- materialized: view
-- gwas_catalog.association_snp (ADR-0027 "nested fields: sibling tables"): the
-- lake parent keeps `snps` as the Catalog's raw cell; this view is the join key
-- people want from it. Columns:
--   line_number  the parent's key (lake.gwas_catalog.association)
--   snp_id       one variant as the Catalog wrote it: an rsID (`rs12120788`) or,
--                for ~17% of mentions, a position (`chr1:227959354`)
--
-- The split is the one bioc-on-ice's clinical.gwas_catalog__association uses for
-- its `snp_ids` list ('; ' haplotypes, ', ' lists, ' x ' interactions), so the
-- values match what bioc publishes; bioc derives its own association_key at
-- release time by joining this view to the parent on line_number (ADR-0015 b).
-- Distinct per row: 129 cells at 2026-10-22 repeat an rsID ('rs767235315,
-- rs767235315'), so (line_number, snp_id) is the grain. No ordinal: the cell
-- order is how the curator listed the variants, not a rank, and bioc sorts it.
-- A view, not a table: the parent is re-landed per Catalog release and this is
-- one regexp over 1.2M rows (#169 refinement).
SELECT a.line_number, s.snp_id
FROM lake.gwas_catalog.association AS a,
     UNNEST(list_distinct(regexp_split_to_array(a.snps, '\s*(;|,| x )\s*'))) AS s(snp_id)
WHERE a.snps IS NOT NULL

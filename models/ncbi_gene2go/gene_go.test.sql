-- test: one row per (taxon, gene, term, evidence, qualifier, category)
-- The business key. A duplicate here means the LEFT JOIN to ontology.terms
-- fanned out (more than one row per GO curie in one ontology), not that
-- gene2go itself has duplicates.
SELECT taxon_id, gene_id, go_id, evidence, qualifier, category
FROM lake.ncbi_gene2go.gene_go
GROUP BY ALL HAVING count(*) > 1

-- test: no null identifiers or key parts
SELECT * FROM lake.ncbi_gene2go.gene_go
WHERE gene_id IS NULL OR taxon_id IS NULL OR go_id IS NULL
   OR evidence IS NULL OR qualifier IS NULL OR category IS NULL

-- test: category is one of GO's three aspects
SELECT DISTINCT category FROM lake.ncbi_gene2go.gene_go
WHERE category NOT IN ('Process', 'Function', 'Component')

-- test: go_id is a GO curie
SELECT DISTINCT go_id FROM lake.ncbi_gene2go.gene_go WHERE go_id NOT LIKE 'GO:%'

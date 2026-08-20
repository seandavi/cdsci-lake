-- one row per (taxon, gene, term, evidence, qualifier, category)
AUDIT (
  name ncbi_gene2go_gene_go_one_row_per_taxon_gene_term,
);
-- The business key. A duplicate here means the LEFT JOIN to ontology.terms
-- fanned out (more than one row per GO curie in one ontology), not that
-- gene2go itself has duplicates.
SELECT taxon_id, gene_id, go_id, evidence, qualifier, category
FROM @this_model
GROUP BY ALL HAVING count(*) > 1;

-- no null identifiers or key parts
AUDIT (
  name ncbi_gene2go_gene_go_no_null_identifiers_or_key_parts,
);
SELECT * FROM @this_model
WHERE gene_id IS NULL OR taxon_id IS NULL OR go_id IS NULL
   OR evidence IS NULL OR qualifier IS NULL OR category IS NULL;

-- category is one of GO's three aspects
AUDIT (
  name ncbi_gene2go_gene_go_category_is_one_of_go_s,
);
SELECT DISTINCT category FROM @this_model
WHERE category NOT IN ('Process', 'Function', 'Component');

-- go_id is a GO curie
AUDIT (
  name ncbi_gene2go_gene_go_go_id_is_a_go_curie,
);
SELECT DISTINCT go_id FROM @this_model WHERE go_id NOT LIKE 'GO:%';

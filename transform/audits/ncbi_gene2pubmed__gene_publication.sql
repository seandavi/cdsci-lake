-- (gene_id, pmid) is unique — the SELECT DISTINCT holds the grain
AUDIT (
  name ncbi_gene2pubmed_gene_publication_gene_id_pmid_is_unique_the,
);
SELECT gene_id, pmid FROM @this_model
GROUP BY gene_id, pmid HAVING count(*) > 1;

-- no incomplete edges
AUDIT (
  name ncbi_gene2pubmed_gene_publication_no_incomplete_edges,
);
SELECT * FROM @this_model
WHERE gene_id IS NULL OR pmid IS NULL;

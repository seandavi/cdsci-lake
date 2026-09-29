-- test: (gene_id, pmid) is unique — the SELECT DISTINCT holds the grain
SELECT gene_id, pmid FROM lake.ncbi_gene2pubmed.gene_publication
GROUP BY gene_id, pmid HAVING count(*) > 1

-- test: no incomplete edges
SELECT * FROM lake.ncbi_gene2pubmed.gene_publication
WHERE gene_id IS NULL OR pmid IS NULL

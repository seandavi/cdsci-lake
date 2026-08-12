-- test: gene_id is unique
SELECT gene_id FROM lake.ncbi_gene.gene GROUP BY gene_id HAVING count(*) > 1

-- test: no NEWENTRY placeholder rows survive
SELECT gene_id FROM lake.ncbi_gene.gene WHERE symbol = 'NEWENTRY'

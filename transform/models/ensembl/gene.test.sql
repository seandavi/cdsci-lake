-- test: (gene_id, ncbitaxon_id, ensembl_release) is unique
SELECT gene_id, ncbitaxon_id, ensembl_release
FROM lake.ensembl.gene
GROUP BY gene_id, ncbitaxon_id, ensembl_release HAVING count(*) > 1

-- test: no null key part, and coordinates are sane
SELECT * FROM lake.ensembl.gene
WHERE gene_id IS NULL OR ncbitaxon_id IS NULL OR ensembl_release IS NULL
   OR "start" > "end" OR strand NOT IN ('+', '-')

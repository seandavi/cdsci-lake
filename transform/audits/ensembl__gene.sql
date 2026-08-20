-- (gene_id, ncbitaxon_id, ensembl_release) is unique
AUDIT (
  name ensembl_gene_gene_id_ncbitaxon_id_ensembl_release,
);
SELECT gene_id, ncbitaxon_id, ensembl_release
FROM @this_model
GROUP BY gene_id, ncbitaxon_id, ensembl_release HAVING count(*) > 1;

-- no null key part, and coordinates are sane
AUDIT (
  name ensembl_gene_no_null_key_part_and_coordinates,
);
SELECT * FROM @this_model
WHERE gene_id IS NULL OR ncbitaxon_id IS NULL OR ensembl_release IS NULL
   OR "start" > "end" OR strand NOT IN ('+', '-');

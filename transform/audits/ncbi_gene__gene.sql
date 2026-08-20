-- gene_id is unique
AUDIT (
  name ncbi_gene_gene_gene_id_is_unique,
);
SELECT gene_id FROM @this_model GROUP BY gene_id HAVING count(*) > 1;

-- no NEWENTRY placeholder rows survive
AUDIT (
  name ncbi_gene_gene_no_newentry_placeholder_rows_survive,
);
SELECT gene_id FROM @this_model WHERE symbol = 'NEWENTRY';

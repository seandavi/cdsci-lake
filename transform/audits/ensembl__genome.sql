-- one genome per (taxon, release)
AUDIT (
  name ensembl_genome_one_genome_per_taxon_release,
);
SELECT ncbitaxon_id, ensembl_release
FROM @this_model
GROUP BY ncbitaxon_id, ensembl_release HAVING count(*) > 1;

-- no null identifying field
AUDIT (
  name ensembl_genome_no_null_identifying_field,
);
SELECT * FROM @this_model
WHERE genome_id IS NULL OR ncbitaxon_id IS NULL OR assembly_name IS NULL;

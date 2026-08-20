-- no empty or null identifiers on either side
AUDIT (
  name ncbi_gene_mapping_no_empty_or_null_identifiers_on,
);
SELECT * FROM @this_model
WHERE source_id IS NULL OR target_id IS NULL
   OR trim(source_id) = '' OR trim(target_id) = '';

-- MIM is always remapped to OMIM
AUDIT (
  name ncbi_gene_mapping_mim_is_always_remapped_to_omim,
);
SELECT * FROM @this_model WHERE target_namespace = 'MIM';

-- the whole tuple is distinct
AUDIT (
  name ncbi_gene_mapping_the_whole_tuple_is_distinct,
);
SELECT source_namespace, source_id, target_namespace, target_id, taxon_id
FROM @this_model
GROUP BY ALL HAVING count(*) > 1;

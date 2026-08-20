-- (source_id, target_id) is unique
AUDIT (
  name uniprot_identifier_mapping_source_id_target_id_is_unique,
);
SELECT source_id, target_id FROM @this_model
GROUP BY source_id, target_id HAVING count(*) > 1;

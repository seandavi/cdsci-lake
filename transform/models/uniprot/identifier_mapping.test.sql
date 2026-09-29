-- test: (source_id, target_id) is unique
SELECT source_id, target_id FROM lake.uniprot.identifier_mapping
GROUP BY source_id, target_id HAVING count(*) > 1

-- test: no empty or null identifiers on either side
SELECT * FROM lake.ncbi_gene.mapping
WHERE source_id IS NULL OR target_id IS NULL
   OR trim(source_id) = '' OR trim(target_id) = ''

-- test: MIM is always remapped to OMIM
SELECT * FROM lake.ncbi_gene.mapping WHERE target_namespace = 'MIM'

-- test: the whole tuple is distinct
SELECT source_namespace, source_id, target_namespace, target_id, taxon_id
FROM lake.ncbi_gene.mapping
GROUP BY ALL HAVING count(*) > 1

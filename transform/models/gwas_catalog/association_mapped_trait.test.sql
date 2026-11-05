-- test: (line_number, trait_id) is unique
SELECT line_number, trait_id FROM lake.gwas_catalog.association_mapped_trait
GROUP BY ALL HAVING count(*) > 1

-- test: no null or empty trait_id
SELECT * FROM lake.gwas_catalog.association_mapped_trait
WHERE line_number IS NULL OR trait_id IS NULL OR trait_id = ''

-- test: a trait_id is a CURIE, or an IRI that had no PREFIX_LOCAL tail
SELECT DISTINCT trait_id FROM lake.gwas_catalog.association_mapped_trait
WHERE trait_id NOT SIMILAR TO '[A-Za-z0-9]+:[A-Za-z0-9]+' AND trait_id NOT LIKE 'http%'

-- test: every row joins back to its parent
SELECT b.line_number FROM lake.gwas_catalog.association_mapped_trait AS b
LEFT JOIN lake.gwas_catalog.association AS a USING (line_number)
WHERE a.line_number IS NULL

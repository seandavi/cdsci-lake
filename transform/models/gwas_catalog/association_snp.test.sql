-- test: (line_number, snp_id) is unique
SELECT line_number, snp_id FROM lake.gwas_catalog.association_snp
GROUP BY ALL HAVING count(*) > 1

-- test: no null or empty snp_id
SELECT * FROM lake.gwas_catalog.association_snp
WHERE line_number IS NULL OR snp_id IS NULL OR snp_id = ''

-- test: every row joins back to its parent
SELECT b.line_number FROM lake.gwas_catalog.association_snp AS b
LEFT JOIN lake.gwas_catalog.association AS a USING (line_number)
WHERE a.line_number IS NULL

-- test: one genome per (taxon, release)
SELECT ncbitaxon_id, ensembl_release
FROM lake.ensembl.genome
GROUP BY ncbitaxon_id, ensembl_release HAVING count(*) > 1

-- test: no null identifying field
SELECT * FROM lake.ensembl.genome
WHERE genome_id IS NULL OR ncbitaxon_id IS NULL OR assembly_name IS NULL

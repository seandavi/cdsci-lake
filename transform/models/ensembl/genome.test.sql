-- test: one row per (species, release)
SELECT species, ensembl_release
FROM lake.ensembl.genome
GROUP BY species, ensembl_release HAVING count(*) > 1

-- test: no null identifying field
SELECT * FROM lake.ensembl.genome
WHERE species IS NULL OR ncbitaxon_id IS NULL OR assembly_name IS NULL

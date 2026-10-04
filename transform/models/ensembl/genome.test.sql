-- test: one row per (assembly, release) -- not per taxon: a taxon can carry
-- several assemblies in one release (pig breeds, mouse strains, haplotypes)
SELECT genome_id, ensembl_release
FROM lake.ensembl.genome
GROUP BY genome_id, ensembl_release HAVING count(*) > 1

-- test: no null identifying field
SELECT * FROM lake.ensembl.genome
WHERE genome_id IS NULL OR ncbitaxon_id IS NULL OR assembly_name IS NULL

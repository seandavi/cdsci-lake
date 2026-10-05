-- test: (transcript_id, species, ensembl_release) is unique
SELECT transcript_id, species, ensembl_release
FROM lake.ensembl.transcript
GROUP BY transcript_id, species, ensembl_release HAVING count(*) > 1

-- test: exactly one Ensembl Canonical transcript per gene
SELECT gene_id, species, ensembl_release, count(*) AS n_canonical
FROM lake.ensembl.transcript
WHERE canonical
GROUP BY gene_id, species, ensembl_release HAVING count(*) <> 1

-- test: every transcript's gene_id also appears as a gene line for the same assembly/release
-- (asserted against ensembl.feature, not ensembl.gene: gene and transcript are
-- siblings in the DAG, so a test naming ensembl.gene would depend on run order)
SELECT DISTINCT t.gene_id, t.species, t.ensembl_release
FROM lake.ensembl.transcript t
LEFT JOIN (
    SELECT DISTINCT gene_id, species, ensembl_release
    FROM lake.ensembl.feature WHERE feature = 'gene'
) g
  ON g.gene_id = t.gene_id
 AND g.species = t.species
 AND g.ensembl_release = t.ensembl_release
WHERE g.gene_id IS NULL

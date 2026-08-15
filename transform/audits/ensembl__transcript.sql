-- (transcript_id, ncbitaxon_id, ensembl_release) is unique
AUDIT (
  name ensembl_transcript_transcript_id_ncbitaxon_id_ensembl_release,
);
SELECT transcript_id, ncbitaxon_id, ensembl_release
FROM @this_model
GROUP BY transcript_id, ncbitaxon_id, ensembl_release HAVING count(*) > 1;

-- exactly one Ensembl Canonical transcript per gene
AUDIT (
  name ensembl_transcript_exactly_one_ensembl_canonical_transcript_per,
);
SELECT gene_id, ncbitaxon_id, ensembl_release, count(*) AS n_canonical
FROM @this_model
WHERE canonical
GROUP BY gene_id, ncbitaxon_id, ensembl_release HAVING count(*) <> 1;

-- every transcript's gene_id also appears as a gene line for the same taxon/release
AUDIT (
  name ensembl_transcript_every_transcript_s_gene_id_also,
);
-- (asserted against ensembl.feature, not ensembl.gene: gene and transcript are
-- siblings in the DAG, so a test naming ensembl.gene would depend on run order)
SELECT DISTINCT t.gene_id, t.ncbitaxon_id, t.ensembl_release
FROM @this_model t
LEFT JOIN (
    SELECT DISTINCT gene_id, ncbitaxon_id, ensembl_release
    FROM lake.ensembl.feature WHERE feature = 'gene'
) g
  ON g.gene_id = t.gene_id
 AND g.ncbitaxon_id = t.ncbitaxon_id
 AND g.ensembl_release = t.ensembl_release
WHERE g.gene_id IS NULL;

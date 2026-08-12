-- test: every gene/transcript/exon line yields the id that names it
SELECT feature, count(*) AS n
FROM lake.ensembl.feature
WHERE (feature = 'gene' AND gene_id IS NULL)
   OR (feature = 'transcript' AND transcript_id IS NULL)
   OR (feature = 'exon' AND (exon_id IS NULL OR transcript_id IS NULL))
GROUP BY feature

-- test: exon_number is numeric wherever it is present
SELECT DISTINCT exon_number
FROM lake.ensembl.feature
WHERE exon_number IS NOT NULL AND TRY_CAST(exon_number AS INTEGER) IS NULL

-- test: the parse never lets a quote leak into an extracted id
SELECT DISTINCT gene_id, transcript_id, exon_id
FROM lake.ensembl.feature
WHERE gene_id LIKE '%"%' OR transcript_id LIKE '%"%' OR exon_id LIKE '%"%'

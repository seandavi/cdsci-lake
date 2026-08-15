-- every gene/transcript/exon line yields the id that names it
AUDIT (
  name ensembl_feature_every_gene_transcript_exon_line_yields,
);
SELECT feature, count(*) AS n
FROM @this_model
WHERE (feature = 'gene' AND gene_id IS NULL)
   OR (feature = 'transcript' AND transcript_id IS NULL)
   OR (feature = 'exon' AND (exon_id IS NULL OR transcript_id IS NULL))
GROUP BY feature;

-- exon_number is numeric wherever it is present
AUDIT (
  name ensembl_feature_exon_number_is_numeric_wherever_it,
);
SELECT DISTINCT exon_number
FROM @this_model
WHERE exon_number IS NOT NULL AND TRY_CAST(exon_number AS INTEGER) IS NULL;

-- the parse never lets a quote leak into an extracted id
AUDIT (
  name ensembl_feature_the_parse_never_lets_a_quote,
);
SELECT DISTINCT gene_id, transcript_id, exon_id
FROM @this_model
WHERE gene_id LIKE '%"%' OR transcript_id LIKE '%"%' OR exon_id LIKE '%"%';

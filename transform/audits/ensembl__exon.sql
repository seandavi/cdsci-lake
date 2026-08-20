-- (exon_id, transcript_id, ncbitaxon_id, ensembl_release) is unique — the CDS join never fans out
AUDIT (
  name ensembl_exon_exon_id_transcript_id_ncbitaxon_id,
);
SELECT exon_id, transcript_id, ncbitaxon_id, ensembl_release
FROM @this_model
GROUP BY exon_id, transcript_id, ncbitaxon_id, ensembl_release HAVING count(*) > 1;

-- the joined CDS lies inside its exon
AUDIT (
  name ensembl_exon_the_joined_cds_lies_inside_its,
);
SELECT exon_id, transcript_id, "start", "end", cds_start, cds_end
FROM @this_model
WHERE cds_start IS NOT NULL
  AND (cds_start < "start" OR cds_end > "end" OR cds_start > cds_end);

-- cds_phase is a real GTF frame wherever a CDS was joined
AUDIT (
  name ensembl_exon_cds_phase_is_a_real_gtf,
);
SELECT DISTINCT cds_phase FROM @this_model
WHERE cds_start IS NOT NULL AND (cds_phase IS NULL OR cds_phase NOT IN (0, 1, 2));

-- ranks within a transcript are a gapless 1..n run
AUDIT (
  name ensembl_exon_ranks_within_a_transcript_are_a,
);
SELECT transcript_id, ncbitaxon_id, ensembl_release
FROM @this_model
GROUP BY transcript_id, ncbitaxon_id, ensembl_release
HAVING min(rank) <> 1 OR max(rank) <> count(*);

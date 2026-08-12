-- test: (exon_id, transcript_id, ncbitaxon_id, ensembl_release) is unique — the CDS join never fans out
SELECT exon_id, transcript_id, ncbitaxon_id, ensembl_release
FROM lake.ensembl.exon
GROUP BY exon_id, transcript_id, ncbitaxon_id, ensembl_release HAVING count(*) > 1

-- test: the joined CDS lies inside its exon
SELECT exon_id, transcript_id, "start", "end", cds_start, cds_end
FROM lake.ensembl.exon
WHERE cds_start IS NOT NULL
  AND (cds_start < "start" OR cds_end > "end" OR cds_start > cds_end)

-- test: cds_phase is a real GTF frame wherever a CDS was joined
SELECT DISTINCT cds_phase FROM lake.ensembl.exon
WHERE cds_start IS NOT NULL AND (cds_phase IS NULL OR cds_phase NOT IN (0, 1, 2))

-- test: ranks within a transcript are a gapless 1..n run
SELECT transcript_id, ncbitaxon_id, ensembl_release
FROM lake.ensembl.exon
GROUP BY transcript_id, ncbitaxon_id, ensembl_release
HAVING min(rank) <> 1 OR max(rank) <> count(*)

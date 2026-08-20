MODEL (
  name ensembl.exon,
  kind FULL,
  cron '@daily',
  tags ('license:ensembl-no-restrictions'),
  description 'One row per (exon, transcript) per release, with the coding bounds and phase of the CDS lying in it.',
  column_descriptions (
    exon_id = 'Ensembl stable exon id (ENSE…). Not unique on its own — one exon is shared by every transcript that contains it, so the key is (exon_id, transcript_id, ncbitaxon_id, ensembl_release).',
    rank = 'The exon''s 1-based position within its transcript (GTF `exon_number`), counted 5''→3'' on the transcript''s own strand.',
    cds_start = 'Start of the coding sequence within this exon, or NULL for a wholly non-coding exon. From the CDS line joined on (transcript_id, exon_number).',
    cds_phase = 'GTF `frame` of the joined CDS line — bases to skip before the first whole codon (0/1/2). NULL for a non-coding exon.'
  ),
  audits (ensembl_exon_exon_id_transcript_id_ncbitaxon_id, ensembl_exon_the_joined_cds_lies_inside_its, ensembl_exon_cds_phase_is_a_real_gtf, ensembl_exon_ranks_within_a_transcript_are_a)
);

-- ensembl.exon (ADR-0015): the one genuinely non-trivial model in this source.
-- A GTF writes the coding sub-interval of an exon as a *separate* `CDS` line
-- carrying the same transcript_id and exon_number as the exon it lies in, so a
-- LEFT self-join on (transcript_id, exon_number) folds coding bounds and phase
-- onto the exon row rather than leaving CDS as a fourth feature table nobody
-- joins correctly. The join is LEFT because a UTR-only or non-coding exon has
-- no CDS line at all.
--
-- The self-join does not fan out: (transcript_id, exon_number) selects at most
-- one CDS line, so the row count equals the count of `exon` lines. Verified
-- against bioc-on-ice's release-116 output (0 duplicate (exon_id,
-- transcript_id, taxon_id) triples across 5.09M human + 3.76M mouse exon rows)
-- and asserted by this model's first test.
SELECT
    e.exon_id,
    e.transcript_id,
    e.ncbitaxon_id,
    e.ensembl_release,
    e.seqname AS sequence_name,
    e."start",
    e."end",
    e.strand,
    TRY_CAST(e.exon_number AS INTEGER) AS rank,
    c."start" AS cds_start,
    c."end" AS cds_end,
    TRY_CAST(c.frame AS INTEGER) AS cds_phase,
    'ENSEMBL' AS source
FROM lake.ensembl.feature e
LEFT JOIN lake.ensembl.feature c
  ON c.feature = 'CDS'
 AND c.transcript_id = e.transcript_id
 AND c.exon_number = e.exon_number
 AND c.ncbitaxon_id = e.ncbitaxon_id
 AND c.ensembl_release = e.ensembl_release
WHERE e.feature = 'exon'

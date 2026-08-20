MODEL (
  name ensembl.transcript,
  kind FULL,
  cron '@daily',
  tags ('license:ensembl-no-restrictions'),
  description 'One row per Ensembl transcript per release — the GTF''s `transcript` feature lines.',
  column_descriptions (
    transcript_id = 'Ensembl stable transcript id (ENST…, or the species'' native id — yeast uses e.g. YDL246C_mRNA). Unversioned; version in its own column.',
    canonical = 'The Ensembl Canonical transcript for its gene (GTF `tag "Ensembl_canonical"`). Exactly one per gene, asserted by this model''s test.'
  ),
  audits (ensembl_transcript_transcript_id_ncbitaxon_id_ensembl_release, ensembl_transcript_exactly_one_ensembl_canonical_transcript_per, ensembl_transcript_every_transcript_s_gene_id_also)
);

-- ensembl.transcript (ADR-0015): projection of the `transcript` lines from
-- ensembl.feature, keyed (transcript_id, ncbitaxon_id, ensembl_release), with
-- gene_id carried through as the FK to ensembl.gene. `canonical` is a tag
-- membership test rather than an attribute pull -- `tag` is the one repeatable
-- GTF key, so a value regex on it would be ambiguous.
SELECT
    transcript_id,
    gene_id,
    ncbitaxon_id,
    ensembl_release,
    transcript_version AS version,
    transcript_biotype AS biotype,
    canonical,
    seqname AS sequence_name,
    "start",
    "end",
    strand,
    'ENSEMBL' AS source
FROM lake.ensembl.feature
WHERE feature = 'transcript'

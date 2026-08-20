MODEL (
  name ensembl.feature,
  kind FULL,
  cron '@daily',
  tags ('license:ensembl-no-restrictions'),
  description 'Ensembl GTF lines with the attribute blob parsed into columns — the shared parse behind ensembl.gene/transcript/exon.',
  column_descriptions (
    ncbitaxon_id = 'NCBI Taxonomy id (bioregistry canonical prefix `ncbitaxon`, not `ncbi_taxon`/`taxon`), stamped at land time from Ensembl''s own species_EnsemblVertebrates.txt for the release.',
    ensembl_release = 'The Ensembl release this row was landed from. Part of every derived table''s key — releases are immutable and stack rather than overwrite.',
    canonical = 'True when the line carries `tag "Ensembl_canonical"`. Verified against bioc-on-ice''s release-116 output: exactly one canonical transcript per gene (78,941 canonical transcripts vs 78,941 genes for taxon 9606).'
  ),
  audits (ensembl_feature_every_gene_transcript_exon_line_yields, ensembl_feature_exon_number_is_numeric_wherever_it, ensembl_feature_the_parse_never_lets_a_quote)
);

-- ensembl.feature (ADR-0015): one row per GTF line, the ~11 attribute pulls done
-- once here instead of three times in gene/transcript/exon. Ported from
-- bioc-on-ice's `ensembl.py` `feat` temp table, which materialized the same
-- extraction per transform run; here it is a real model so the DAG runs it once
-- and the downstream three read columns, not regexes.
--
-- Every pull is `nullif(regexp_extract(...), '')` because regexp_extract returns
-- '' (not NULL) on no match, and "attribute absent" is common and meaningful:
-- yeast (R64-1-1) carries no gene_version/transcript_version/exon_version at
-- all, human carries all three. An empty string would make those look present.
SELECT
    feature,
    seqname,
    "start",
    "end",
    strand,
    frame,
    ncbitaxon_id,
    ensembl_release,
    species,
    assembly,
    genome_accession,
    nullif(regexp_extract(attribute, 'gene_id "([^"]*)"', 1), '')            AS gene_id,
    nullif(regexp_extract(attribute, 'gene_version "([^"]*)"', 1), '')       AS gene_version,
    nullif(regexp_extract(attribute, 'gene_name "([^"]*)"', 1), '')          AS gene_name,
    nullif(regexp_extract(attribute, 'gene_biotype "([^"]*)"', 1), '')       AS gene_biotype,
    nullif(regexp_extract(attribute, 'gene_source "([^"]*)"', 1), '')        AS gene_source,
    nullif(regexp_extract(attribute, 'transcript_id "([^"]*)"', 1), '')      AS transcript_id,
    nullif(regexp_extract(attribute, 'transcript_version "([^"]*)"', 1), '') AS transcript_version,
    nullif(regexp_extract(attribute, 'transcript_biotype "([^"]*)"', 1), '') AS transcript_biotype,
    nullif(regexp_extract(attribute, 'exon_id "([^"]*)"', 1), '')            AS exon_id,
    nullif(regexp_extract(attribute, 'exon_number "([^"]*)"', 1), '')        AS exon_number,
    attribute LIKE '%tag "Ensembl_canonical"%'                               AS canonical
FROM lake.ensembl.gtf

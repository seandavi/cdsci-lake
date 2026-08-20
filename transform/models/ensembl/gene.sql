MODEL (
  name ensembl.gene,
  kind FULL,
  cron '@daily',
  tags ('license:ensembl-no-restrictions'),
  description 'One row per Ensembl gene per release — the GTF''s `gene` feature lines.',
  column_descriptions (
    gene_id = 'Ensembl stable gene id (ENSG…, or the species'' native id for non-Ensembl genebuilds — yeast carries SGD ids such as YDL246C). External and citable; unversioned, with the version in its own column.',
    symbol = 'The gene''s official symbol (GTF `gene_name`). NULL where Ensembl has no symbol for the gene — common outside the well-annotated genomes.',
    gene_type = 'GTF `gene_biotype` (protein_coding, lncRNA, …).',
    curation_source = 'GTF `gene_source` — which genebuild annotated the gene (ensembl, havana, ensembl_havana, sgd, …), not this lake''s `source`.'
  ),
  audits (ensembl_gene_gene_id_ncbitaxon_id_ensembl_release, ensembl_gene_no_null_key_part_and_coordinates)
);

-- ensembl.gene (ADR-0015): a straight projection of the `gene` lines from
-- ensembl.feature, keyed (gene_id, ncbitaxon_id, ensembl_release). Column names
-- follow bioc-on-ice's annotation.gene shape (symbol/gene_type/curation_source)
-- because those are the better names, but `ncbitaxon_id` deliberately does *not*
-- follow its `taxon_id` — this lake's id-naming convention (scripts/
-- lint_id_columns.py) wants the bioregistry canonical prefix.
SELECT
    gene_id,
    ncbitaxon_id,
    ensembl_release,
    gene_version AS version,
    gene_name AS symbol,
    gene_biotype AS gene_type,
    gene_source AS curation_source,
    seqname AS sequence_name,
    "start",
    "end",
    strand,
    'ENSEMBL' AS source
FROM lake.ensembl.feature
WHERE feature = 'gene'

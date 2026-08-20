MODEL (
  name ncbi_gene2accession.mapping,
  kind FULL,
  cron '@daily',
  tags ('license:us-public-domain'),
  description 'Entrez↔accession cross-references from NCBI gene2accession — RefSeq RNA/protein + GenBank genomic, all taxa (cdsci-lake#37).',
  column_descriptions (
    source_namespace = 'Vocabulary of `source_id`. Always ENTREZ here — gene2accession has exactly one gene identifier per row.',
    source_id = 'NCBI Gene identifier (bioregistry prefix `ncbigene`) as text, to match `ncbi_gene.mapping`''s column type.',
    target_namespace = 'Vocabulary of `target_id` — REFSEQ_RNA, REFSEQ_PROTEIN, or GENBANK_GENOMIC.',
    target_id = 'The accession **with its version suffix** exactly as NCBI ships it (NM_000546.6). Strip with split_part(target_id, ''.'', 1) if an unversioned form is wanted.',
    taxon_id = 'NCBI Taxonomy id (bioregistry prefix `ncbitaxon`), from each row''s own tax_id.',
    source = 'Writer scope, always NCBI_ACCESSION — deliberately not ''NCBI'', which is `ncbi_gene.mapping`''s scope (bioc-on-ice ADR-0004''s one-writer-one-scope rule).'
  ),
  audits (ncbi_gene2accession_mapping_no_empty_or_null_identifiers_on, ncbi_gene2accession_mapping_the_whole_tuple_is_distinct_select, ncbi_gene2accession_mapping_the_discriminator_actually_separated_refseq_from, ncbi_gene2accession_mapping_every_refseq_target_carries_a_known, ncbi_gene2accession_mapping_only_entrez_on_the_source_side)
);

-- ncbi_gene2accession.mapping: the lake-side cross-reference table, ported from
-- bioc-on-ice's ncbi_accession.transform() — with its discriminator corrected.
--
-- bioc-on-ice used a bare `contains(accession, '_')` to mean "this is RefSeq".
-- A full scan of the real 284.8M-row dump (2026-08-11) says that is **wrong for
-- the protein column**: 4,818 rows across 931 distinct PDB entries carry a PDB
-- *chain* accession there — `3SID_A.1`, `6Y3D_aA.1`, `1FX0_A.1` — which contains
-- an underscore and is not RefSeq at all. Under the ported rule those became
-- REFSEQ_PROTEIN. The rule here is therefore the RefSeq accession *shape*,
-- exactly two uppercase letters then an underscore, which is what actually
-- distinguishes NP_/XP_/WP_/YP_ from a 4-character PDB id.
--
-- What the scan does confirm, and what this model relies on:
--   * RNA: 85,693,555 underscored values, every one NM_/NR_/XM_/XR_ — no
--     exceptions in the whole file.
--   * Genomic: 107,607,287 underscored values, every one NC_/NW_/NZ_/NG_/NT_/AC_
--     — so `NOT contains(genomic_accession, '_')` really does mean INSDC
--     (166,900,675 rows), and it is kept as-is: it is the conservative
--     direction, since anything underscored and unexpected is excluded rather
--     than mislabelled GenBank.
--   * Protein: only the 4,818 PDB rows above break the underscore rule.
-- `mapping.test.sql` asserts the corrected rule holds, so a future NCBI
-- accession shape fails loudly instead of quietly polluting a namespace.
--
-- What the three branches deliberately do NOT emit, carried over from
-- bioc-on-ice unchanged and worth stating because it is asymmetric:
--   * GenBank RNA/protein accessions (the non-underscored ones) — most of the
--     protein column for bacteria;
--   * RefSeq genomic accessions (NC_/NZ_/NW_) — which for prokaryotes is where
--     nearly all the genomic assertions live.
-- A REFSEQ_GENOMIC / GENBANK_PROTEIN namespace is a two-line addition to the
-- UNION when a consumer needs one; adding it unasked would change what
-- downstream readers of this namespace set already expect.
--
-- ponytail: SELECT DISTINCT over ~280M union rows (285M raw) is the expensive step
-- (it spills — hence Settings.duckdb_temp_directory must point at /data).
-- Cheaper only if the raw landing were already deduplicated, which it is not
-- and should not be: raw is verbatim.
--
-- Not taxon-scoped: each branch carries its own row's tax_id, so one table
-- covers every organism rather than one call per species (same rule as
-- ncbi_gene.mapping). Not shaped for bioc-on-ice's annotation.identifier_mapping
-- either (no confidence/valid_from/valid_to) — reverse-ETL is out of scope,
-- cdsci-lake#63.
SELECT DISTINCT source_namespace, source_id, target_namespace, target_id, taxon_id,
       'NCBI_ACCESSION' AS source
FROM (
    SELECT 'ENTREZ' AS source_namespace, CAST(gene_id AS VARCHAR) AS source_id,
           'REFSEQ_RNA' AS target_namespace, rna_accession AS target_id, taxon_id
    FROM lake.ncbi_gene2accession.gene2accession
    WHERE regexp_matches(rna_accession, '^[A-Z]{2}_')
  UNION ALL
    -- the branch the PDB chain accessions broke; see the header
    SELECT 'ENTREZ', CAST(gene_id AS VARCHAR), 'REFSEQ_PROTEIN', protein_accession, taxon_id
    FROM lake.ncbi_gene2accession.gene2accession
    WHERE regexp_matches(protein_accession, '^[A-Z]{2}_')
  UNION ALL
    SELECT 'ENTREZ', CAST(gene_id AS VARCHAR), 'GENBANK_GENOMIC', genomic_accession, taxon_id
    FROM lake.ncbi_gene2accession.gene2accession
    WHERE NOT contains(genomic_accession, '_')
)
WHERE source_id IS NOT NULL AND target_id IS NOT NULL

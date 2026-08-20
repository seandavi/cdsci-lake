MODEL (
  name uniprot.identifier_mapping,
  kind FULL,
  cron '@daily',
  tags ('license:cc-by-4.0'),
  description 'UniProt accession<->Entrez gene ID mapping, shaped for bioc-on-ice''s annotation.identifier_mapping (cdsci-lake#32).',
  audits (uniprot_identifier_mapping_source_id_target_id_is_unique)
);

-- uniprot.identifier_mapping: reshapes lake.uniprot.idmapping's (accession,
-- gene_id) pairs into the (source_namespace, source_id, target_namespace,
-- target_id, taxon_id, source, confidence, valid_from, valid_to) tuple
-- bioc-on-ice's annotation.identifier_mapping already uses for every other
-- cross-reference direction (verified live: ENTREZ -> SYMBOL/ALIAS/HGNC/...).
-- `ENTREZ`, not `ENTREZID` -- the OrgDb keytype name (SPEC.md's own parity
-- list) and the identifier_mapping namespace value differ; checked the live
-- table before writing this, not assumed from the keytype list.
-- `taxon_id` is UniProt's own per-row NCBI-taxon column, not inferred from
-- which organism file the row was downloaded from -- more precise if a
-- future multi-organism load ever mixes files.
SELECT
    'ENTREZ' AS source_namespace,
    CAST(gene_id AS VARCHAR) AS source_id,
    'UNIPROT' AS target_namespace,
    accession AS target_id,
    TRY_CAST(ncbi_taxon AS INTEGER) AS taxon_id,
    'UniProt' AS source,
    CAST(NULL AS DOUBLE) AS confidence,
    snapshot_version AS valid_from,
    CAST(NULL AS VARCHAR) AS valid_to
FROM lake.uniprot.idmapping
WHERE TRY_CAST(ncbi_taxon AS INTEGER) IS NOT NULL

-- description: NCBI Gene cross-references — Entrez ↔ Ensembl/symbol/alias/dbXref authority, all taxa (cdsci-lake#36).
-- license: us-public-domain
-- column source_namespace: Vocabulary of `source_id` (ENTREZ, ENSEMBL). Uppercase, matching the namespace names bioc-on-ice's annotation.identifier_mapping already uses.
-- column target_namespace: Vocabulary of `target_id` — SYMBOL, ALIAS, ENTREZ, or a dbXrefs authority (HGNC, OMIM, Ensembl, AllianceGenome, …).
-- column taxon_id: NCBI Taxonomy id (bioregistry prefix `ncbitaxon`), from each row's own tax_id.
-- ncbi_gene.mapping: the lake-side cross-reference table, ported from
-- bioc-on-ice's ncbi.transform() `mapping` derivation.
--
-- Deliberately NOT shaped for bioc-on-ice's annotation.identifier_mapping
-- (no valid_from/valid_to, no confidence): reverse-ETL is out of scope here
-- (cdsci-lake#63 -- build the table in the lake first, publish later, if at
-- all). Reshaping to a publish contract is a second, separate model.
--
-- Also not taxon-scoped: the four UNION ALL branches each carry their own row's
-- tax_id, so one table covers every organism NCBI knows rather than one call
-- per species.
WITH info AS (
    -- NEWENTRY is NCBI's GeneRIF placeholder, one row per taxon and not a gene.
    SELECT * FROM lake.ncbi_gene.gene_info WHERE symbol IS DISTINCT FROM 'NEWENTRY'
)
SELECT DISTINCT source_namespace, source_id, target_namespace, target_id, taxon_id,
       'NCBI' AS source
FROM (
    SELECT 'ENSEMBL' AS source_namespace, ensembl_gene_id AS source_id,
           'ENTREZ' AS target_namespace, CAST(gene_id AS VARCHAR) AS target_id,
           taxon_id
    FROM lake.ncbi_gene.gene2ensembl
    WHERE ensembl_gene_id IS NOT NULL AND gene_id IS NOT NULL
  UNION ALL
    SELECT 'ENTREZ', CAST(gene_id AS VARCHAR), 'SYMBOL', symbol, taxon_id
    FROM info WHERE symbol IS NOT NULL
  UNION ALL
    SELECT 'ENTREZ', CAST(gene_id AS VARCHAR), 'ALIAS', trim(alias), taxon_id
    FROM (SELECT gene_id, taxon_id, unnest(str_split(synonyms, '|')) AS alias
          FROM info WHERE synonyms IS NOT NULL)
    WHERE trim(alias) <> ''
  UNION ALL
    -- dbXrefs are 'Authority:id' pairs. Split at the FIRST colon only: HGNC's and
    -- AllianceGenome's own ids embed one ('HGNC:HGNC:11998'), and that prefixed
    -- form is the canonical HGNC identifier. MIM is renamed OMIM -- MIM is NCBI's
    -- abbreviation for that authority, and one authority under two namespace
    -- names would be the real bug.
    SELECT 'ENTREZ', CAST(gene_id AS VARCHAR),
           CASE WHEN ns = 'MIM' THEN 'OMIM' ELSE ns END, id, taxon_id
    FROM (SELECT gene_id, taxon_id, upper(split_part(x, ':', 1)) AS ns,
                 substr(x, strpos(x, ':') + 1) AS id
          FROM (SELECT gene_id, taxon_id, unnest(str_split(dbxrefs, '|')) AS x
                FROM info WHERE dbxrefs IS NOT NULL)
          -- no colon means no authority; without this the whole string would
          -- become both the namespace and the identifier
          WHERE x LIKE '%:%')
    WHERE ns <> '' AND id <> ''
)

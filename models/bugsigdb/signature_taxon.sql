-- description: BugSigDB signature<->NCBI-taxon bridge table, one row per taxon member of a signature.
-- license: cc-by-4.0
-- column ncbitaxon_id: NCBI Taxonomy id (bioregistry prefix `ncbitaxon`, not `ncbi_taxon` -- verified against bioregistry.io / lake.ref.bioregistry 2026-08-10). Bare local id, no embedded prefix -- this column is single-vocabulary.
-- bugsigdb.signature_taxon (cdsci-lake#31, ADR-0015 pilot model #2): explodes
-- lake.bugsigdb.signatures' two nested member-list columns into one row per
-- signature member. `metaphlan_taxon_names` (comma-separated) and
-- `ncbi_taxonomy_ids` (semicolon-separated) are positionally matched --
-- verified against the full v1.3.1 export: 0 rows where the two lists differ
-- in length. Each element is itself a `|`-separated lineage
-- (`k__..|p__..|..|s__Species name`, taxids in the same rank order); only the
-- leaf (most specific asserted rank) is split out here, the full lineage is
-- kept alongside for provenance.
--
-- ponytail: no rank rollup (kingdom/phylum/.../species as separate columns) --
-- that needs NCBI Taxonomy for a real taxonomic join (bioc-on-ice's bugsigdb.py
-- flagged the same gap, tracked as bioc-on-ice#18). Add it if a consumer needs
-- to query at a fixed rank rather than take whatever rank each member asserts.
WITH lists AS (
    SELECT
        bsdb_id,
        string_split(metaphlan_taxon_names, ',') AS taxa,
        string_split(ncbi_taxonomy_ids, ';') AS taxids
    FROM lake.bugsigdb.signatures
    WHERE metaphlan_taxon_names IS NOT NULL AND ncbi_taxonomy_ids IS NOT NULL
),
members AS (
    SELECT
        bsdb_id,
        ord AS member_index,
        trim(taxon_lineage) AS taxon_lineage,
        trim(taxids[ord]) AS taxon_lineage_ids,
        list_extract(string_split(trim(taxon_lineage), '|'), -1) AS leaf_raw
    FROM lists, UNNEST(taxa) WITH ORDINALITY AS u(taxon_lineage, ord)
    WHERE trim(taxon_lineage) <> ''
)
SELECT
    bsdb_id,
    member_index,
    regexp_extract(leaf_raw, '^([a-z])__', 1) AS taxon_rank,
    regexp_replace(leaf_raw, '^[a-z]__', '') AS taxon_name,
    TRY_CAST(list_extract(string_split(taxon_lineage_ids, '|'), -1) AS BIGINT) AS ncbitaxon_id,
    taxon_lineage,
    taxon_lineage_ids
FROM members

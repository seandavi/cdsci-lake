-- description: GWAS Catalog study->mapped trait bridge: one row per EFO-family term in a study's MAPPED_TRAIT_URI cell (cdsci-lake#169).
-- license: embl-ebi-terms-of-use
-- materialized: view
-- gwas_catalog.study_mapped_trait: the study-level twin of
-- gwas_catalog.association_mapped_trait, same CURIE rule, keyed by the parent's
-- own key. Columns:
--   study_accession  the parent's key (lake.gwas_catalog.study), `GCST...`
--   trait_id         `PREFIX:LOCAL` from the IRI tail, verbatim IRI otherwise
--
-- The background trait (MAPPED BACKGROUND TRAIT URI: 15,049 studies, 22,001
-- terms at 2026-10-22) is not bridged: it names the population a study was run
-- in, not the trait studied, and stays on the parent as a cell. Add a
-- study_background_trait view when a consumer wants to look studies up by it.
SELECT s.study_accession,
       regexp_replace(trim(u.uri), '^.*/([A-Za-z0-9]+)_([A-Za-z0-9]+)$', '\1:\2') AS trait_id
FROM lake.gwas_catalog.study AS s,
     UNNEST(str_split(s.mapped_trait_uri, ',')) AS u(uri)
WHERE s.mapped_trait_uri IS NOT NULL

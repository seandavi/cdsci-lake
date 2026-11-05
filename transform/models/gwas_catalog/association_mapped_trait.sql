-- description: GWAS Catalog association->mapped trait bridge: one row per EFO-family term in an association's MAPPED_TRAIT_URI cell (cdsci-lake#169).
-- license: embl-ebi-terms-of-use
-- materialized: view
-- gwas_catalog.association_mapped_trait (ADR-0027 "nested fields: sibling
-- tables"): the lake parent keeps `mapped_trait_uri` as the Catalog's raw
-- ', '-separated IRI cell; this view is the join key people want from it. Columns:
--   line_number  the parent's key (lake.gwas_catalog.association)
--   trait_id     the IRI's tail `/<PREFIX>_<LOCAL>` as a CURIE `PREFIX:LOCAL` --
--                `EFO:0008483`, `MONDO:0005344`, `OBA:VT0001253` -- which is the
--                `curie` form lake.ontology.terms uses for EFO (ontology = 'efo').
--                An IRI without that tail is kept verbatim.
--
-- The local id is alphanumeric, not numeric: OBA's VT-derived terms
-- (`OBA_VT0001253`, 103,911 of 1,316,773 mentions at 2026-10-22) are real EFO
-- terms and resolve in lake.ontology.terms only in this form. The prefix is the
-- Catalog's own: `Orphanet:` where EFO says `ORDO:` (11 of 16,875 distinct ids do
-- not resolve for that reason); mapping namespaces is a product's call.
-- (line_number, trait_id) is the grain: no cell repeats a term. No ordinal: the
-- cell lists the terms a trait maps to, in no order the Catalog defines. A view,
-- not a table (#169 refinement).
SELECT a.line_number,
       regexp_replace(trim(u.uri), '^.*/([A-Za-z0-9]+)_([A-Za-z0-9]+)$', '\1:\2') AS trait_id
FROM lake.gwas_catalog.association AS a,
     UNNEST(str_split(a.mapped_trait_uri, ',')) AS u(uri)
WHERE a.mapped_trait_uri IS NOT NULL

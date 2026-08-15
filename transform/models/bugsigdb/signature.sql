MODEL (
  name bugsigdb.signature,
  kind FULL,
  cron '@daily',
  tags ('license:cc-by-4.0'),
  description 'BugSigDB signature-level fields (curation metadata, description, taxon-member lists), one row per bsdb_id.',
  audits (bugsigdb_signature_bsdb_id_is_unique)
);

-- bugsigdb.signature (ADR-0015): one row per bsdb_id, same grain as
-- lake.bugsigdb.signatures itself but narrowed to the columns that actually
-- vary at signature grain -- curation/review metadata, the two taxon-member
-- columns, and free-text description/abundance -- plus FKs into study and
-- experiment. Everything constant within (study, experiment) lives in
-- bugsigdb.experiment instead; join back through experiment_id rather than
-- re-carrying those columns here. `signature_taxon.sql` (the taxon-member
-- bridge table) intentionally still reads lake.bugsigdb.signatures directly,
-- not this table -- it only needs bsdb_id + the two taxon columns, both
-- present identically on either source, and predates this decomposition.
SELECT
    bsdb_id,
    study AS study_id,
    study || '/' || regexp_extract(experiment, '\d+') AS experiment_id,
    signature_page_name,
    source_in_paper,
    curated_date,
    curator,
    revision_editor,
    reviewer,
    description,
    abundance_in_group_1,
    metaphlan_taxon_names,
    ncbi_taxonomy_ids
FROM lake.bugsigdb.signatures

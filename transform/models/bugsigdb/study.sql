MODEL (
  name bugsigdb.study,
  kind FULL,
  cron '@daily',
  tags ('license:cc-by-4.0'),
  description 'BugSigDB study-level fields (bibliographic), one row per study.',
  column_descriptions (
    study_id = 'This lake''s synthesized join key (a stringified PMID, verified 1:1 with `pmid`) -- not a citable external identifier, see the note below on why it''s `study_id` and not bare `study`.'
  ),
  audits (bugsigdb_study_study_id_is_unique)
);

-- bugsigdb.study (ADR-0015): one row per BugSigDB study, dedup'd from
-- lake.bugsigdb.signatures' study-level columns. `study_id` is the natural
-- key -- it's a stringified PMID in BugSigDB's own scheme, verified 1:1 with
-- `pmid` against the full v1.3.1 export (0 studies with >1 distinct pmid, 0
-- pmids with >1 distinct study). Named `study_id`, not the bare `study` the
-- source column originally carried -- it's this lake's own synthesized join
-- key (same reasoning as `experiment_id`), not a citable external identifier
-- like `pmid`/`bsdb_id`, so it gets the `_id` suffix that signals that
-- (2026-08-10 id-naming-convention session). Column set verified empirically
-- too: every column below has zero within-study variation across all 7,425
-- signature rows; anything that varied went into experiment.sql or
-- signature.sql instead.
SELECT
    study AS study_id,
    pmid,
    doi,
    url,
    authors_list,
    title,
    journal,
    year,
    keywords,
    study_design,
    state
FROM lake.bugsigdb.signatures
GROUP BY ALL

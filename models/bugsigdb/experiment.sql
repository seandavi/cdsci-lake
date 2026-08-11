-- description: BugSigDB experiment-level fields (subject groups, sequencing method, diversity metrics), one row per (study, experiment).
-- license: cc-by-4.0
-- column experiment_id: Reconstructs BugSigDB's own study/experiment_num numbering (from bsdb_id's shape) -- this lake's synthesized join key, not a citable external identifier.
-- bugsigdb.experiment (ADR-0015): one row per (study, experiment) -- BugSigDB
-- nests experiments under a study, and the raw "Experiment N" label is only
-- unique within its study, not globally (two different studies both have an
-- "Experiment 1"). `experiment_id` derives BugSigDB's own numbering instead of
-- inventing a surrogate key: bsdb_id is shaped study/experiment_num/signature_num
-- (e.g. `bsdb:41077329/9/2`), so `experiment_id` reconstructs the middle segment
-- (`41077329/9`) directly from `experiment`'s trailing digits.
--
-- Column set verified empirically: every column below is constant within
-- (study, experiment) but varies across experiments within the same study --
-- see study.sql (constant within study) / signature.sql (varies even within
-- an experiment) for the other two tiers.
SELECT
    study AS study_id,
    experiment,
    study || '/' || regexp_extract(experiment, '\d+') AS experiment_id,
    location_of_subjects,
    host_species,
    body_site,
    uberon_id,
    condition,
    efo_id,
    group_0_name,
    group_1_name,
    group_1_definition,
    group_0_sample_size,
    group_1_sample_size,
    antibiotics_exclusion,
    sequencing_type,
    variable_region_16s,
    sequencing_platform,
    data_transformation,
    statistical_test,
    significance_threshold,
    mht_correction,
    lda_score_above,
    matched_on,
    confounders_controlled_for,
    pielou,
    shannon,
    chao1,
    simpson,
    inverse_simpson,
    richness
FROM lake.bugsigdb.signatures
GROUP BY ALL

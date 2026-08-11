-- description: PMID<->DOI<->PMCID<->grant<->NCT crosswalk, one row per PMID, merged across icite/pmc/openalex/reporter/ctgov.
-- license: mixed -- icite (us-public-domain), pmc (mixed-oa, per-article), openalex (cc0), reporter (us-public-domain), ctgov (us-public-domain). pmc's per-article terms are the binding constraint; check the source article before redistributing a pmc-derived row.
-- ref.id_crosswalk (ROADMAP.md, ADR-0015 pilot model): PMID <-> DOI <-> PMCID <->
-- core_project_num <-> NCT. One row per PMID -- the common key across all five
-- source tables. DOI/PMCID are single-valued (any_value across sources, since a
-- pmid rarely disagrees on its own DOI/PMCID); grants and trials are genuinely
-- many-to-one on a pmid (one paper can cite many grants/trials), kept as arrays
-- rather than exploded, so a consumer chooses whether to UNNEST.
--
-- ponytail: any_value() picks one source's DOI/PMCID per pmid with no conflict
-- resolution or provenance -- if sources disagree, whichever DuckDB scans first
-- wins silently. Upgrade to a per-source doi_map (favor icite > openalex > pmc)
-- if a consumer needs to know which source asserted an id, or a conflict shows
-- up in practice.
WITH pmids AS (
    SELECT pmid FROM lake.icite.metadata WHERE pmid IS NOT NULL
    UNION
    SELECT pmid FROM lake.pmc.documents WHERE pmid IS NOT NULL
    UNION
    SELECT pmid FROM lake.openalex.works WHERE pmid IS NOT NULL
    UNION
    SELECT pmid FROM lake.reporter.publink WHERE pmid IS NOT NULL
    UNION
    SELECT pmid FROM lake.ctgov.references WHERE pmid IS NOT NULL
),
doi_by_pmid AS (
    SELECT pmid, any_value(doi) AS doi
    FROM (
        SELECT pmid, lower(doi) AS doi FROM lake.icite.metadata WHERE doi IS NOT NULL
        UNION ALL
        SELECT pmid, lower(doi) AS doi FROM lake.openalex.works
        WHERE doi IS NOT NULL AND pmid IS NOT NULL
        UNION ALL
        SELECT pmid, lower(doi) AS doi FROM lake.pmc.documents WHERE doi IS NOT NULL
    )
    GROUP BY pmid
),
pmcid_by_pmid AS (
    SELECT pmid, any_value(pmcid) AS pmcid
    FROM (
        SELECT pmid, pmcid FROM lake.pmc.documents WHERE pmid IS NOT NULL
        UNION ALL
        SELECT pmid, pmcid FROM lake.openalex.works
        WHERE pmid IS NOT NULL AND pmcid IS NOT NULL
    )
    GROUP BY pmid
),
grants_by_pmid AS (
    SELECT pmid, list(DISTINCT project_number) AS core_project_nums
    FROM lake.reporter.publink
    WHERE pmid IS NOT NULL
    GROUP BY pmid
),
ncts_by_pmid AS (
    SELECT pmid, list(DISTINCT nct_id) AS nct_ids
    FROM lake.ctgov.references
    WHERE pmid IS NOT NULL
    GROUP BY pmid
)
SELECT
    p.pmid,
    d.doi,
    c.pmcid,
    g.core_project_nums,
    n.nct_ids
FROM pmids p
LEFT JOIN doi_by_pmid d USING (pmid)
LEFT JOIN pmcid_by_pmid c USING (pmid)
LEFT JOIN grants_by_pmid g USING (pmid)
LEFT JOIN ncts_by_pmid n USING (pmid)

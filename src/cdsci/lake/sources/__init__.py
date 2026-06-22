"""Source ingestors — one canonical corpus each, bulk-loaded into the lake.

Each subpackage (``icite``, ``reporter``, …) fetches a *source-faithful* bulk
dump and curates it into a ``lake.<table>``. A source imports :mod:`cri.lake` /
:mod:`cri.download` and **nothing else in the repo** — never another source,
never a project. Cohort scoping and entity resolution are a project's job
(ADR-0022), so they are deliberately absent here: these tables describe *all*
papers/grants, not "ours".
"""

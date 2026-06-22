"""``cri`` — the shared research-intelligence substrate.

A single repository with **strong internal separation** (ADR-0022/0023):

* ``cri`` (this package root) — the *platform library*: the DuckLake connection
  (:mod:`cri.lake`), shared config (:mod:`cri.config`), and I/O helpers
  (:mod:`cri.download`). Depends on nothing else in the repo.
* ``cri.sources.*`` — *ingestors*. Each pulls one canonical, source-faithful
  corpus (bulk dumps) into the lake. A source imports the platform library and
  **never another source or a project**.
* ``cri.projects.*`` — *consumers* (marts, dashboards, "ask" portals). Read the
  lake; own their cohort/entity-resolution logic. (Not yet migrated here — the
  existing ``cu_openalex.cancer_center`` package is the first such project.)

The boundary rule, stated so it can be enforced: the lake holds *what is true
about a paper/grant*; a project decides *and it belongs to our cohort*. Entity
resolution never enters a source or the shared lake.
"""

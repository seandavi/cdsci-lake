# 0010. MeSH controlled vocabulary + hierarchy as a `mesh` source

- Status: accepted
- Date: 2026-06-24

## Context

The lake has OpenAlex topics (`lake.openalex.topics` — an algorithmic taxonomy with
its own `domain→field→subfield→topic` hierarchy, assigned to works) but **no MeSH**.
MeSH (NLM Medical Subject Headings) is the human-curated controlled vocabulary that
indexes PubMed; it is clinically meaningful (what oncology/clinical researchers think
in), the bridge to ClinicalTrials.gov conditions and to drug/chemical vocabularies,
and **complementary to** — not a substitute for — OpenAlex topics. We want its
relationships and **hierarchy** in the lake so literature can be rolled up to broader
subject categories (e.g. cancer-center publications by disease area).

Two facts shape the design (both verified against the live catalog):

- **The article↔MeSH links already exist, keyed by UI.** `omicidx.pubmed_article.
  mesh_terms` is a structured semicolon-delimited string carrying descriptor **and**
  qualifier UIs, e.g. `D006801:Humans; D000653:Amniotic Fluid / Q000737:chemistry; …`.
  So the join from literature to MeSH is on stable `D…`/`Q…` UIs — no name matching.
  The one thing the flattening drops is the **major-topic flag** (MajorTopicYN).
- **No maintained Python library produces MeSH descriptor tables.** `pubmed_parser`
  parses MEDLINE articles, not the descriptor file; `pyMeSHSim` is unmaintained
  (no PyPI release, Py3.6, bcolz); `obonet`/`pronto` need an OBO/OWL MeSH that NLM
  does not publish; the SPARQL endpoint caps SELECT at 1000 rows with buggy
  pagination; the RDF N-Triples dump is ~2 GB. The canonical annual artifact is the
  descriptor **XML**, and a thin streaming parse is the lowest-total-effort path.

## Decision

**Add a dedicated `mesh` schema as its own source** (registered in `ops.SOURCES`;
annual cadence; `us-public-domain` / NLM). Two phases.

### Phase 1a — the vocabulary + hierarchy (build first; self-contained)

- **Source:** the MeSH descriptor + qualifier **XML** —
  `…/MESH_FILES/xmlmesh/desc20NN.xml` (~313 MB) and `qual20NN.xml` (~0.3 MB). **Skip
  Supplementary Concept Records** (`supp20NN.xml`, ~786 MB, updated *daily*, not
  descriptors) in Phase 1. Do **not** use the ASCII `.bin` (NLM discontinued it
  Jan 2026); the SPARQL endpoint (1000-row cap) and RDF dump (2 GB) were considered
  and rejected for the bulk build.
- **Parse:** stdlib `xml.etree.ElementTree.iterparse` (streaming, clear each
  `DescriptorRecord`) → bronze Parquet → curate — the same medallion path as
  ctgov/pmc. Streaming keeps memory flat and adds **no new dependency** (lxml is
  faster but not needed at annual cadence).
- **Tables:**
  - `mesh.descriptor` — `descriptor_ui` (key), `name`, `scope_note`, `snapshot_version`.
  - `mesh.tree` — **the hierarchy**, exploded one row per `(descriptor_ui,
    tree_number)` with a derived `parent_tree_number`. A descriptor has 0..N tree
    numbers (**polyhierarchy is native**), so this is a bridge table, never a column.
  - `mesh.qualifier` — the ~80 subheadings from `qual20NN.xml`.
  - `mesh.descriptor_qualifier` — allowable-qualifier bridge `(descriptor_ui,
    qualifier_ui)` from the descriptor file's `AllowableQualifiersList`.
  - `mesh.entry_term` — `(descriptor_ui, term, is_preferred)` synonyms (classify via
    the `PreferredConcept`/`ConceptPreferredTerm` flags, don't guess).
- **Hierarchy is the tree number.** Broader/narrower = tree-number prefix
  (`parent_tree_number`, or `tree_number LIKE 'C04%'` for "everything under
  Neoplasms"); deep traversal via a recursive CTE. No separate edge table required.

### Phase 1b — the literature edge (depends only on omicidx + Phase 1a)

- `mesh.article_heading` — `(pmid, descriptor_ui, qualifier_ui)`, materialized by
  exploding `omicidx.pubmed_article.mesh_terms` (split on `;` then `/`, extract the
  `D…`/`Q…` UIs). Joins `mesh.tree` for hierarchy rollups and `omicidx`/`icite`/
  `reporter.publink` for the literature graph.
- **Known fidelity gap:** `mesh_terms` lost the major/minor topic flag. Recovering it
  needs structured PubMed `MeshHeadingList` (MajorTopicYN) — ideally surfaced
  **upstream in omicidx** rather than re-parsed here. Tracked as a Phase 2 upgrade;
  Phase 1b ships without it.

### Out of scope / later

Supplementary Concept Records, PharmacologicalActions, see-also cross-refs (Phase 2).

**No `mesh ↔ openalex_topic` crosswalk** — researched, and **no canonical mapping
exists**: an OpenAlex Topic exposes only an OpenAlex id + a single Wikipedia URL (no
MeSH/UMLS/Wikidata), UMLS does not include OpenAlex topics, and the only published
alignment is a *derived, approximate* third-party paper. Per "build it only if
canonical", we do not. A derived bridge (topic → Wikipedia → Wikidata P486 → MeSH)
is possible but lossy and must be labelled non-canonical if ever built. Note the
real authoritative literature↔MeSH bridge is **PMID → MeSH** — which Phase 1b
provides — and that OpenAlex's *work-level* `mesh` field carries `is_major_topic`
(the flag omicidx's flattening drops), an alternative source for the Phase 2
major-topic upgrade should `openalex.works` ever keep `mesh`.

## Consequences

- Literature rolls up the MeSH tree on stable UIs — disease-area aggregation of
  publications, a clinically meaningful subject layer, and a bridge toward ctgov
  conditions and drug/chemical vocabularies.
- Small and annual: ~30k descriptors, a streaming XML parse, no new dependency. SCRs
  (the bulk of MeSH volume) are deferred until a concrete need appears.
- One honest limitation: major-topic weighting is absent until structured headings
  are sourced (preferably upstream). The vocabulary + hierarchy (Phase 1a) is
  unaffected and independently useful as a crosswalk target.

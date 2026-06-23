"""OpenAlex — the scholarly graph, read anonymously from the public S3 snapshot.

OpenAlex (CC0) is the one source that gives us the **ROR-keyed institution graph**
plus topics, funders, and sources — the missing primitive for peer-center
benchmarking (institution ↔ grants ↔ pubs). The works corpus is pruned by domain
(Life + Health Sciences), abstracts are reconstructed from the inverted index, and
``referenced_works`` is split into the ``openalex.work_references`` edge table.

Tables (schema ``openalex``):

* ``works``            — key ``id``; typed core + reconstructed ``abstract``,
  flattened ``primary_topic``/``open_access``/``primary_location``, ``pmid``/``pmcid``
  crosswalk ids, and ``authorships``/``topics``/``keywords``/``grants`` as JSON.
* ``work_references``  — key (``work_id``, ``referenced_work_id``); the citation graph
  as id→id edges (covers non-PubMed works iCite can't see).
* ``institutions`` / ``sources`` / ``funders`` / ``topics`` — the reference entities.

See ADR-0005 (prune + domain-filter policy) and ``docs/design/openalex.md``.
"""

from .ingest import ENTITIES, ingest_entity, ingest_works, part_urls, run

__all__ = ["ENTITIES", "ingest_entity", "ingest_works", "part_urls", "run"]

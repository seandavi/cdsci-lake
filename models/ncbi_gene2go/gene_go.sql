-- description: Direct GO annotations per Entrez gene, all taxa, resolved against the GO release in lake.ontology (cdsci-lake#39).
-- license: us-public-domain
-- column gene_id: NCBI Gene identifier (bioregistry prefix `ncbigene`). Bare local id.
-- column taxon_id: NCBI Taxonomy id (bioregistry prefix `ncbitaxon`), from each row's own tax_id.
-- column go_id: GO term CURIE, e.g. `GO:0006099` — joins lake.ontology.terms on (ontology='go', curie).
-- column evidence: GO evidence code (IEA, IDA, IMP, …). Part of the business key.
-- column qualifier: The GO relation NCBI asserts (enables, involved_in, located_in, part_of, NOT|…). Part of the business key.
-- column go_term: The term label as gene2go itself shipped it, i.e. as of NCBI's own GO snapshot.
-- column category: GO aspect — Process, Function or Component.
-- column go_label: The term's CURRENT label in this lake's GO release; NULL when go_id is absent from that release (obsoleted-and-removed, or the two snapshots have drifted).
-- column go_obsolete: TRUE when GO marks the term obsolete; NULL when go_id is not in this lake's GO release at all — never conflate "not obsolete" with "not found".
-- ncbi_gene2go.gene_go: DIRECT annotations only. The GOALL-style closure over
-- ancestor terms is a recursive walk of lake.ontology.edges and belongs in its
-- own model — computing it here would bake one ontology snapshot invisibly
-- into every annotation row.
--
-- Not taxon-scoped, unlike the per-species table this was ported from: raw is
-- landed whole here and every row carries its own tax_id, so one table covers
-- every organism and per-species scoping is a WHERE clause a consumer writes.
--
-- DISTINCT drops `pubmed`: it is an attribute of the citation, not of the
-- annotation, and rows identical but for their PMID list collapse once it is
-- gone. It stays whole in lake.ncbi_gene2go.gene2go for whoever needs it.
--
-- The LEFT JOIN is the point of the model: gene2go's own go_term is frozen at
-- whatever GO release NCBI last regenerated against, so `go_label`/`go_obsolete`
-- are what tell a consumer the annotation still resolves. LEFT, not INNER —
-- an annotation to a term this lake's GO release doesn't have is a fact worth
-- surfacing, not a row to silently drop. Requires the `ontology` source to have
-- been run for GO (cdsci-lake#40).
SELECT DISTINCT
    g.gene_id,
    g.taxon_id,
    g.go_id,
    g.evidence,
    g.qualifier,
    g.go_term,
    g.category,
    t.label AS go_label,
    t.obsolete AS go_obsolete
FROM lake.ncbi_gene2go.gene2go AS g
LEFT JOIN lake.ontology.terms AS t
       ON t.ontology = 'go' AND t.curie = g.go_id

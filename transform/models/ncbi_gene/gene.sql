-- description: NCBI's view of a gene — one row per GeneID, all taxa (cdsci-lake#36).
-- license: us-public-domain
-- column gene_id: NCBI Gene identifier (bioregistry prefix `ncbigene`). Bare local id, no embedded prefix.
-- column taxon_id: NCBI Taxonomy id (bioregistry prefix `ncbitaxon`). Bare local id.
-- column gene_type: NCBI's `type_of_gene` (protein-coding, pseudo, ncRNA, biological-region, …).
-- ncbi_gene.gene: the tidy projection of the raw gene_info landing table.
-- Unlike bioc-on-ice's annotation.ncbi__gene, this is NOT scoped to a taxon --
-- raw is landed whole here (every organism NCBI knows), and per-species scoping
-- is a WHERE clause a consumer writes, not something baked into the table. The
-- taxon comes from each row's own tax_id, not from which file it arrived in.
--
-- NEWENTRY is NCBI's placeholder for GeneRIF submissions against a gene that is
-- not in Gene: one row per taxon, and not a gene. Everything else stays,
-- including the biological-region records, which outnumber the genes.
SELECT
    gene_id,
    taxon_id,
    symbol,
    description,
    type_of_gene AS gene_type,
    chromosome,
    map_location
FROM lake.ncbi_gene.gene_info
WHERE symbol IS DISTINCT FROM 'NEWENTRY'

-- test: no empty or null identifiers on either side
SELECT * FROM lake.ncbi_gene2accession.mapping
WHERE source_id IS NULL OR target_id IS NULL
   OR trim(source_id) = '' OR trim(target_id) = ''

-- test: the whole tuple is distinct — SELECT DISTINCT holds the grain
SELECT source_namespace, source_id, target_namespace, target_id, taxon_id
FROM lake.ncbi_gene2accession.mapping
GROUP BY ALL HAVING count(*) > 1

-- test: the discriminator actually separated RefSeq from GenBank. A REFSEQ_*
-- target must have the two-letters-then-underscore RefSeq shape (a bare
-- `contains(_)` would also admit PDB chain accessions like 3SID_A.1, which the
-- real dump does put in the protein column), and a GENBANK_* target must have
-- no underscore at all.
SELECT * FROM lake.ncbi_gene2accession.mapping
WHERE (starts_with(target_namespace, 'GENBANK') AND contains(target_id, '_'))
   OR (starts_with(target_namespace, 'REFSEQ')
       AND NOT regexp_matches(target_id, '^[A-Z]{2}_'))

-- test: every REFSEQ target carries a known RefSeq prefix — the underscore rule
-- is only sound as long as underscored == RefSeq
SELECT * FROM lake.ncbi_gene2accession.mapping
WHERE starts_with(target_namespace, 'REFSEQ')
  AND split_part(target_id, '_', 1) NOT IN
      ('NM', 'NR', 'XM', 'XR', 'NP', 'XP', 'YP', 'AP', 'WP', 'NC', 'NG', 'NT', 'NW', 'NZ', 'AC')

-- test: only ENTREZ on the source side
SELECT * FROM lake.ncbi_gene2accession.mapping WHERE source_namespace <> 'ENTREZ'

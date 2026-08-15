-- no empty or null identifiers on either side
AUDIT (
  name ncbi_gene2accession_mapping_no_empty_or_null_identifiers_on,
);
SELECT * FROM @this_model
WHERE source_id IS NULL OR target_id IS NULL
   OR trim(source_id) = '' OR trim(target_id) = '';

-- the whole tuple is distinct — SELECT DISTINCT holds the grain
AUDIT (
  name ncbi_gene2accession_mapping_the_whole_tuple_is_distinct_select,
);
SELECT source_namespace, source_id, target_namespace, target_id, taxon_id
FROM @this_model
GROUP BY ALL HAVING count(*) > 1;

-- the discriminator actually separated RefSeq from GenBank. A REFSEQ_*
AUDIT (
  name ncbi_gene2accession_mapping_the_discriminator_actually_separated_refseq_from,
);
-- target must have the two-letters-then-underscore RefSeq shape (a bare
-- `contains(_)` would also admit PDB chain accessions like 3SID_A.1, which the
-- real dump does put in the protein column), and a GENBANK_* target must have
-- no underscore at all.
SELECT * FROM @this_model
WHERE (starts_with(target_namespace, 'GENBANK') AND contains(target_id, '_'))
   OR (starts_with(target_namespace, 'REFSEQ')
       AND NOT regexp_matches(target_id, '^[A-Z]{2}_'));

-- every REFSEQ target carries a known RefSeq prefix — the underscore rule
AUDIT (
  name ncbi_gene2accession_mapping_every_refseq_target_carries_a_known,
);
-- is only sound as long as underscored == RefSeq
SELECT * FROM @this_model
WHERE starts_with(target_namespace, 'REFSEQ')
  AND split_part(target_id, '_', 1) NOT IN
      ('NM', 'NR', 'XM', 'XR', 'NP', 'XP', 'YP', 'AP', 'WP', 'NC', 'NG', 'NT', 'NW', 'NZ', 'AC');

-- only ENTREZ on the source side
AUDIT (
  name ncbi_gene2accession_mapping_only_entrez_on_the_source_side,
);
SELECT * FROM @this_model WHERE source_namespace <> 'ENTREZ';

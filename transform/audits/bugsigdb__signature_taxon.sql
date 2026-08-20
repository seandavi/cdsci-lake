-- (bsdb_id, member_index) is unique
AUDIT (
  name bugsigdb_signature_taxon_bsdb_id_member_index_is_unique,
);
SELECT bsdb_id, member_index FROM @this_model
GROUP BY bsdb_id, member_index HAVING count(*) > 1;

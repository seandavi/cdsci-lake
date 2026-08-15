-- bsdb_id is unique
AUDIT (
  name bugsigdb_signature_bsdb_id_is_unique,
);
SELECT bsdb_id FROM @this_model GROUP BY bsdb_id HAVING count(*) > 1;

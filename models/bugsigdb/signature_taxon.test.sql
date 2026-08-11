-- test: (bsdb_id, member_index) is unique
SELECT bsdb_id, member_index FROM lake.bugsigdb.signature_taxon
GROUP BY bsdb_id, member_index HAVING count(*) > 1

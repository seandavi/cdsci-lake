-- test: bsdb_id is unique
SELECT bsdb_id FROM lake.bugsigdb.signature GROUP BY bsdb_id HAVING count(*) > 1

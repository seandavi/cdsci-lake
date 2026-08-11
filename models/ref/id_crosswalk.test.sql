-- test: pmid is unique
SELECT pmid FROM lake.ref.id_crosswalk GROUP BY pmid HAVING count(*) > 1

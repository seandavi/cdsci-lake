-- test: study_id is unique
SELECT study_id FROM lake.bugsigdb.study GROUP BY study_id HAVING count(*) > 1

-- test: experiment_id is unique
SELECT experiment_id FROM lake.bugsigdb.experiment GROUP BY experiment_id HAVING count(*) > 1

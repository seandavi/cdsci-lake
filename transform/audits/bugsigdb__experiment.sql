-- experiment_id is unique
AUDIT (
  name bugsigdb_experiment_experiment_id_is_unique,
);
SELECT experiment_id FROM @this_model GROUP BY experiment_id HAVING count(*) > 1;

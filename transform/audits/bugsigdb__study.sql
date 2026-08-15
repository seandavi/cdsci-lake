-- study_id is unique
AUDIT (
  name bugsigdb_study_study_id_is_unique,
);
SELECT study_id FROM @this_model GROUP BY study_id HAVING count(*) > 1;

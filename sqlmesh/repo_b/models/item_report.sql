MODEL (
  name repo_b_example.item_report,
  kind FULL,
  cron '@daily',
  grain item_id
);

-- Cross-repo: sqlmesh_example.full_model is owned by repo_a. Does this resolve
-- when repo_b is planned alone (repo_a's models injected from shared state)?
SELECT
  c.item_id,
  c.item_name,
  f.num_orders
FROM repo_b_example.item_catalog AS c
LEFT JOIN sqlmesh_example.full_model AS f
  ON f.item_id = c.item_id

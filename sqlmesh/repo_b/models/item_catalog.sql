MODEL (
  name repo_b_example.item_catalog,
  kind FULL,
  cron '@daily',
  grain item_id
);

-- Standalone on purpose: repo_b must be plannable alone to isolate the
-- multi-repo state behavior from any cross-repo dependency.
SELECT
  item_id::INT AS item_id,
  item_name::TEXT AS item_name
FROM (
  VALUES (1, 'widget'), (2, 'gadget'), (3, 'doohickey')
) AS t(item_id, item_name)

MODEL (
  name sqlmesh_example.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (assert_positive_order_ids),
);

SELECT
  item_id,
  COUNT(id) * 100 AS num_orders,
  MAX(event_date) AS last_order_date,
  MIN(event_date) AS first_order_date,
FROM
  sqlmesh_example.incremental_model
GROUP BY item_id
  
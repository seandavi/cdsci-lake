MODEL (
  name repo_b_example.item_summary,
  kind FULL,
  cron '@daily',
  grain item_id
);

-- Built in the `cdsci` env only, then promoted to prod — tests whether the
-- env choice is an up-front commitment or a cheap, reversible plan-time call.
SELECT
  item_id,
  item_name,
  COALESCE(num_orders, 0) AS num_orders
FROM repo_b_example.item_report

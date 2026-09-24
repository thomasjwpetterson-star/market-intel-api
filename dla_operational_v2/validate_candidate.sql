WITH supply AS (
  SELECT *
  FROM market_intel_gold.metric_niin_supply_state_20260916_candidate
),
price AS (
  SELECT *
  FROM market_intel_gold.metric_niin_price_summary_20260916_candidate
),
inventory_latest AS (
  SELECT *
  FROM market_intel_silver.fact_dla_inventory_snapshot_20260916_candidate
  WHERE snapshot_date = '2026-08-31'
    AND public_release_eligible
),
reorder_latest AS (
  SELECT *
  FROM market_intel_silver.fact_dla_reorder_point_snapshot_20260916_candidate
  WHERE snapshot_date = '2026-08-31'
    AND public_release_eligible
)
SELECT
  (SELECT count(*) FROM supply) AS supply_rows,
  (SELECT count(DISTINCT niin) FROM supply) AS supply_distinct_niins,
  (SELECT count(*) FROM supply WHERE backorder_qty > 0) AS backordered_niins,
  (SELECT count(*) FROM supply WHERE below_reorder_point) AS below_rop_niins,
  (SELECT count(*) FROM supply WHERE forecast_12m_qty > 0) AS forecast_12m_niins,
  (SELECT count(*) FROM supply WHERE stock_sources_agree = false) AS stock_disagreements,
  (SELECT count(*) FROM price) AS price_rows,
  (SELECT count(DISTINCT niin) FROM price) AS price_distinct_niins,
  (SELECT count(*) FROM inventory_latest) AS latest_inventory_source_rows,
  (SELECT count(DISTINCT niin) FROM inventory_latest) AS latest_inventory_niins,
  (SELECT count(*) FROM reorder_latest) AS latest_reorder_source_rows,
  (SELECT count(DISTINCT niin) FROM reorder_latest) AS latest_reorder_niins

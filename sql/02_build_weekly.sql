TRUNCATE TABLE core.sku_weekly;

INSERT INTO core.sku_weekly (
  week_start,
  sku_id,
  units_week,
  avg_net_price,
  avg_discount_pct,
  promo_flag_week
)
SELECT
  date_trunc('week', invoice_date)::date AS week_start,
  sku_id,
  SUM(units) AS units_week,
  AVG(net_unit_price) AS avg_net_price,
  AVG(discount_pct) AS avg_discount_pct,
  MAX(promo_flag) AS promo_flag_week
FROM core.line_items
GROUP BY 1, 2
ORDER BY 1, 2;
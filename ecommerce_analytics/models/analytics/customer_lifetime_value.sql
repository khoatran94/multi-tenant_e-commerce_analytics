{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics'
) }}
-- Output: period_month + clv + supporting metrics

WITH date_bounds AS (
  SELECT
    DATE_TRUNC('month', MIN(purchased_at))::DATE AS first_month,
    DATE_TRUNC('month', MAX(purchased_at))::DATE AS last_month
  FROM {{ ref('fact_orders') }}
  WHERE is_delivered = TRUE
),

date_series AS (
  SELECT
    (first_month + (gs.n * INTERVAL '1 month'))::DATE AS period_month
  FROM date_bounds
  CROSS JOIN LATERAL (
    SELECT generate_series(
      0,
      (EXTRACT(YEAR FROM last_month) - EXTRACT(YEAR FROM first_month)) * 12 +
      (EXTRACT(MONTH FROM last_month) - EXTRACT(MONTH FROM first_month))
    ) AS n
  ) gs
),

monthly_data AS (
  SELECT
    DATE_TRUNC('month', purchased_at)::DATE AS period_month,
    customer_unique_id,
    COUNT(DISTINCT order_id) AS orders_in_period,
    SUM(item_price + freight_cost) AS revenue_in_period
  FROM {{ ref('fact_orders') }}
  WHERE is_delivered = TRUE
  GROUP BY 1, 2
),

monthly_agg AS (
  SELECT
    ds.period_month,

    -- Supporting metrics
    COALESCE(SUM(md.revenue_in_period), 0) AS total_revenue_in_period,
    COALESCE(SUM(md.orders_in_period), 0) AS total_orders_in_period,
    COALESCE(COUNT(DISTINCT md.customer_unique_id), 0) AS customers_in_period,

    -- CLV components
    COALESCE(
      NULLIF(SUM(md.revenue_in_period), 0) / NULLIF(SUM(md.orders_in_period), 0),
      0
    ) AS avg_purchase_value,

    COALESCE(
      NULLIF(SUM(md.orders_in_period)::NUMERIC, 0) / NULLIF(COUNT(DISTINCT md.customer_unique_id), 0),
      0
    ) AS avg_frequency,

    -- inclusive lifespan +1
    COALESCE(
      AVG(EXTRACT(DAY FROM (c.last_order_date - c.first_order_date + INTERVAL '1 day'))::NUMERIC / 30.4375),
      0
    ) AS avg_lifespan_months

  FROM date_series ds
  LEFT JOIN monthly_data md ON ds.period_month = md.period_month
  LEFT JOIN {{ ref('dim_customer') }} c
    ON md.customer_unique_id = c.customer_unique_id
  GROUP BY ds.period_month
)

SELECT
  period_month,
  ROUND(avg_purchase_value * avg_frequency * avg_lifespan_months, 2) AS clv,
  total_revenue_in_period,
  total_orders_in_period,
  customers_in_period,
  ROUND(avg_lifespan_months, 2) AS avg_lifespan_months
FROM monthly_agg
ORDER BY period_month

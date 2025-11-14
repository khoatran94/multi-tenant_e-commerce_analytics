{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics'
) }}

SELECT
  c.customer_unique_id,
  c.city,
  c.state,
  c.first_order_date,
  c.last_order_date,

  c.total_orders,
  c.total_item_price,
  c.total_freight_cost,
  c.total_item_price + c.total_freight_cost                             AS total_spent,

  ROUND(c.total_item_price / NULLIF(c.total_orders, 0), 2)         AS avg_item_price,
  ROUND(c.total_freight_cost / NULLIF(c.total_orders, 0), 2)       AS avg_freight_per_order,
  ROUND((c.total_item_price + c.total_freight_cost) / NULLIF(c.total_orders, 0), 2) AS avg_order_value,

  EXTRACT(DAY FROM (c.last_order_date - c.first_order_date + INTERVAL '1 day')) AS customer_lifetime_days,
  EXTRACT(DAY FROM (CURRENT_DATE - c.last_order_date)) AS days_since_last_order,

  ROUND(
    EXTRACT(DAY FROM (c.last_order_date - c.first_order_date))::NUMERIC / NULLIF(c.total_orders, 0),
    1
  ) AS order_frequency_days

FROM {{ ref('dim_customer') }} c
WHERE c.total_orders > 0

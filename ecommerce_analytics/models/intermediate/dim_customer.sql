{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (customer_unique_id)"
    ]
) }}

WITH customer_addresses AS (
  -- Most frequent address per customer_unique_id
  SELECT
    c.customer_unique_id,
    c.zip_code,
    c.customer_city as city,
    c.customer_state as state,
    COUNT(*) as addr_count,
    ROW_NUMBER() OVER (
      PARTITION BY c.customer_unique_id
      ORDER BY COUNT(*) DESC, MAX(o.purchased_at) DESC
    ) as rn
  FROM {{ ref('stg_customers') }} c
  JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
  GROUP BY c.customer_unique_id, c.zip_code, c.customer_city, c.customer_state
),

most_frequent_address AS (
  SELECT
    customer_unique_id,
    zip_code,
    city,
    state
  FROM customer_addresses
  WHERE rn = 1
)

SELECT
  c.customer_unique_id,
  addr.zip_code,
  addr.city,
  addr.state,
  MIN(o.purchased_at) as first_order_date,
  MAX(o.purchased_at) as last_order_date,
  COUNT(DISTINCT o.order_id) as total_orders,
  COALESCE(
    SUM(CASE WHEN o.order_status = 'delivered' THEN oi.item_price + oi.freight_cost END),
    0
  ) as total_spent
FROM {{ ref('stg_customers') }} c
JOIN {{ ref('stg_orders') }} o ON c.customer_id = o.customer_id
LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
JOIN most_frequent_address addr ON c.customer_unique_id = addr.customer_unique_id
GROUP BY
  c.customer_unique_id,
  addr.zip_code,
  addr.city,
  addr.state

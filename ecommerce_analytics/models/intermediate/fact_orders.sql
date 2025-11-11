{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (order_id, order_item_seq)"
    ]
) }}

SELECT
  -- Composite primary key (grain = one row per order item)
  oi.order_id,
  oi.order_item_seq,                                         

  -- Foreign keys (natural keys)
  o.customer_id,
  c.customer_unique_id,
  oi.product_id,

  -- Order status & flags
  o.order_status,
  o.order_status = 'delivered'                               AS is_delivered,
  o.order_status IN ('canceled', 'unavailable')              AS is_canceled,
  CASE 
    WHEN o.delivered_at > o.estimated_delivery 
    THEN TRUE ELSE FALSE 
  END                                                        AS is_late,

  -- Timestamps
  o.purchased_at,
  o.approved_at,
  o.shipped_at,
  o.delivered_at,
  o.estimated_delivery,

  -- Monetary values (per item)
  oi.item_price                                              AS item_price,
  oi.freight_cost                                            AS freight_cost,
  (oi.item_price + oi.freight_cost)                          AS total_item_value

FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.customer_id

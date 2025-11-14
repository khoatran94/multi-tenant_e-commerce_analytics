{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (product_id)"]
) }}

WITH product_with_english AS (
  SELECT
    p.product_id,
    p.product_category,
    p.name_length,
    p.description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    COALESCE(t.product_category_english, p.product_category) AS product_category_en
  FROM {{ ref('stg_products') }} p
  LEFT JOIN {{ ref('stg_product_category_translation') }} t
    ON p.product_category = t.product_category_portuguese
)

SELECT
  p.product_id,
  p.product_category_en AS product_category,
  p.name_length,
  p.description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,

  MIN(CASE 
  	WHEN o.order_status = 'delivered' THEN o.purchased_at 
      END) AS first_seen_at,
  MAX(CASE 
  	WHEN o.order_status = 'delivered' THEN o.purchased_at 
      END) AS last_seen_at,
  COUNT(DISTINCT CASE 
                   WHEN o.order_status = 'delivered' THEN oi.order_id 
                 END) AS total_orders,
  COUNT(CASE 
            WHEN o.order_status = 'delivered' THEN oi.order_item_seq 
        END) AS total_units_sold,
  COALESCE(SUM(CASE 
  		  WHEN o.order_status = 'delivered' THEN oi.item_price 
  		END), 
  	   0)     AS total_item_price,
  COALESCE(SUM(CASE 
  		  WHEN o.order_status = 'delivered' THEN oi.freight_cost 
  	       END), 
  	   0)   AS total_freight_cost

FROM product_with_english p
LEFT JOIN {{ ref('stg_order_items') }} oi ON p.product_id = oi.product_id
LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
GROUP BY
  p.product_id,
  p.product_category_en,
  p.name_length,
  p.description_length,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm

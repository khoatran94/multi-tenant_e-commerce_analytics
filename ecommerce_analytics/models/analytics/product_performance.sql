{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics'
) }}

WITH product_metrics AS (
    SELECT
        p.product_id,
        COALESCE(p.product_category, 'Unknown') AS product_category,
        p.name_length,
        p.description_length,
        p.product_photos_qty,
        ROUND(p.product_weight_g / 1000.0, 2) AS weight_kg,
        ROUND(p.product_length_cm * p.product_height_cm * p.product_width_cm / 1000000.0, 3) AS volume_m3,

        p.first_seen_at,
        p.last_seen_at,

        p.total_orders,
        p.total_units_sold,
        p.total_item_price,
        p.total_freight_cost,
        (p.total_item_price + p.total_freight_cost) AS total_gmv,

        ROUND(COALESCE((p.total_item_price + p.total_freight_cost) / NULLIF(p.total_orders,0), 0), 2) AS gmv_per_order,

        -- % of category GMV
        ROUND((p.total_item_price + p.total_freight_cost) / NULLIF(SUM(p.total_item_price + p.total_freight_cost) 
              OVER (PARTITION BY COALESCE(p.product_category, 'Unknown')), 0), 4) AS pct_of_category_gmv,

        -- % of total units sold
        ROUND(p.total_orders::NUMERIC / NULLIF(SUM(p.total_orders) OVER (), 0), 4) AS pct_of_total_units_sold

    FROM {{ ref('dim_product') }} p
),

reviews AS (
    SELECT
        oi.product_id,
        COUNT(*) AS total_reviews,
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        COUNT(CASE WHEN r.review_score >= 4 THEN 1 END) AS positive_reviews,
        COUNT(CASE WHEN r.review_score <= 2 THEN 1 END) AS negative_reviews,
        ROUND(COUNT(CASE WHEN r.review_score >= 4 THEN 1 END)::NUMERIC / NULLIF(COUNT(*),0),4) AS pct_positive_review,
        ROUND(COUNT(CASE WHEN r.review_score <= 2 THEN 1 END)::NUMERIC / NULLIF(COUNT(*),0),4) AS pct_negative_review
    FROM {{ ref('stg_order_items') }} oi
    JOIN {{ ref('stg_reviews') }} r
        ON oi.order_id = r.order_id
    GROUP BY oi.product_id
)

SELECT
    pm.*,
    COALESCE(r.total_reviews, 0)       AS total_reviews,
    COALESCE(r.avg_review_score, 0)    AS avg_review_score,
    COALESCE(r.positive_reviews, 0)    AS positive_reviews,
    COALESCE(r.pct_positive_review, 0)  AS pct_positive_review,
    COALESCE(r.negative_reviews, 0)    AS negative_reviews,
    COALESCE(r.pct_negative_review, 0)  AS pct_negative_review,

    ROW_NUMBER() OVER (ORDER BY pm.total_gmv DESC)        AS gmv_rank,
    ROW_NUMBER() OVER (ORDER BY pm.total_orders DESC)     AS units_rank

FROM product_metrics pm
LEFT JOIN reviews r ON pm.product_id = r.product_id
ORDER BY total_gmv DESC

{{ config(
    materialized='table',
    schema=var('tenant_id') ~ '_analytics'
) }}

-- DAILY SALES SUMMARY — CEO Dashboard (optimized, fast, continuous)
-- Your genius optimization: use dim_customer.first_order_date → no subquery hell

WITH date_series AS (
    SELECT generate_series(
        (SELECT DATE(MIN(purchased_at)) FROM {{ ref('fact_orders') }} WHERE is_delivered),
        (SELECT DATE(MAX(purchased_at)) FROM {{ ref('fact_orders') }} WHERE is_delivered),
        interval '1 day'
    )::DATE AS sales_date
),

daily_orders AS (
    SELECT
        DATE(o.purchased_at) AS sales_date,
        COUNT(DISTINCT o.order_id)                    AS total_orders,
        COUNT(DISTINCT o.customer_unique_id)          AS unique_customers,
        SUM(o.item_price + o.freight_cost)            AS total_gmv,
        SUM(o.item_price)                             AS total_revenue,
        SUM(o.freight_cost)                           AS total_freight,
        -- YOUR GENIUS: returning = has ordered before today
        COUNT(DISTINCT CASE 
            WHEN c.first_order_date < DATE(o.purchased_at) 
            THEN o.customer_unique_id 
        END)                                          AS returning_customers
    FROM {{ ref('fact_orders') }} o
    LEFT JOIN {{ ref('dim_customer') }} c
        ON o.customer_unique_id = c.customer_unique_id
    WHERE o.is_delivered
    GROUP BY DATE(o.purchased_at)
),

daily_items AS (
    SELECT
        DATE(o.purchased_at) AS sales_date,
        AVG(oi.items_per_order)                       AS avg_items_per_order,
        SUM(oi.items_per_order)                       AS total_items_sold
    FROM {{ ref('fact_orders') }} o
    JOIN (
        SELECT order_id, MAX(order_item_seq) AS items_per_order
        FROM {{ ref('stg_order_items') }}
        GROUP BY order_id
    ) oi ON o.order_id = oi.order_id
    WHERE o.is_delivered
    GROUP BY DATE(o.purchased_at)
)

SELECT
    ds.sales_date,
    
    COALESCE(d_orders.total_orders, 0)                    AS total_orders,
    COALESCE(d_orders.unique_customers, 0)                AS unique_customers,
    COALESCE(d_orders.returning_customers, 0)             AS returning_customers,
    ROUND(100.0 * COALESCE(d_orders.returning_customers,0) / NULLIF(d_orders.unique_customers,0), 2) AS pct_returning_customers,
    
    COALESCE(d_orders.total_revenue, 0)                   AS total_revenue,
    COALESCE(d_orders.total_gmv, 0)                       AS total_gmv,
    COALESCE(d_orders.total_freight, 0)                   AS total_freight,
    ROUND(COALESCE(d_orders.total_freight,0) / NULLIF(d_orders.total_revenue,0), 3) AS freight_ratio,
    
    ROUND(COALESCE(d_orders.total_gmv,0) / NULLIF(d_orders.total_orders,0), 2)      AS avg_order_value,
    ROUND(COALESCE(d_orders.total_revenue,0) / NULLIF(d_orders.total_orders,0), 2)          AS avg_order_value_excl_freight,
    ROUND(COALESCE(di.avg_items_per_order,0), 2)                            AS avg_items_per_order,
    ROUND(COALESCE(d_orders.total_revenue,0) / NULLIF(di.total_items_sold,0), 2)      AS avg_item_price,
    
    CASE WHEN LAG(d_orders.total_revenue) OVER (ORDER BY ds.sales_date) IS NULL THEN NULL
         ELSE ROUND(100.0 * (d_orders.total_revenue - LAG(d_orders.total_revenue) OVER (ORDER BY ds.sales_date)) 
                    / LAG(d_orders.total_revenue) OVER (ORDER BY ds.sales_date), 2)
    END                                            AS revenue_growth_pct,
    
    COALESCE(d_orders.total_revenue > 0, FALSE) AS has_sales

FROM date_series ds
LEFT JOIN daily_orders d_orders ON ds.sales_date = d_orders.sales_date
LEFT JOIN daily_items di ON ds.sales_date = di.sales_date
ORDER BY ds.sales_date

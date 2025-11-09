{{ config(
    materialized='view',
    schema=var('tenant_id') ~ '_core'
) }}

select
  order_id,
  order_item_id::int as order_item_seq,
  product_id,
  seller_id,
  price::numeric(12,2) as item_price,
  freight_value::numeric(12,2) as freight_cost,
  shipping_limit_date::timestamp as shipping_limit_at
from {{ source('raw_' ~ var('tenant_id'), 'order_items') }}

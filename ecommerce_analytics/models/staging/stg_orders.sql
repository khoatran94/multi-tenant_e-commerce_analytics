{{ config(materialized='view', schema= var('tenant_id') ~ '_core') }}

select
  order_id,
  customer_id,
  order_status,
  NULLIF(TRIM(order_purchase_timestamp), '')::timestamp as purchased_at,
  NULLIF(TRIM(order_approved_at), '')::timestamp as approved_at,
  NULLIF(TRIM(order_delivered_carrier_date), '')::timestamp as shipped_at,
  NULLIF(TRIM(order_delivered_customer_date), '')::timestamp as delivered_at,
  NULLIF(TRIM(order_estimated_delivery_date), '')::timestamp as estimated_delivery
from {{ source('raw_' ~ var('tenant_id'), 'orders') }}

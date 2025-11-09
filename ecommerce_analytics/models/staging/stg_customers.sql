{{ config(materialized='view', schema= var('tenant_id') ~ '_core'  ) }}
select
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix::int as zip_code,
  customer_city,
  customer_state
from {{ source('raw_' ~ var('tenant_id'), 'customers') }}

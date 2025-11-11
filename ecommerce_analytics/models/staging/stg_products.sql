{{ config(materialized='view', schema= var('tenant_id') ~ '_core') }}

select
  product_id,
  NULLIF(TRIM(product_category_name), '') as product_category,
  NULLIF(TRIM(product_name_lenght), '')::int as name_length,
  NULLIF(TRIM(product_description_lenght), '')::int as description_length,
  NULLIF(TRIM(product_photos_qty), '')::int as product_photos_qty,
  NULLIF(TRIM(product_weight_g), '')::int as product_weight_g,
  NULLIF(TRIM(product_length_cm), '')::int as product_length_cm,
  NULLIF(TRIM(product_height_cm), '')::int as product_height_cm,
  NULLIF(TRIM(product_width_cm), '')::int as product_width_cm
from {{ source('raw_' ~ var('tenant_id'), 'products') }}

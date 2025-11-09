{{ config(materialized='view', schema= var('tenant_id') ~ '_core') }}

select
  product_id,
  product_category_name as product_category,
  product_name_lenght::int as name_length,
  product_description_lenght::int as description_length,
  product_photos_qty::int,
  product_weight_g::int,
  product_length_cm::int,
  product_height_cm::int,
  product_width_cm::int
from {{ source('raw_' ~ var('tenant_id'), 'products') }}

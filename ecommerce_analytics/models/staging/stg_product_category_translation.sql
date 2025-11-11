{{ config(
    materialized='view',
    schema=var('tenant_id') ~ '_core'
) }}

SELECT
  _product_category_name AS product_category_portuguese,
  product_category_name_english AS product_category_english
FROM {{ source('raw_' ~ var('tenant_id'), 'product_category_name_translation') }}

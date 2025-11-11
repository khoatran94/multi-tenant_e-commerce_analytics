{{ config(materialized='view', schema= var('tenant_id') ~ '_core') }}

select
  review_id,
  order_id,
  review_score::int,
  NULLIF(TRIM(review_comment_title), '') as review_comment_title,
  NULLIF(TRIM(review_comment_message), '') as review_comment_message,
  review_creation_date::timestamp as review_at,
  review_answer_timestamp::timestamp as answered_at
from {{ source('raw_' ~ var('tenant_id'), 'order_reviews') }}

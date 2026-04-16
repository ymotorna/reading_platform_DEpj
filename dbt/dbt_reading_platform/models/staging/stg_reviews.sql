-- raw (source) -> stg \\ +sk

{{ config(tags=['hourly']) }}

select
    {{ dbt_utils.generate_surrogate_key(['review_id']) }} as review_sk,
    *

from {{ source('external_source', 'reviews') }}






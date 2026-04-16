-- raw (source) -> stg \\ +sk

{{ config(tags=['hourly']) }}

select
    {{ dbt_utils.generate_surrogate_key(['payment_id']) }} as payment_sk,
    *

from {{ source('external_source', 'payments') }}






-- raw (source) -> stg \\ +sk

{{ config(tags=['hourly']) }}

select
    {{ dbt_utils.generate_surrogate_key(['session_id']) }} as session_sk,
    *

from {{ source('external_source', 'reading_sessions') }}






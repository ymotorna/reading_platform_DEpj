-- raw (source) -> stg \\ +sk

{{ config(tags=['daily']) }}

select
    {{ dbt_utils.generate_surrogate_key(['author_id']) }} as author_sk,
    *

from {{ source('external_source', 'authors') }}






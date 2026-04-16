-- raw (source) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['author_id']) }} as author_sk,
    *

from {{ source('external_source', 'authors') }}






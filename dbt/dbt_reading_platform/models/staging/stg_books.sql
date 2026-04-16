-- raw (source) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['book_id']) }} as book_sk,
    *

from {{ source('external_source', 'books') }}






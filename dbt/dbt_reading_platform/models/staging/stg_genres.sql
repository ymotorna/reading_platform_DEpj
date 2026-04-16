-- raw (seed) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['genre_id']) }} as genre_sk,
    *

from {{ ref('genres') }}
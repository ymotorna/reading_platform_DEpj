-- raw (seed) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['language_code']) }} as language_sk,
    *

from {{ ref('languages') }}
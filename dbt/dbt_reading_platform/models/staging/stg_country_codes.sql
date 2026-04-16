-- raw (seed) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_sk,
    *

from {{ ref('country_codes') }}
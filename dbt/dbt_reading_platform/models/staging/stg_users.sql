-- raw -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
    *

from {{ source('external_source', ref('users') }}






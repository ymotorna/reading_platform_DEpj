-- raw -> stg \\ +sk

{{ config(tags=['daily']) }}

select
    {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_sk,
    *

from {{ source('external_source', ref('users') }}






-- raw (seed) -> stg \\ +sk

select
    {{ dbt_utils.generate_surrogate_key(['plan_type']) }} as sub_plan_sk,
    *

from {{ ref('subscription_types') }}
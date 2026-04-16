-- seed -> raw \\ data as is

with sourced as (

    select * from {{ ref('subscription_types') }}

)

select * from sourced
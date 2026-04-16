-- seed -> raw \\ data as is

with sourced as (

    select * from {{ ref('country_codes') }}

)

select * from sourced
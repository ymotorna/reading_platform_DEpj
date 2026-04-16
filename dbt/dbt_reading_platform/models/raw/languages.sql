-- seed -> raw \\ data as is

with sourced as (

    select * from {{ ref('languages') }}

)

select * from sourced
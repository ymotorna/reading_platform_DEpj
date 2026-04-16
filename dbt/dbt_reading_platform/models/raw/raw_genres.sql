-- seed -> raw \\ data as is

with sourced as (

    select * from {{ ref('genres') }}

)

select * from sourced
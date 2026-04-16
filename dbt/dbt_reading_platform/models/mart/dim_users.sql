-- all stg_user cols

{{ config(
    materialized='table',
    tags=['daily']
) }}

select *
from {{ ref('stg_users') }}



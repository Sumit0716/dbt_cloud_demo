
{{ config(
    tags=["priorty_main"] ) }}

{{ config(
    materialized="table") }}

with source_data as (

    select 1 as id
    union all
    select 2 as id

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null

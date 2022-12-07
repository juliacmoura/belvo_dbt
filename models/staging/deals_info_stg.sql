with
    deals_info as (
        select 
        *
         from  {{ source('modeling_sources', 'deals_info') }}
    )

 select * from deals_info
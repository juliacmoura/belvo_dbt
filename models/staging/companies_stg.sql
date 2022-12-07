with companies as (select * from {{ source('raw_sources', 'companies') }})

select * from companies 
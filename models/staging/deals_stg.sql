with deals as (select * from {{ source('raw_sources', 'deals') }})

select * from deals
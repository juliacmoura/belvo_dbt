with owners as (select * from {{ source('raw_sources', 'owners') }})

select * from owners
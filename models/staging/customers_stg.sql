with customers as (select * from {{ source('raw_sources', 'customers') }})

select * from customers 
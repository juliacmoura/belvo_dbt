with contacts as (select * from {{ source('raw_sources', 'contacts') }})

select * from contacts 
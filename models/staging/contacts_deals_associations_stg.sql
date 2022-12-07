with contacts_deals_associations as (
        select * from {{ source('raw_sources', 'contacts_deals_associations') }}
    )

select * from contacts_deals_associations
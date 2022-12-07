with
    companies_deals_associations as (
        select * from {{ source('raw_sources', 'companies_deals_associations') }}
    )

select * from companies_deals_associations
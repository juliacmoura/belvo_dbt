--{{ config(materialized='table') }}

with
    companies_deals_associations as (
        select * from  {{ref ('companies_deals_associations_stg')}}
    ),

    contacts_deals_associations as (
        select * from  {{ref ('contacts_deals_associations_stg')}}
    ),

    companies as (select * from {{ref ('companies_stg')}}),

    contacts as (select * from {{ref ('contacts_stg')}}),

    base as (
        select
            contacts_deals_associations.contact_id,
            companies_deals_associations.company_id,
            companies_deals_associations.deal_ids,
            companies.name as company_name,
            companies.country
        from companies_deals_associations
        full join contacts_deals_associations using (deal_ids)
        left join companies on (companies_deals_associations.company_id = companies.id)
    )

select
    base.contact_id,
    base.company_id,
    base.deal_ids,
    base.company_name,
    contacts.name as contact_name,
    contacts.job,
    contacts.channel,
    base.country
from base
left join contacts on (base.contact_id = contacts.id)

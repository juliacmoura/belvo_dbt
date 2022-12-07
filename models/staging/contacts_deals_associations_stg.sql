with
    contacts_deals_associations as (
        select 
        regexp_replace(regexp_replace(deal_ids, '\\['),'\\]') as deal_ids,
        CONTACT_ID
         from  {{ source('raw_sources', 'contacts_deals_associations') }}
    ),

 NS AS (
      select 1 as n union all
      select 2 union all
      select 3 union all
      select 4 union all
      select 5 union all
      select 6 union all
      select 7 union all
      select 8 union all
      select 9 union all
      select 10
    )
    select
      TRIM(SPLIT_PART(contacts_deals_associations.deal_Ids, ',', NS.n)) AS deal_Ids,
      contacts_deals_associations.CONTACT_ID
    from NS inner join contacts_deals_associations ON (NS.n <= REGEXP_COUNT(contacts_deals_associations.deal_Ids, ',') + 1)
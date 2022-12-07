with
    companies_deals_associations as (
        select 
        regexp_replace(regexp_replace(deal_ids, '\\['),'\\]') as deal_ids,
        company_id
         from  {{ source('raw_sources', 'companies_deals_associations') }}
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
      TRIM(SPLIT_PART(companies_deals_associations.deal_Ids, ',', NS.n)) AS deal_Ids,
      companies_deals_associations.company_id
    from NS inner join companies_deals_associations ON (NS.n <= REGEXP_COUNT(companies_deals_associations.deal_Ids, ',') + 1)
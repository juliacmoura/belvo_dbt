with
    deals_info as (
        select 
        regexp_replace(regexp_replace(deal_ids, '\\['),'\\]') as deal_ids,
        company_name,
        contact_name,
        job,
        channel,
        country
         from  {{ source('modeling_sources', 'deals_info') }}
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
      TRIM(SPLIT_PART(deals_info.deal_Ids, ',', NS.n)) AS deal_Ids,
      deals_info.company_name, 
      deals_info.contact_name,
      deals_info.job,
      deals_info.channel,
      deals_info.country
    from NS inner join deals_info ON (NS.n <= REGEXP_COUNT(deals_info.deal_Ids, ',') + 1)
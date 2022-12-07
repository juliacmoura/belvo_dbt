with
    owners as (
        select 
        *
         from  {{ref ('owners_stg')}}
    ),
    deals as (
        select * from  {{ref ('deals_stg')}}
    )

    

    select 
    deals.id as deal_id,
    deals.EXTERNALID as external_id,
    deals.OWNERID as owner_id,
    deals.name as deal_name,
    deals.product as product,
    deals.amount as deal_amount,
    deals.closed as closed, 
    deals.status as deal_status,
    cast (deals.created_date as date) as created_date,
    cast (deals.closed_2 as date) as closed_date,
    owners.name as owner_name,
    owners.team as owner_team,
    owners.job_position as owner_job_position
    
     from deals left join owners on (deals.OWNERID = owners.id)

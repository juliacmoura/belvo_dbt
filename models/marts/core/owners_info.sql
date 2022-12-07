--{{ config(materialized='table') }}

with
    owners as (
        select * from  {{ref ('owners_stg')}}
    ),

    customers as (
        select * from  {{ref ('customers_stg')}}
    )

select 
customers.ID as customer_id,
customers.owner_id,
customers.customer_name,
customers.customer_phase,
cast(customers.start_date as date) as start_date,
customers.end_date as end_date,
owners.name as owner_name,
owners.team as owner_team, 
owners.job_position as owner_job_position
from customers left join owners on (customers.owner_id = owners.id) 

with owners as (
select * from {{ref ('owners_info')}}

)

select 
owner_job_position,
owner_team,
owner_name,
customer_phase,
count (distinct customer_id) as total_customers

from owners

group by 1,2,3,4
order by 5 desc

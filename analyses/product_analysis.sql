select 
closed,
product,
deal_status,
count (distinct deal_id) as total_deals, 
count (distinct owner_id) as total_owners,
sum (deal_amount) as total_amount,
round(sum (deal_amount) / count (distinct deal_id),0) as avg_amount, 
round(sum (deal_amount) / count (distinct owner_id),0) as amount_per_owner,
round(avg (CLOSED_DATE - CREATED_DATE), 0) as avg_days_close

from {{ref ('deals_owners')}}

--where closed = 'False'

group by 1,2,3
order by 1,2

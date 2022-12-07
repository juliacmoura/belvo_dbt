select 
closed,
count (distinct deal_id) as total_deals, 
count (distinct owner_id) as total_owners,
sum (deal_amount) as total_amount,
round(sum (deal_amount) / count (distinct deal_id),0) as avg_amount, 
round(count (distinct deal_id) / count (distinct owner_id),0) as deals_per_owners,
round(sum (deal_amount) / count (distinct owner_id),0) as amount_per_owner

from {{ref ('deals_owners')}}

group by 1

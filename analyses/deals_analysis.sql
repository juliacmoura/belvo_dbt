with info as (
select 
*
from {{ref ('deals_info')}}
),
deals_amt as (
select * 
from {{ref ('deals_owners')}}
)
select 
closed,
product,
channel,
count (distinct deal_id) as total_deals, 
count (distinct owner_id) as total_owners,
count (distinct contact_name) as total_contacts,
sum (deal_amount) as total_amount,
round(sum (deal_amount) / count (distinct deal_id),0) as avg_amount

from deals_amt left join info on (deals_amt.DEAL_ID = info.deal_ids) 

where closed = 'False'

group by 1,2,3
order by 1,2,4 desc

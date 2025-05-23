with final_balances as (select * from {{ ref("acct_start_end_balance") }})

select fin.*, acct.account_branch, acct.account_number, cust.first_name, cust.last_name, cust.country_name,
cit.city, st.state
from final_balances as fin
left join
    {{ source("staging_finance", "accounts") }} as acct
    on fin.account_id = acct.account_id
inner join 
{{ source("staging_finance", "dim_customers") }} as cust
on acct.customer_id = cust.customer_id
inner join {{ source("staging_finance", "dim_city") }} as cit
on cust.customer_city= cit.city_id
inner join {{source("staging_finance", "dim_state")}} as st
on cit.state_id = st.state_id

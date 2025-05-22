with all_trans as (select * from {{ ref("all_trans_monthly_balance") }})

select distinct d_year.action_year, d_month.action_month, all_trans.account_id
from {{ source("staging_finance", "d_time") }} as d_time
inner join
    {{ source("staging_finance", "d_year") }} as d_year
    on d_time.year_id = d_year.year_id
inner join
    {{ source("staging_finance", "d_month") }} as d_month
    on d_time.month_id = d_month.month_id
left join all_trans on d_year.action_year = all_trans.action_year  -- and d_month.action_month = all_trans.action_month

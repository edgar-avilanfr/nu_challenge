{{ config(
    materialized='table',
    clustered_by=["account_id", "action_year","action_month"]
) }}

with all_trans as (select * from {{ ref("all_trans_monthly_balance") }})

select date_range.action_year, date_range.action_month, all_trans.account_id
from
    (
        select distinct d_year.action_year, d_month.action_month
        from {{ source("staging_finance", "d_time") }} as d_time
        inner join
            {{ source("staging_finance", "d_year") }} as d_year
            on d_time.year_id = d_year.year_id
        inner join
            {{ source("staging_finance", "d_month") }} as d_month
            on d_time.month_id = d_month.month_id
    ) as date_range
left join (select distinct (account_id) from all_trans) as all_trans on 1 = 1
order by 1, 2, 3


{{ config(
    materialized='table',
    clustered_by=["account_id", "year_month"]
) }}

with
    nonpix_agg as (select * from {{ ref("nonpix_trans_combined") }}),
    pix_agg as (select * from {{ ref("pix_trans_monthly_agg") }})

select
    action_year,
    action_month,
    year_month,
    account_id,
    sum(total_ins) as total_ins,
    sum(total_outs) as total_outs,
    sum(monthly_balance) as monthly_balance
from
    (
        select *
        from nonpix_agg
        union all
        select *
        from pix_agg
    )
group by action_year, action_month, year_month, account_id

with
    agg_trans_ins as (select * from {{ ref("nonpix_trans_ins") }}),
    agg_trans_outs as (select * from {{ ref("nonpix_trans_outs") }})

select
    coalesce(ins.action_year, outs.action_year) as action_year,
    coalesce(ins.action_month, outs.action_month) as action_month,
    coalesce(ins.year_month, outs.year_month) as year_month,
    coalesce(ins.account_id, outs.account_id) as account_id,
    coalesce(ins.total_ins, 0) as total_ins,
    coalesce(outs.total_outs, 0) as total_outs,
    coalesce(ins.total_ins, 0) - coalesce(outs.total_outs, 0) as monthly_balance
from agg_trans_ins as ins
full outer join
    agg_trans_outs as outs
    on ins.year_month = outs.year_month
    and ins.account_id = outs.account_id

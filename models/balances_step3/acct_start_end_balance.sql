with
    all_trans as (select * from {{ ref("all_trans_monthly_balance") }}),
    acct_combo as (select * from {{ ref("acct_month_combo") }})

select
    acct_combo.action_year,
    acct_combo.action_month,
    acct_combo.account_id,
    cast(acct_combo.action_year as string)
    || lpad(cast(acct_combo.action_month as string), 2, '0') as year_month,
    coalesce(all_trans.total_ins, 0) as total_ins,
    coalesce(all_trans.total_outs, 0) as total_outs,
    coalesce(all_trans.monthly_balance, 0) as monthly_balance
from acct_combo
left join
    all_trans
    on acct_combo.action_year = all_trans.action_year
    and acct_combo.action_month = all_trans.action_month
    and acct_combo.account_id = all_trans.account_id
where acct_combo.account_id = 3159540850757342720

with
    all_trans as (select * from {{ ref("all_trans_monthly_balance") }}),
    acct_combo as (select * from {{ ref("acct_month_combo") }})

--select count(*)
--from acct_combo
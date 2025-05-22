with
    all_trans as (select * from {{ ref("all_trans_monthly_balance") }}),
    acct_combo as (select * from {{ ref("acct_month_combo") }})

select
    action_year,
    action_month,
    year_month,
    account_id,
    coalesce(
        sum(monthly_balance) over (
            partition by account_id
            order by action_year, action_month asc
            rows between unbounded preceding and 1 preceding
        ),
        0
    ) as start_balance,
    total_ins,
    total_outs,
    monthly_balance,
    coalesce(
        sum(monthly_balance) over (
            partition by account_id
            order by action_year, action_month asc
            rows between unbounded preceding and current row
        ),
        0
    ) as end_balance,
from
    (
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
    )
--where account_id in (2120040664426333696, 2680031936548642304, 1371735070453628416 ,2377112943367431680,451840458667849728)

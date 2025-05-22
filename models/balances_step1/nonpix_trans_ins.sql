select
    d_year.action_year,
    d_month.action_month,
    cast(d_year.action_year as string)
    || lpad(cast(d_month.action_month as string), 2, '0') as year_month,
    ins.account_id,
    sum(ins.amount) as total_ins
from {{ source("staging_finance", "transfer_ins") }} as ins
left join
    {{ source("staging_finance", "d_time") }} as d_time
    on safe_cast(ins.transaction_completed_at as int) = d_time.time_id
inner join
    {{ source("staging_finance", "d_year") }} as d_year
    on d_time.year_id = d_year.year_id
inner join
    {{ source("staging_finance", "d_month") }} as d_month
    on d_time.month_id = d_month.month_id
where ins.status = 'completed'
group by d_year.action_year, d_month.action_month, account_id
order by 1, 2 asc

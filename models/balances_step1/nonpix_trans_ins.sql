select 
d_year.action_year,
d_month.action_month, 
CAST(d_year.action_year as string)||LPAD(CAST(d_month.action_month AS STRING), 2, '0') as year_month,
ins.account_id,
sum(ins.amount) as total_ins
from {{source('staging_finance','transfer_ins')}} as ins
left join {{source('staging_finance','d_time')}} as d_time 
on SAFE_CAST(ins.transaction_completed_at AS INT) = d_time.time_id
inner join {{source('staging_finance','d_year')}} as d_year
on d_time.year_id= d_year.year_id
inner join {{source('staging_finance','d_month')}} as d_month
on d_time.month_id= d_month.month_id
where ins.status = 'completed'
group by d_year.action_year,
d_month.action_month, 
account_id
order by 1,2 asc
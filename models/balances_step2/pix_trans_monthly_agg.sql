select *, total_ins - total_outs as monthly_balance
from
    (
        select
            d_year.action_year,
            d_month.action_month,
            cast(d_year.action_year as string)
            || lpad(cast(d_month.action_month as string), 2, '0') as year_month,
            pix.account_id,
            sum(
                case when pix.in_or_out = 'pix_in' then pix_amount else 0 end
            ) as total_ins,
            sum(
                case when pix.in_or_out = 'pix_out' then pix_amount else 0 end
            ) as total_outs
        from {{ source("staging_finance", "pix_movements") }} as pix
        left join
            {{ source("staging_finance", "d_time") }} as d_time
            on safe_cast(pix.pix_completed_at as int) = d_time.time_id
        inner join
            {{ source("staging_finance", "d_year") }} as d_year
            on d_time.year_id = d_year.year_id
        inner join
            {{ source("staging_finance", "d_month") }} as d_month
            on d_time.month_id = d_month.month_id
        where pix.status = 'completed'
        group by d_year.action_year, d_month.action_month, account_id
    )

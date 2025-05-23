with final_balances as (select * from {{ ref("acct_start_end_balance") }})

select fin.*, acct.account_branch, acct.account_number
from final_balances as fin
left join
    {{ source("staging_finance", "accounts") }} as acct
    on fin.account_id = acct.account_id

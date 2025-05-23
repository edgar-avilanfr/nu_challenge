# NU challenge

This is my solution to the challenge.
I decided to take a modular approach to solving the problem since a lot of transformations and aggregations had to be made before moving to calculating starting and ending balances per account, as per the modeling tool I used dbt since you have several benefits in using it for complex solutions:

1.- Loaded my tables into BigQuery staging dataset

2.- Initialized a dbt project

3.- Calculated aggregations for both, non-pix transactions ins and outs, and then merged them together into the model “nonpix_trans_combined” combining these into a single table containing ins and outs per month per account, along with the monthly balance.

4.- Did the same with pix transactions, I specified the same format as previous table, then combined pix and non-pix transactions into “all_trans_monthly_balance”

5.- Since not all accounts had data in all 12 months, I created a template in model “acct_month_combo” which includes the combination of accounts and months in year 2020 regardless of having transactions in each month or not.

6.- Using the above template, I joined to “all_trans_monthly_balance” and created the final model “acct_start_end_balance” where I performed the sum over previous monthly balances to get starting and ending balances per month per account.

7.- Finally I created data mart “mart_monthly_balances”, where I joined previous result to the accounts, customer and city tables to the analyst to make more types of analysis and find more insights.

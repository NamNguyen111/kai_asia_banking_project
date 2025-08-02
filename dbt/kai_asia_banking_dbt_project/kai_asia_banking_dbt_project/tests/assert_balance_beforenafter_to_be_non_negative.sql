-- tests/test_negative_balances.sql
select
    entry_id,
    balance_before,
    balance_after
from {{ ref('stg_transaction_entries') }}
where balance_before < 0 
   or balance_after < 0
   or debit_amount < 0
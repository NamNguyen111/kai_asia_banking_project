select account_id, balance
from {{ref('stg_accounts')}}
where balance < 0
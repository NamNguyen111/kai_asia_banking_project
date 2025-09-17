-- Giới hạn 90 ngày gần nhất và chỉ lấy tài khoản đủ 90 ngày tuổi

{{ 
    config(
        materialized='table',
        on_schema_change='fail',
        schema=var("custom_schema", "intermediate")
    ) 
}}

-- Tìm giao dịch cuối mỗi ngày, nhóm theo account_id và date(created_at)
with daily_tx as (
    select
        account_id,
        date(created_at) as tx_date,
        max(created_at) as last_tx_time,
        count(*) as daily_tx_count
    from {{ ref('stg_transaction_entries') }}
    where created_at >= current_date - interval '90 days' AND account_id != 'BANK_CASH'
    group by account_id, date(created_at)
),

balances as (
    select
        d.account_id,
        d.tx_date,
        te.balance_after as end_of_day_balance,
        te.entry_id,
        row_number() over ( -- Row number nhỡ có 2 giao dịch cùng thời điểm
            partition by d.account_id, d.tx_date 
            order by te.entry_id desc
        ) as rn
    from daily_tx d
    join {{ ref('stg_transaction_entries') }} te  
      on te.account_id = d.account_id
     and te.created_at = d.last_tx_time
), 

calendar as (
    select generate_series(
        current_date - interval '90 days',
        current_date,
        interval '1 day'
    )::date as cal_date
), 

-- Lọc chỉ lấy tài khoản đủ 90 ngày tuổi
eligible_accounts as (
    select 
        account_id,
        customer_id,
        account_type,
        created_at
    from {{ ref('stg_accounts') }}
    where status = 'ACTIVE'
      and account_type = 'CHECKING'
      and created_at <= current_date - interval '90 days'  -- Chỉ lấy tài khoản >= 90 ngày tuổi
),

expanded as (
    select
        ea.account_id,
        c.cal_date,
        b.end_of_day_balance
    from eligible_accounts ea  -- Sử dụng eligible_accounts thay vì stg_accounts
    cross join calendar c
    left join balances b
      on ea.account_id = b.account_id
     and c.cal_date = b.tx_date
),

-- Lấy số dư cuối trước kỳ 90 ngày để làm base (chỉ cho eligible accounts)
base_balance as (
    select
        ea.account_id,
        coalesce(
            -- Ưu tiên: Lấy số dư từ giao dịch gần nhất trước 90 ngày
            te_before.balance_after,
            -- Fallback: Lấy balance_before từ giao dịch đầu tiên của account
            te_first.balance_before,
            -- Fallback cuối: Lấy từ current balance trong accounts table
            a.balance
        ) as starting_balance
    from eligible_accounts ea
    left join (
        -- Số dư từ giao dịch gần nhất trước 90 ngày
        select 
            account_id,
            balance_after,
            row_number() over (partition by account_id order by created_at desc) as rn
        from {{ ref('stg_transaction_entries') }}
        where created_at < current_date - interval '90 days'
    ) te_before on ea.account_id = te_before.account_id and te_before.rn = 1
    left join (
        -- balance_before từ giao dịch đầu tiên (chính là initial balance)
        select 
            account_id,
            balance_before,
            row_number() over (partition by account_id order by created_at asc) as rn
        from {{ ref('stg_transaction_entries') }}
    ) te_first on ea.account_id = te_first.account_id and te_first.rn = 1
    left join {{ ref('stg_accounts') }} a on ea.account_id = a.account_id
),

-- Tìm ngày giao dịch đầu tiên của mỗi account
first_transaction_date as (
    select 
        account_id,
        min(date(created_at)) as first_tx_date
    from {{ ref('stg_transaction_entries') }}
    group by account_id
),

-- Lấy initial balance, handle cả trường hợp chưa có giao dịch nào
account_initial_balance as (
    select 
        ea.account_id,
        coalesce(
            first_tx.balance_before,  -- Ưu tiên từ balance_before của giao dịch đầu tiên
            a.balance,                -- Fallback: balance hiện tại nếu chưa có giao dịch
            0                         -- Fallback cuối: 0
        ) as initial_balance
    from eligible_accounts ea
    left join (
        select 
            account_id,
            balance_before,
            row_number() over (partition by account_id order by created_at asc) as rn
        from {{ ref('stg_transaction_entries') }}
    ) first_tx on ea.account_id = first_tx.account_id and first_tx.rn = 1
    left join {{ ref('stg_accounts') }} a on ea.account_id = a.account_id
),

-- Sửa lại CTE filled với logic đầy đủ
filled as (
    select
        e.account_id,
        e.cal_date as data_date,
        case 
            -- Nếu ngày này trước ngày có giao dịch đầu tiên (hoặc chưa có giao dịch nào)
            when e.cal_date < coalesce(ftd.first_tx_date, '9999-12-31'::date) then 
                coalesce(aib.initial_balance, 0)
            -- Nếu ngày này từ ngày có giao dịch đầu tiên trở đi
            else 
                coalesce(
                    e.end_of_day_balance,
                    max(e.end_of_day_balance) over (
                        partition by e.account_id 
                        order by e.cal_date 
                        rows between unbounded preceding and current row
                    ),
                    bb.starting_balance,
                    coalesce(aib.initial_balance, 0)
                )
        end as balance
    from expanded e
    left join base_balance bb on e.account_id = bb.account_id
    left join first_transaction_date ftd on e.account_id = ftd.account_id
    left join account_initial_balance aib on e.account_id = aib.account_id
)

select
    f.account_id,
    ea.customer_id,
    ea.account_type,
    f.data_date,
    f.balance
from filled f
join eligible_accounts ea on f.account_id = ea.account_id
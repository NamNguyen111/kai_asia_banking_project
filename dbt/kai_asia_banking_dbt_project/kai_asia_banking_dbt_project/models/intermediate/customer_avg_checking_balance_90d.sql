-- models/marts/customer_avg_checking_balance_90d.sql
-- Số dư checking bình quân 90 ngày cho mỗi khách hàng

{{ 
    config(
        materialized='table'
    ) 
}}

with daily_customer_total as (
    select
        customer_id,
        data_date,
        sum(balance) as total_checking_balance
    from {{ ref('checking_accounts_scd_90d') }}
    group by customer_id, data_date
)

select
    customer_id,
    cast(avg(total_checking_balance) as bigint) as avg_checking_balance_90d
from daily_customer_total
group by customer_id
{{
    config(
        materialized='table',
        on_schema_change='fail',
        schema = var("custom_schema", "intermediate")
    )
}}

with savings as (
    select
        cast(customer_id as varchar(20)) as customer_id,
        cast(sum(balance) as bigint) as savings_total
    from {{ ref('stg_accounts') }}
    where created_at <= current_date - interval '90 days'
      and account_type = 'SAVINGS'
    group by customer_id
),
term_deposits as (
    select
        cast(customer_id as varchar(20)) as customer_id,
        cast(sum(principal) as bigint) as term_total
    from {{ ref('stg_term_deposit_holdings') }}
    where created_at <= current_date - interval '90 days'
    group by customer_id
),
certificates as (
    select
        cast(customer_id as varchar(20)) as customer_id,
        cast(sum(amount) as bigint) as cert_total
    from {{ ref('stg_certificate_of_deposit_holdings') }}
    where created_at <= current_date - interval '90 days'
    group by customer_id
)

select
    cast(coalesce(s.customer_id, t.customer_id, c.customer_id) as varchar(20)) as customer_id,
    cast(
        coalesce(s.savings_total,0) 
        + coalesce(t.term_total,0) 
        + coalesce(c.cert_total,0) 
        as bigint
    ) as total_assets_90d
from savings s
full outer join term_deposits t using (customer_id)
full outer join certificates c using (customer_id)

{% snapshot snp_customers %}

{{
    config(
        target_schema=var("custom_schema", "snapshots"),
        target_database='db_banking',
        unique_key='customer_id',
        strategy='check',
        check_cols=['income_range', 'occupation', 'phone_number', 'email_address', 'address', 'date_of_birth', 'status', 'customer_segment'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date"
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
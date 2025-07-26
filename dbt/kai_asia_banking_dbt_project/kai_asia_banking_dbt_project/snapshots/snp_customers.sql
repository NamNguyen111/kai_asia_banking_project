{% snapshot snp_customers %}

{{
    config(
        target_schema='snapshots',
        target_database='db_banking',
        unique_key='customer_id',
        strategy='check',
        check_cols=['full_name', 'phone_number', 'email_address', 'address', 'date_of_birth'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date"
    )
}}

select * from {{ source('staging', 'stg_customers') }}

{% endsnapshot %}
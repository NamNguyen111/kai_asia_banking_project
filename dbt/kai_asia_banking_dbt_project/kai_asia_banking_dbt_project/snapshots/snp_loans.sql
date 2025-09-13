{% snapshot snp_loans %}
{{
    config(
        target_schema=var("custom_schema", "snapshots"),
        target_database='db_banking',
        unique_key='loan_id',
        strategy='check',
        check_cols=['remaining_balance','status'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date"
    )
}}
select * from {{ ref('stg_loans') }}
{% endsnapshot %}
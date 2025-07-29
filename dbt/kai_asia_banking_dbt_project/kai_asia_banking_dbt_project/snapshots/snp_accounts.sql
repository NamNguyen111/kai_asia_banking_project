{% snapshot snp_accounts %}
{{
    config(
        target_schema=var("custom_schema", "snapshots"),
        target_database='db_banking',
        unique_key='account_id',
        strategy='check',
        check_cols=['balance','status'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date",
        post_hook="
            UPDATE {{ this }}
            SET status = 'DISABLE'
            WHERE dbt_valid_to != '9999-12-31'::date
              AND status = 'ACTIVE';
        "
    )
}}
select * from {{ source('staging','stg_accounts') }}
{% endsnapshot %}
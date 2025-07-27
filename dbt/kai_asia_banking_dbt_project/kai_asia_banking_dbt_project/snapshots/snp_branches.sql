{% snapshot snp_branches %}

{{
    config(
        target_schema='snapshots',
        target_database='db_banking',
        unique_key='branch_id',
        strategy='check',
        check_cols=['branch_name', 'address'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date",
        pre_hook="COMMIT;",
        post_hook="
            UPDATE {{ this }}
            SET status = 'DISABLE'
            WHERE dbt_valid_to != '9999-12-31'::date
              AND status = 'ACTIVE';
            COMMIT;
        "
    )
}}

select * from {{ source('staging', 'stg_branches') }}

{% endsnapshot %}
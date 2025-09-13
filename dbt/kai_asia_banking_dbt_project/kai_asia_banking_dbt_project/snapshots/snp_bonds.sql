{% snapshot snp_bonds %}
{{
    config(
        target_schema=var("custom_schema", "snapshots"),
        target_database='db_banking',
        unique_key='bond_id',
        strategy='check',
        check_cols=['bond_name','issuer', 'face_value', 'interest_rate'],
        hard_deletes='ignore',
        dbt_valid_to_current="'9999-12-31'::date"
    )
}}
select * from {{ ref('stg_bonds') }}
{% endsnapshot %}
{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = 'account_id',
        on_schema_change = 'fail'
    )
}}

WITH source_data as (
    SELECT account_id,
            customer_id,
            account_number,
            account_type,
            balance,
            status,
            branch_id,
            created_at
    FROM {{ source('raw', 'accounts') }}
),

cleaned AS (
    SELECT account_id,
            customer_id,
            TRIM(account_number) as account_number,
            UPPER(TRIM(account_type)) as account_type,
            CAST(balance AS NUMERIC) AS balance,
            UPPER(TRIM(status)) as status,
            branch_id,
            DATE(created_at) as data_date,
            created_at,
            CURRENT_TIMESTAMP as etl_at,
            'raw_accounts' as etl_source_model

    FROM source_data

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
        AND account_id is NOT NULL
    {% else %}
        WHERE account_id is NOT NULL
    {% endif %}
)

SELECT * from cleaned
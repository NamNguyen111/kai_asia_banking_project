{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = 'transaction_id',
        on_schema_change = 'fail'
    )
}}

WITH source_data as (
    SELECT transaction_id,
            reference_number,
            from_account_id,
            to_account_id,
            amount,
            transaction_type,
            channel,
            description,
            created_at
    FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT transaction_id,
            TRIM(reference_number) as reference_number,
            from_account_id,
            to_account_id,
            CAST(amount AS NUMERIC) AS amount,
            UPPER(TRIM(transaction_type)) as transaction_type,
            UPPER(TRIM(channel)) as transaction_channel,
            TRIM(description) as transaction_description,
            DATE(created_at) as data_date,
            created_at,
            CURRENT_TIMESTAMP as etl_at,
            'raw_transactions' as etl_source_model

    FROM source_data

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
        AND transaction_id is NOT NULL
    {% else %}
        WHERE transaction_id is NOT NULL
    {% endif %}
)

SELECT * from cleaned
{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = 'entry_id',
        on_schema_change = 'fail'
    )
}}

WITH source_data as (
    SELECT entry_id,
            transaction_id,
            account_id,
            debit_amount,
            credit_amount,
            balance_before,
            balance_after,
            entry_type,
            entry_sequence,
            description,
            created_at
    FROM {{ source('raw', 'transaction_entries') }}
),

cleaned AS (
    SELECT entry_id,
            transaction_id,
            account_id,
            CAST(debit_amount as NUMERIC) AS debit_amount,
            CAST(credit_amount as NUMERIC) AS credit_amount,
            CAST(balance_before as NUMERIC) AS balance_before,
            CAST(balance_after as NUMERIC) AS balance_after,
            TRIM(UPPER(entry_type)) AS entry_type,
            entry_sequence,
            TRIM(description) AS entry_description,
            DATE(created_at) AS data_date,
            created_at,
            CURRENT_TIMESTAMP AS etl_at,
            'stg_transaction_entries' AS etl_source_model

    FROM source_data

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
        AND entry_id is NOT NULL
    {% else %}
        WHERE entry_id is NOT NULL
    {% endif %}
)

SELECT * from cleaned
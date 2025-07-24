{{
    config(
        materialized = 'incremental',
        schema = 'staging',
        unique_key = 'entry_id',
        on_schema_change = 'fail'
    )
}}
WITH source_data AS (
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
    WHERE entry_id IS NOT NULL
    {% if is_incremental() %}
        AND created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
),
cleaned AS (
    SELECT entry_id,
           transaction_id,
           account_id,
           CAST(debit_amount AS NUMERIC(15,2)) AS debit_amount,
           CAST(credit_amount AS NUMERIC(15,2)) AS credit_amount,
           CAST(balance_before AS NUMERIC(15,2)) AS balance_before,
           CAST(balance_after AS NUMERIC(15,2)) AS balance_after,
           TRIM(UPPER(entry_type)) AS entry_type,
           entry_sequence,
           TRIM(description) AS entry_description,
           DATE(created_at) AS data_date,
           created_at,
           CURRENT_TIMESTAMP AS etl_at,
           'stg_transaction_entries' AS etl_source_model
    FROM source_data
    WHERE entry_sequence IN (1, 2)  -- Chỉ chấp nhận entry_sequence = 1 hoặc 2
),
validated AS (
    SELECT *
    FROM cleaned
    WHERE 
        -- Basic validations
        transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        -- Amount validations
        AND (debit_amount >= 0 OR debit_amount IS NULL)
        AND (credit_amount >= 0 OR credit_amount IS NULL)
        -- Business rule: không thể có cả debit và credit cùng lúc
        AND NOT (debit_amount > 0 AND credit_amount > 0)
        -- Business rule: phải có ít nhất một trong hai
        AND (debit_amount > 0 OR credit_amount > 0)
)
SELECT * FROM validated
{{
    config(
        materialized='incremental',
        unique_key='entry_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        entry_id,
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

    {% if is_incremental() %}
        -- chỉ lấy các entry_id chưa tồn tại trong staging
        WHERE entry_id NOT IN (SELECT entry_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        entry_id,                                -- UUID, giữ nguyên
        transaction_id,                          -- UUID, giữ nguyên
        UPPER(TRIM(account_id))       AS account_id,
        CAST(debit_amount AS NUMERIC) AS debit_amount,
        CAST(credit_amount AS NUMERIC)AS credit_amount,
        CAST(balance_before AS NUMERIC)AS balance_before,
        CAST(balance_after AS NUMERIC) AS balance_after,
        UPPER(TRIM(entry_type))       AS entry_type,        -- chuẩn hóa DEBIT / CREDIT
        CAST(entry_sequence AS INT)   AS entry_sequence,
        TRIM(description)             AS description,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CURRENT_TIMESTAMP             AS etl_at,
        'raw.transaction_entries'     AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        transaction_id,
        reference_number,
        from_account_id,
        to_account_id,
        amount,
        transaction_type,
        channel,
        description,
        created_at
    FROM {{ source('raw', 'transactions') }}

    {% if is_incremental() %}
        -- chỉ lấy những transaction_id chưa tồn tại trong bảng staging
        WHERE transaction_id NOT IN (SELECT transaction_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        transaction_id,                           -- giữ nguyên UUID
        TRIM(reference_number)          AS reference_number,
        UPPER(TRIM(from_account_id))    AS from_account_id,
        UPPER(TRIM(to_account_id))      AS to_account_id,
        CAST(amount AS NUMERIC)         AS amount,
        UPPER(TRIM(transaction_type))   AS transaction_type,
        UPPER(TRIM(channel))            AS transaction_channel,
        TRIM(description)               AS transaction_description,
        CAST(created_at AS TIMESTAMP)   AS created_at,
        DATE(created_at)                AS data_date,
        CURRENT_TIMESTAMP               AS etl_at,
        'raw.transactions'              AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

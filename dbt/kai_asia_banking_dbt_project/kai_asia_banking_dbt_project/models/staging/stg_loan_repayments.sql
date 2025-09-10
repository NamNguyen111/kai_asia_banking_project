{{
    config(
        materialized='incremental',
        unique_key='repayment_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        repayment_id,
        loan_id,
        due_date,
        repayment_date,
        principal_paid,
        interest_paid,
        late_fee_paid,
        remaining_balance,
        status,
        payment_method,
        created_at
    FROM {{ source('raw', 'loan_repayments') }}

    {% if is_incremental() %}
        -- chỉ lấy các repayment_id chưa tồn tại trong staging
        WHERE repayment_id NOT IN (SELECT repayment_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        repayment_id,                                  -- SERIAL, giữ nguyên
        loan_id,                                       -- UUID, giữ nguyên
        CAST(due_date AS DATE)             AS due_date,
        CAST(repayment_date AS DATE)       AS repayment_date,
        CAST(principal_paid AS NUMERIC)    AS principal_paid,
        CAST(interest_paid AS NUMERIC)     AS interest_paid,
        CAST(late_fee_paid AS NUMERIC)     AS late_fee_paid,
        CAST(remaining_balance AS NUMERIC) AS remaining_balance,
        UPPER(TRIM(status))                AS status,         -- ON_TIME, LATE
        UPPER(TRIM(payment_method))        AS payment_method, -- CHUYỂN KHOẢN, TIỀN MẶT...
        CAST(created_at AS TIMESTAMP)      AS created_at,
        DATE(created_at)                   AS data_date,
        CURRENT_TIMESTAMP                  AS etl_at,
        'raw.loan_repayments'              AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

{{
    config(
        materialized='incremental',
        unique_key='term_deposit_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        term_deposit_id,
        customer_id,
        principal,
        interest_rate,
        interest_payment_interval,
        start_date,
        maturity_date,
        interest_calculation_method,
        status,
        created_at
    FROM {{ source('raw', 'term_deposit_holdings') }}

    {% if is_incremental() %}
        -- chỉ lấy các term_deposit_id chưa tồn tại trong staging
        WHERE term_deposit_id NOT IN (SELECT term_deposit_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        term_deposit_id,                               -- SERIAL, giữ nguyên
        UPPER(TRIM(customer_id))         AS customer_id,
        CAST(principal AS NUMERIC)       AS principal,
        CAST(interest_rate AS NUMERIC(5,2)) AS interest_rate,
        CAST(interest_payment_interval AS INT) AS interest_payment_interval,
        CAST(start_date AS DATE)         AS start_date,
        CAST(maturity_date AS DATE)      AS maturity_date,
        UPPER(TRIM(interest_calculation_method)) AS interest_calculation_method, -- SIMPLE, COMPOUND
        UPPER(TRIM(status))              AS status,        -- ACTIVE, CLOSED, MATURED
        CAST(created_at AS TIMESTAMP)    AS created_at,
        DATE(created_at)                 AS data_date,
        CURRENT_TIMESTAMP                AS etl_at,
        'raw.term_deposit_holdings'      AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

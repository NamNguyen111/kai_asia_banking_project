{{
    config(
        materialized='view',
        unique_key='cd_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        cd_id,
        customer_id,
        amount,
        interest_rate,
        start_date,
        maturity_date,
        term_length,
        interest_calculation_method,
        status,
        created_at
    FROM {{ source('raw', 'certificate_of_deposit_holdings') }}

),

cleaned AS (

    SELECT
        cd_id,                                         -- SERIAL, giữ nguyên
        UPPER(TRIM(customer_id))           AS customer_id,
        CAST(amount AS NUMERIC)            AS amount,
        CAST(interest_rate AS NUMERIC(5,2)) AS interest_rate,
        CAST(start_date AS DATE)           AS start_date,
        CAST(maturity_date AS DATE)        AS maturity_date,
        CAST(term_length AS INT)           AS term_length,
        UPPER(TRIM(interest_calculation_method)) AS interest_calculation_method, -- SIMPLE, COMPOUND
        UPPER(TRIM(status))                AS status,       -- ACTIVE, REDEEMED, MATURED
        CAST(created_at AS TIMESTAMP)      AS created_at,
        CURRENT_TIMESTAMP                  AS etl_at,
        'raw.certificate_of_deposit_holdings' AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned;

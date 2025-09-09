{{
    config(
        materialized='view',
        unique_key='loan_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        loan_id,
        customer_id,
        loan_type,
        loan_amount,
        interest_rate,
        term_length,
        start_date,
        maturity_date,
        repayment_method,
        penalty_rate,
        collateral,
        status,
        remaining_balance,
        created_at
    FROM {{ source('raw', 'loans') }}

),

cleaned AS (

    SELECT
        loan_id,                                      -- UUID, giữ nguyên
        UPPER(TRIM(customer_id))          AS customer_id,
        UPPER(TRIM(loan_type))            AS loan_type,            -- TÍN CHẤP, THẾ CHẤP...
        CAST(loan_amount AS NUMERIC)      AS loan_amount,
        CAST(interest_rate AS NUMERIC(5,2)) AS interest_rate,
        CAST(term_length AS INT)          AS term_length,
        CAST(start_date AS DATE)          AS start_date,
        CAST(maturity_date AS DATE)       AS maturity_date,
        UPPER(TRIM(repayment_method))     AS repayment_method,     -- EMI, INTEREST_ONLY
        CAST(penalty_rate AS NUMERIC(5,2)) AS penalty_rate,
        TRIM(collateral)                  AS collateral,
        UPPER(TRIM(status))               AS status,               -- ONGOING, CLOSED, DEFAULTED
        CAST(remaining_balance AS NUMERIC) AS remaining_balance,
        CAST(created_at AS TIMESTAMP)     AS created_at,
        CURRENT_TIMESTAMP                 AS etl_at,
        'raw.loans'                       AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned;

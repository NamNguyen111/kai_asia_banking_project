{{ 
    config(
        materialized='view',
        unique_key='account_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    ) 
}}

WITH source_data AS (

    SELECT
        account_id,
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

    SELECT
        UPPER(TRIM(account_id))          AS account_id,       
        UPPER(TRIM(customer_id))         AS customer_id,      -- FK -> customers
        TRIM(account_number)             AS account_number,
        UPPER(TRIM(account_type))        AS account_type,     -- chuẩn hóa SAVINGS / CHECKING
        CAST(balance AS NUMERIC)         AS balance,
        UPPER(TRIM(status))              AS status,
        UPPER(TRIM(branch_id))           AS branch_id,        -- FK -> branches
        CAST(created_at AS TIMESTAMP)    AS created_at,
        CURRENT_TIMESTAMP                AS etl_at,
        'raw.accounts'                   AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

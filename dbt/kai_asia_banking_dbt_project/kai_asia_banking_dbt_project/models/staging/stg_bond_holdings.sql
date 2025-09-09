{{
    config(
        materialized='incremental',
        unique_key='bond_transaction_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        bond_transaction_id,
        bond_code,
        customer_id,
        buy_quantity,
        status,
        created_at
    FROM {{ source('raw', 'bond_holdings') }}

    {% if is_incremental() %}
        -- chỉ lấy các bond_transaction_id chưa tồn tại trong staging
        WHERE bond_transaction_id NOT IN (SELECT bond_transaction_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        bond_transaction_id,                          -- UUID giữ nguyên
        UPPER(TRIM(bond_code))        AS bond_code,
        UPPER(TRIM(customer_id))      AS customer_id,
        CAST(buy_quantity AS INT)     AS buy_quantity,
        UPPER(TRIM(status))           AS status,         -- normalize ACTIVE, MATURED, REDEEMED
        CAST(created_at AS TIMESTAMP) AS created_at,
        CURRENT_TIMESTAMP             AS etl_at,
        'raw.bond_holdings'           AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

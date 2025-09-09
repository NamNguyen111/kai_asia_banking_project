{{
    config(
        materialized='incremental',
        unique_key='fund_cert_holding_id',
        on_schema_change='fail',
        schema=var("custom_schema", "staging")
    )
}}

WITH source_data AS (

    SELECT
        fund_cert_holding_id,
        customer_id,
        fund_name,
        units,
        nav_price,
        purchase_date,
        status,
        created_at
    FROM {{ source('raw', 'fund_certificate_holdings') }}

    {% if is_incremental() %}
        -- chỉ lấy các fund_cert_holding_id chưa tồn tại trong staging
        WHERE fund_cert_holding_id NOT IN (SELECT fund_cert_holding_id FROM {{ this }})
    {% endif %}
),

cleaned AS (

    SELECT
        fund_cert_holding_id,                         -- UUID giữ nguyên
        UPPER(TRIM(customer_id))        AS customer_id,
        TRIM(fund_name)                 AS fund_name,
        CAST(units AS INT)              AS units,
        CAST(nav_price AS NUMERIC)      AS nav_price,
        CAST(purchase_date AS DATE)     AS purchase_date,
        UPPER(TRIM(status))             AS status,        -- normalize ACTIVE / REDEEMED / EXPIRED
        CAST(created_at AS TIMESTAMP)   AS created_at,
        CURRENT_TIMESTAMP               AS etl_at,
        'raw.fund_certificate_holdings' AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

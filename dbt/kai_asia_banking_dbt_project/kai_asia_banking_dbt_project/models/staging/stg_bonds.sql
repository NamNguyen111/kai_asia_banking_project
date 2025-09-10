{{ 
    config(
        materialized='view',
        unique_key='bond_code',
        on_schema_change = 'fail',
        schema=var("custom_schema", "staging")
    ) 
}}

WITH source_data as (

    SELECT
        bond_code,
        bond_name,
        bond_type,
        coupon_frequency,
        issuer,
        cast(face_value as numeric)       as face_value,
        cast(interest_rate as numeric(5,2)) as interest_rate,
        issue_date,
        maturity_date
    FROM {{ source('raw', 'bonds') }}

),

cleaned AS (

    SELECT
        UPPER(TRIM(bond_code))              as bond_id,         -- surrogate key
        TRIM(bond_name) as bond_name,
        TRIM(bond_type) as bond_type,
        TRIM(coupon_frequency) as coupon_frequency,
        TRIM(issuer) as issuer,
        face_value,
        interest_rate,
        issue_date::DATE as issue_date,
        CAST(maturity_date as DATE) as maturity_date,
        CURRENT_TIMESTAMP as etl_at,
        'raw.bonds' as etl_source_model
    FROM source_data
)

SELECT * from cleaned

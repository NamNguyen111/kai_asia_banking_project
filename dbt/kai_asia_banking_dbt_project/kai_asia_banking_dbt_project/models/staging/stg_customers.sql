{{ 
    config(
        materialized='view',
        unique_key='customer_id',
        on_schema_change = 'fail',
        schema=var("custom_schema", "staging")
    ) 
}}

WITH source_data AS (

    SELECT
        customer_id,
        full_name,
        gender,
        income_range,
        occupation,
        phone,
        email,
        id_number,
        address,
        date_of_birth,
        status,
        customer_segment,
        customer_since,
        created_at
    FROM {{ source('raw', 'customers') }}

),

cleaned AS (

    SELECT
        UPPER(TRIM(customer_id))       AS customer_id,         
        INITCAP(TRIM(full_name))       AS full_name,
        gender,                                                
        CAST(income_range AS NUMERIC)  AS income_range,
        TRIM(occupation)               AS occupation,
        TRIM(phone)                    AS phone_number,
        TRIM(email)                    AS email_address,
        UPPER(TRIM(id_number))         AS identity_number,
        TRIM(address)                  AS address,
        CAST(date_of_birth AS DATE)    AS date_of_birth,
        UPPER(TRIM(status))            AS status,
        UPPER(TRIM(customer_segment))  AS customer_segment,
        CAST(customer_since AS DATE)   AS customer_since,
        CAST(created_at AS TIMESTAMP)  AS created_at,
        DATE(created_at)               AS data_date,
        CURRENT_TIMESTAMP              AS etl_at,
        'raw.customers'                AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

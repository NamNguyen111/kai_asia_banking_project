{{
    config(
        materialized='incremental',
        schema = 'staging',
        unique_key='customer_id',
        on_schema_change='fail',
    )
}}

WITH source_data as (
    SELECT  
        customer_id,
        full_name,
        phone,
        email,
        id_number,
        address,
        date_of_birth,
        status,
        created_at,
        updated_at
    FROM {{ source('raw','customers') }}
),

cleaned AS (
    SELECT
        customer_id,

        TRIM(full_name) as full_name,
        phone as phone_number,
        TRIM(email) as email_address,
        TRIM(id_number) as identity_number,
        TRIM(address) as address,
        date_of_birth as date_of_birth,
        UPPER(TRIM(status)) as status,
        DATE(created_at) as data_date,

        created_at,
        updated_at,

        CURRENT_TIMESTAMP as etl_at,
        'stg_customers' as etl_source_model
    
    FROM source_data

    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{this}})
        AND customer_id is NOT NULL
    {% else %}
        WHERE customer_id is not NULL
    {% endif %}
)

SELECT * from cleaned


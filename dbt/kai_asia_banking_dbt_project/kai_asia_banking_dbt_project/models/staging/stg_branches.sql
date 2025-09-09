{{ 
    config(
        materialized='view',
        unique_key='branch_id',
        on_schema_change = 'fail',
        schema=var("custom_schema", "staging")
    ) 
}}

WITH source_data AS (

    SELECT
        branch_id,
        branch_name,
        address,
        status,
        created_at
    FROM {{ source('raw', 'branches') }}

),

cleaned AS (

    SELECT
        UPPER(TRIM(branch_id))     AS branch_id,        
        TRIM(branch_name)          AS branch_name,
        TRIM(address)              AS address,
        UPPER(TRIM(status))        AS status,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CURRENT_TIMESTAMP          AS etl_at,
        'raw.branches'             AS etl_source_model
    FROM source_data
)

SELECT * FROM cleaned

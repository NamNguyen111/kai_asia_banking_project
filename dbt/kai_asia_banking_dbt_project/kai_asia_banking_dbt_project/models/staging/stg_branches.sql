{{
  config(
    materialized='incremental',
    schema='staging',
    unique_key='branch_id',
    on_schema_change='fail'
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
    -- Primary key
    branch_id,
    
    -- Business fields  
    TRIM(branch_name) AS branch_name,
    TRIM(address) AS address,
    UPPER(TRIM(status)) AS status,
    
    -- Business date (ngày dữ liệu business)
    DATE(created_at) AS data_date,  -- hoặc business_date
    
    -- Technical timestamps
    created_at,
    
    -- Audit fields (theo yêu cầu anh hướng dẫn)
    CURRENT_TIMESTAMP AS etl_at,  -- thời điểm đẩy vào staging
    'stg_branches' AS etl_source_model
    
  FROM source_data
  
  {% if is_incremental() %}
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    AND branch_id IS NOT NULL
  {% else %}
    WHERE branch_id IS NOT NULL
  {% endif %}
)

SELECT * FROM cleaned
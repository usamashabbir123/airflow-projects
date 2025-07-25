{{
    config(
        materialized='table'
    )
}}

SELECT 
  a.lga_code,
  a.lga_name,
  b.suburb_name
FROM {{ source('bronze','nsw_lga_code') }} a
LEFT JOIN {{ source('bronze','lga_suburb') }} b
  ON Lower(a.LGA_NAME) = lower(b.LGA_NAME)
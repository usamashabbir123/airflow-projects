 {{
    config(
        materialized='table'
    )
 }}
-- This model selects Airbnb listings data and casts columns to appropriate types
-- It also formats dates and handles null values appropriately


WITH cleaned_data AS (
  SELECT
  CAST(listing_id AS BIGINT) AS listing_id,
  CAST(scrape_id AS BIGINT) AS scrape_id,
TO_DATE(NULLIF(TRIM(scraped_date), 'NaN'), 'YYYY-MM-DD') AS scraped_date
,
  CAST(host_id AS BIGINT) AS host_id,
  NULLIF(host_name, 'NaN') AS host_name,
TO_DATE(NULLIF(TRIM(host_since), 'NaN'), 'FMDD/FMMM/YYYY') AS host_since,
  CASE 
    WHEN host_is_superhost = 't' THEN TRUE
    WHEN host_is_superhost = 'f' THEN FALSE
    ELSE NULL
  END AS host_is_superhost,
  NULLIF(host_neighbourhood, 'NaN') AS host_neighbourhood,
  listing_neighbourhood,
  property_type,
  room_type,
  CAST(accommodates AS INTEGER) AS accommodates,
  CAST(price AS NUMERIC(10,2)) AS price,
  CASE 
    WHEN has_availability = 't' THEN TRUE
    WHEN has_availability = 'f' THEN FALSE
    ELSE NULL
  END AS has_availability,
  CAST(availability_30 AS INTEGER) AS availability_30,
  CAST(number_of_reviews AS INTEGER) AS number_of_reviews,
  CAST(NULLIF(review_scores_rating, 'NaN') AS NUMERIC(5,1)) AS review_scores_rating,
  CAST(NULLIF(review_scores_accuracy, 'NaN') AS NUMERIC(5,1)) AS review_scores_accuracy,
  CAST(NULLIF(review_scores_cleanliness, 'NaN') AS NUMERIC(5,1)) AS review_scores_cleanliness,
  CAST(NULLIF(review_scores_checkin, 'NaN') AS NUMERIC(5,1)) AS review_scores_checkin,
  CAST(NULLIF(review_scores_communication, 'NaN') AS NUMERIC(5,1)) AS review_scores_communication,
  CAST(NULLIF(review_scores_value, 'NaN') AS NUMERIC(5,1)) AS review_scores_value
FROM {{ source('bronze', 'airbnb_listings_raw') }}
)
SELECT * from cleaned_data

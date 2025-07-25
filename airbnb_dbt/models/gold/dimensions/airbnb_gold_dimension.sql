{{
    config(
        materialized='table'
    )
}}


-- Dimensions model for Airbnb listings data
SELECT
    listing_id,
    scrape_id,
    scraped_date,
    host_id,
    host_since,
    host_is_superhost,
    accommodates,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
    from {{ ref('cleaned_airbnb') }}
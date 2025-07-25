{{
    config(
        materialized='view'
    )
}}

with base as (
  select
    property_type,
    room_type,
    accommodates,
    SUM(30 - availability_30) as total_stays,
    SUM(price * availability_30) AS estimated_revenue,
    SUM(availability_30) AS total_available_nights,
    ROUND(SUM(price * availability_30) / NULLIF(SUM(availability_30), 0), 2) AS estimated_avg_price,
    DATE_TRUNC('month', scraped_date) as month_year,
    count(*) FILTER (where has_availability is true) as total_listing,
    ROUND((count(*) FILTER (where has_availability is true) * 100.0) / NULLIF(count(*), 0), 2) as active_listings_rate,
    count(*) FILTER (where has_availability is false) as total_inactive,
    ROUND((count(*) FILTER (where has_availability is false) * 100.0) / NULLIF(count(*), 0), 2) as inactive_listings_rate,
    max(price) FILTER (where has_availability is true) as max_active_price,
    min(price) FILTER (where has_availability is true) as min_active_salary,
    max(price) FILTER (where has_availability is true) as maximum_active_salary,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY price)
      FILTER (WHERE has_availability IS TRUE) AS median_active_price,
    count(distinct host_name) as distinct_host_name,
    ROUND((count(*) FILTER (where host_is_superhost = 't') * 100.0) / NULLIF(count(distinct host_name), 0), 2) as superhost_rate,
    ROUND(avg(review_scores_rating) FILTER (where has_availability is true), 2) as review_scores_rating
  from {{ ref('cleaned_airbnb') }}
  group by
    property_type,
    room_type,
    accommodates,
    month_year
),

active_lag as (
  select 
    *,
    LAG(active_listings_rate) OVER (
      PARTITION BY property_type 
      ORDER BY month_year
    ) as previous_active
  from base
),

final_active as (
  select 
    *,
    case 
      when previous_active is null or previous_active = 0 then null
      else ROUND(((active_listings_rate - previous_active) * 100.0) / previous_active, 2)
    end as per_active_change
  from active_lag
),

inactive_lag as (
  select 
    *,
    LAG(inactive_listings_rate) OVER (
      PARTITION BY property_type 
      ORDER BY month_year
    ) as previous_inactive
  from final_active
),

final_inactive as (
  select 
    *,
    case 
      when previous_inactive is null or previous_inactive = 0 then null
      else ROUND(((inactive_listings_rate - previous_inactive) * 100.0) / previous_inactive, 2)
    end as per_inactive_change
  from inactive_lag
),

final as (
  select *
  from final_inactive
)

select * from final
order by property_type, room_type, accommodates, month_year

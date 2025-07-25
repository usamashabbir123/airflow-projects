{{
    config(
        materialized='view'
    )
}}
SELECT 
a.host_neighbourhood as host_neighbourhood_lga ,
count(distinct host_name) as distinct_host,
Round(SUM(price * availability_30)filter(where has_availability is true) /count(distinct host_name),2) as estimated_revenue_per_host,
SUM(price * availability_30 ) filter(where has_availability is true) as estimated_revenue,
DATE_TRUNC('month', scraped_date) as month_year
FROM {{ref('cleaned_airbnb')}} a
JOIN {{ref('cleaned_nsw_lga')}} b
ON LOWER(a.host_neighbourhood) = LOWER(b.suburb_name)
group by 
host_neighbourhood_lga,month_year
order by host_neighbourhood_lga ,month_year
{{
    config(
        materialized='table'
    )
}}  

-- Gold fact model for Airbnb listings data
SELECT  
listing_id,
host_name,
price,
host_neighbourhood,
listing_neighbourhood,
property_type,
room_type
from 
{{ ref('cleaned_airbnb') }}
{% snapshot airbnb_snapshot %}
  {{
    config(
      unique_key=' SCRAPE_ID',
      strategy='timestamp',
      updated_at='SCRAPED_DATE'
    )
  }}

  SELECT * FROM {{ source('bronze', 'airbnb_listings_raw') }}
{% endsnapshot %}
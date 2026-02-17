{{ config(
    materialized='table'
) }}

select
    LocationID as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from {{ source('raw_data', 'taxi_zone_lookup') }}

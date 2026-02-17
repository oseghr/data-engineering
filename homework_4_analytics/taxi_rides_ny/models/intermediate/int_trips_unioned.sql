{{ config(
    materialized='view'
) }}

with green_trips as (
    select
        tripid,
        vendorid,
        'Green' as service_type,
        ratecodeid,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        cast(null as numeric) as ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge
    from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select
        tripid,
        vendorid,
        'Yellow' as service_type,
        ratecodeid,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        cast(null as numeric) as ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        congestion_surcharge
    from {{ ref('stg_yellow_tripdata') }}
),

unioned as (
    select * from green_trips
    union all
    select * from yellow_trips
)

select * from unioned

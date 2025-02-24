{{
    config(
        materialized='table'
    )
}}
with fhv_trips as (
    select
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        timestamp_diff(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
    where 
        dropoff_datetime > pickup_datetime 
        and pickup_locationid is not null 
        and dropoff_locationid is not null
),

p90_travel_times as (
    select
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        percentile_cont(trip_duration, 0.9) over (
            partition by year, month, pickup_locationid, dropoff_locationid
        ) as p90_trip_duration_seconds
    from fhv_trips
)


select distinct
    year,
    month,
    pickup_locationid,
    dropoff_locationid,
    pickup_zone,
    dropoff_zone,
    p90_trip_duration_seconds,
    p90_trip_duration_seconds / 60 as p90_trip_duration_minutes
from p90_travel_times
where month=11 and (pickup_zone='Newark Airport' or pickup_zone='SoHo' or pickup_zone='Yorkville East')
order by
pickup_locationid,p90_trip_duration_minutes
desc


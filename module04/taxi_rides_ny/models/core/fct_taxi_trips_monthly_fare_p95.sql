{{
    config(
        materialized='table'
    )
}}

with valid_trips as (
    select 
        service_type,
        fare_amount,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month
    from {{ ref('fact_trips') }}
    where fare_amount > 0 
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit card')
)

select distinct
    service_type,
    year,
    month,
    percentile_cont(fare_amount, 0.97) over(partition by service_type, year, month) as fare_amount_p97,
    percentile_cont(fare_amount, 0.95) over(partition by service_type, year, month) as fare_amount_p95,
    percentile_cont(fare_amount, 0.90) over(partition by service_type, year, month) as fare_amount_p90
from valid_trips
where year = 2020 and month = 4
order by service_type
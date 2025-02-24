{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select 
        service_type,
        total_amount,
        {{ dbt.date_trunc("quarter", "pickup_datetime") }} as quarter_start,
        extract(quarter from pickup_datetime) as quarter,
        extract(year from pickup_datetime) as year
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) between 2019 and 2020
),

quarterly_revenues as (
    select
        service_type,
        year,
        quarter,
        quarter_start,
        sum(total_amount) as quarterly_revenue
    from trips_data
    group by 1, 2, 3, 4
),

revenue_growth as (
    select
        service_type,
        year,
        quarter,
        quarter_start,
        quarterly_revenue,
        lag(quarterly_revenue) over (
            partition by service_type, quarter
            order by year
        ) as prev_year_revenue,
        round(
            (quarterly_revenue - lag(quarterly_revenue) over (
                partition by service_type, quarter
                order by year
            )) * 100.0 / 
            nullif(lag(quarterly_revenue) over (
                partition by service_type, quarter
                order by year
            ), 0),
            2
        ) as revenue_growth_pct
    from quarterly_revenues
)

select
    service_type,
    year,
    quarter,
    quarter_start,
    quarterly_revenue,
    prev_year_revenue,
    revenue_growth_pct,
    concat(
        year, '/Q', quarter, 
        ' ', service_type, ' Taxi had ',
        case 
            when revenue_growth_pct > 0 then '+'
            else ''
        end,
        cast(revenue_growth_pct as string),
        '% revenue growth compared to ',
        cast(year-1 as string), '/Q', quarter
    ) as growth_statement
from revenue_growth
where year = 2020
order by 
    service_type,
    revenue_growth_pct desc
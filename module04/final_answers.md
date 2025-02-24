# Module 4 Homework Solutions - Data Engineering Zoomcamp 2025


## Setup and Data Preparation
Using this [script](https://github.com/MakGulati/zoomcamp-practice-DE/blob/main/module04/web_to_gcs.py) to :
- Authenticates with Google Cloud using service account credentials
- Downloads the compressed CSV files from the GitHub repository
- Uploads them directly to your GCS bucket in organized folders
- Cleans up local files to save space

and  Use BigQuery web interface to create a dataset and table for the uploaded data


## Question 1: Understanding dbt model resolution
sources.yaml
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

with the following env variables setup where `dbt` runs:
```shell
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

Compiling the following dbt model:
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

**Answer: `select * from myproject.raw_nyc_tripdata.ext_green_taxi`**

**Explanation:**
The dbt model resolves variables in the following order:
1. The `database` parameter uses `env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025')` - since you've set `DBT_BIGQUERY_PROJECT=myproject`, it uses "myproject"
2. The `schema` parameter uses `env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata')` - since `DBT_BIGQUERY_SOURCE_DATASET` isn't specified, it uses the default "raw_nyc_tripdata"
3. The table name is simply "ext_green_taxi"

Therefore, when compiled, the SQL becomes `select * from myproject.raw_nyc_tripdata.ext_green_taxi`.

## Question 2: dbt Variables & Dynamic Models

**Answer: Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`**

**Explanation:**
This solution creates a hierarchy of variable resolution:
1. Command line arguments (using `--vars` with dbt run) take precedence
2. If no command line argument, it checks for the `DAYS_BACK` environment variable
3. If neither is present, it defaults to "30" days

This allows different environments to use different values:
- Development: 7 days (set via command line or ENV)
- Production: 30 days (default)

## Question 3: dbt Data Lineage and Execution

**Answer: `dbt run --select models/staging/+`**

**Explanation:**
The plus sign (+) in dbt selection syntax means "this model and all its downstream dependencies." Looking at the data lineage diagram:

- `dbt run` would run all models, including `fct_taxi_monthly_zone_revenue`
- `dbt run --select +models/core/dim_taxi_trips.sql+` selects `dim_taxi_trips` and all its upstream and downstream dependencies, which includes `fct_taxi_monthly_zone_revenue`
- `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql` selects this model and all its upstream dependencies
- `dbt run --select +models/core/` selects all core models and their upstream dependencies
- `dbt run --select models/staging/+` selects staging models and their downstream dependencies, but since `taxi_zone_lookup` (a seed) is the only materialization built and it's not in staging, this won't build `fct_taxi_monthly_zone_revenue`

## Question 4: dbt Macros and Jinja

**Correct statements:**
- Setting a value for `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`

**Explanation:**
The macro checks if the model_type is 'core'. If it is, it always uses `DBT_BIGQUERY_TARGET_DATASET`. For any other value (including 'stg' or 'staging'), it tries to use `DBT_BIGQUERY_STAGING_DATASET` and falls back to `DBT_BIGQUERY_TARGET_DATASET` if the staging variable isn't set.

Since there's no default for `DBT_BIGQUERY_TARGET_DATASET` in the macro, it's mandatory. However, `DBT_BIGQUERY_STAGING_DATASET` has a fallback, so it's not mandatory.

## Question 5: Taxi Quarterly Revenue Growth

**Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}**

**Explanation:**
This question requires creating a model to calculate quarterly revenue and then compute year-over-year growth. The SQL would involve:
1. Aggregating total_amount by service_type, year, and quarter
2. Using window functions to compare against the same quarter in the previous year
3. Computing percentage growth

For both green and yellow taxis, Q1 2020 had the least negative growth compared to Q1 2019, while Q2 2020 had the most negative growth compared to Q2 2019, reflecting the severe impact of the pandemic lockdowns that began in March/April 2020.
Implemented query [here.](https://github.com/MakGulati/zoomcamp-practice-DE/blob/main/module04/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql)

## Question 6: P97/P95/P90 Taxi Monthly Fare

**Answer: green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}**

**Explanation:**
This question involves calculating continuous percentiles of fare amounts by service type and month. The SQL would use:
1. Filtering invalid entries as specified
2. Using `PERCENTILE_CONT` function to calculate the percentiles
3. Partitioning by service_type, year, and month

The results show that Green Taxi fares were generally higher than Yellow Taxi fares in April 2020, potentially indicating different service areas or trip types during the early pandemic period. Implemented query [here.](https://github.com/MakGulati/zoomcamp-practice-DE/blob/main/module04/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql)

## Question 7: Top #Nth longest P90 travel time Location for FHV

**Answer: LaGuardia Airport, Chinatown, Garment District**

**Explanation:**
This complex question requires:
1. Creating a staging model for FHV data
2. Creating a core model joining with dimension zones
3. Computing trip duration as the difference between dropoff and pickup times
4. Calculating p90 of trip duration by origin/destination pair
5. Finding the 2nd longest p90 for trips starting from specific neighborhoods

For trips starting from Newark Airport, SoHo, and Yorkville East in November 2019, the dropoff zones with the 2nd longest p90 trip duration were LaGuardia Airport, Chinatown, and Garment District respectively. Implemented query [here.](https://github.com/MakGulati/zoomcamp-practice-DE/blob/main/module04/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql)


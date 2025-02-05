## Quiz Questions
#### Complete the Quiz shown below. It’s a set of 6 multiple-choice questions to test your understanding of workflow orchestration, Kestra and ETL pipelines for data lakes and warehouses.

1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)? 
- [x] 128.3 MB
- [ ] 134.5 MB
- [ ] 364.7 MB
- [ ] 692.6 MB
---
yellow total=1,240,048,218 
enteries=1,461,897
---
2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

- [ ] {{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv
- [x] green_tripdata_2020-04.csv
- [ ] green_tripdata_04_2020.csv
- [ ] green_tripdata_2020.csv

3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
- [ ] 13,537.299
- [x] 24,648,499
- [ ] 18,324,219
- [ ] 29,430,127

```sql
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2020-__.csv';
```

4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
- [ ] 5,327,301
- [ ] 936,199
- [x] 1,734,051 (can be seen in green_copy_in_to_staging_table)
- [ ] 1,342,034

```sql
SELECT COUNT(*)
FROM public.green_tripdata
WHERE filename LIKE 'green_tripdata_2020-__.csv';
```
5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
- [ ] 1,428,092
- [ ] 706,911
- [x] 1,925,152 (can be seen in yellow_copy_in_to_staging_table)
- [ ] 2,561,031

```sql
SELECT COUNT(*)
FROM public.yellow_tripdata
WHERE filename LIKE 'yellow_tripdata_2021-03.csv';
```

6. How would you configure the timezone to New York in a Schedule trigger?
- [ ] Add a timezone property set to EST in the Schedule trigger configuration
- [x] Add a timezone property set to America/New_York in the Schedule trigger configuration
- [ ] Add a timezone property set to UTC-5 in the Schedule trigger configuration
- [ ] Add a location property set to New_York in the Schedule trigger configuration

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York # New York time zone
    inputs:
      taxi: green

  - id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    timezone: America/New_York # New York time zone
    inputs:
      taxi: yellow
``` 
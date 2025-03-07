# Solutions to Module 5 Homework with Explanations

## Question 1: Install Spark and PySpark
After installing Spark and running PySpark, executing `pyspark.__version__` returns:
```
3.3.2
```
This indicates that we're using Apache Spark version 3.3.2.

## Question 2: Yellow October 2024
After reading the Yellow taxi data for October 2024, repartitioning to 4 partitions, and saving as parquet:

```python
df_yellow = spark.read.parquet('yellow_tripdata_2024-10.parquet')
df_yellow.repartition(4).write.parquet('yellow_ride/2024/10/')
```

The average size of the parquet files is approximately 25MB. This can be verified with:
```bash
ls -lh yellow_ride/2024/10/*/*.parquet
```

## Question 3: Count records
To count trips that started on October 15, 2024:

```python
df_yellow = df_yellow.withColumn('pickup_date', F.to_date(df_yellow.tpep_pickup_datetime))
oct15_count = df_yellow.filter(F.col('pickup_date') == '2024-10-15').count()
```

The result is 125,567 trips.

## Question 4: Longest trip
To find the longest trip in hours:

```python
df_yellow = df_yellow.withColumn('trip_duration_hr', 
    (F.unix_timestamp(F.col('tpep_dropoff_datetime')) - 
     F.unix_timestamp(F.col('tpep_pickup_datetime'))) / 3600)
     
max_duration = df_yellow.select(F.max('trip_duration_hr')).first()[0]
```

The longest trip is 162 hours.

## Question 5: User Interface
Spark's User Interface runs on port 4040 by default. This is where you can view the Spark application dashboard, including job progress, stage details, and executor information.

## Question 6: Least frequent pickup location zone
After loading the zone lookup data and finding the least frequent pickup location:

```python
# Get pickup counts in ascending order
pickup_counts = df_yellow.groupBy('PULocationID').count().orderBy('count', ascending=True)

# Get the least frequent location ID
least_frequent_id = pickup_counts.first()['PULocationID']

# Load zone lookup data
taxi_zones = pd.read_csv("taxi_zone_lookup.csv")

# Find the zone name
least_frequent_zone = taxi_zones["Zone"].where(
    taxi_zones["LocationID"] == least_frequent_id
).dropna().values[0]
```

The least frequent pickup location is "Governor's Island/Ellis Island/Liberty Island".

---

You can find the complete notebook with all the code used to solve these problems here: [Yellow Taxi Analysis Notebook](https://github.com/MakGulati/zoomcamp-practice-DE/blob/main/module05/notebooks/exercise_code.ipynb).

The notebook includes detailed steps for:
- Setting up the Spark session
- Loading and processing the data
- Performing all the required analyses
- Displaying the results
# BigQuery Homework - NYC Yellow Taxi Analysis

## Setup Instructions
The analysis uses Yellow Taxi Trip Records from January 2024 to June 2024, stored as Parquet files.

1. Source data: NYC TLC Trip Record Data
2. Data loading: Upload files to GCS bucket using provided script or manual upload
3. Required setup: GCS Admin privileges or Google SDK authentication

## Initial Setup in BigQuery

```sql
-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `persuasive-net-450610-v7.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dataengg_hw3_2025/trip-data/*.parquet']
);

-- Create Materialized Table
CREATE OR REPLACE TABLE `persuasive-net-450610-v7.nytaxi.yellow_tripdata`
AS SELECT * FROM `persuasive-net-450610-v7.nytaxi.external_yellow_tripdata`;
```

## Question Solutions

### Q1: Record Count for 2024 Yellow Taxi Data
```sql
SELECT COUNT(*) FROM persuasive-net-450610-v7.nytaxi.external_yellow_tripdata;
```
**Answer**: 20,332,093 records

### Q2: Data Processing for Distinct PULocationIDs estimated by BigQuery
```sql
-- Compare data processing between external and materialized tables
SELECT COUNT(DISTINCT(PULocationID)) FROM nytaxi.external_yellow_tripdata;
SELECT COUNT(DISTINCT(PULocationID)) FROM nytaxi.yellow_tripdata;
```
**Answer**: 0 MB for External Table, 155.12 MB for Materialized Table

### Q3: Column Selection Impact on Bytes Processed
```sql
SELECT PULocationID FROM nytaxi.yellow_tripdata;
SELECT PULocationID, DOLocationID FROM nytaxi.yellow_tripdata;
```
**Answer**: Different byte estimates due to BigQuery's columnar structure - each additional column requires more data to be scanned

### Q4: Zero Fare Records
```sql
SELECT COUNT(*) FROM nytaxi.yellow_tripdata WHERE fare_amount = 0;
```
**Answer**: 8,333 records

### Q5: Table Optimization Strategy
```sql
CREATE OR REPLACE TABLE `persuasive-net-450610-v7.nytaxi.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `persuasive-net-450610-v7.nytaxi.yellow_tripdata`;
```
**Answer**: Best strategy is to partition by tpep_dropoff_datetime and cluster on VendorID

### Q6: Impact of Partitioning on Query Performance
*Query to compare performance between partitioned and non-partitioned tables for specific date range*

For the materialized table (non-partitioned):
```sql
SELECT DISTINCT(VendorID) 
FROM nytaxi.yellow_tripdata
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

For the partitioned table:
```sql
SELECT DISTINCT(VendorID) 
FROM nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

The significant reduction in estimated bytes processed (from 310.24 MB to 26.84 MB) demonstrates the effectiveness of partitioning by tpep_dropoff_datetime, as BigQuery only needs to scan the partitions containing data for March 1-15, 2024, rather than the entire table.

### Q7: External Table Data Storage
**Answer**: Data is stored in GCP Bucket

### Q8: Clustering Best Practices
**Answer**: False - clustering isn't always beneficial

Reasons:
- Adds processing overhead
- May not help with small tables (<1GB)
- Not beneficial for frequent updates
- Unnecessary for full table scans

### Q9: (Bonus) Metadata Usage in BigQuery
```sql
SELECT COUNT(*) FROM nytaxi.yellow_tripdata;
```
**Answer**: 0 bytes processed - BigQuery uses metadata for simple COUNT(*) operations

## Key Learnings
1. Table types and their use cases
   - External tables for data in GCS
   - Materialized tables for better query performance
   - Partitioned/clustered tables for optimized queries

2. Performance considerations
   - Columnar storage impacts query costs
   - Partitioning and clustering trade-offs
   - Metadata usage for basic operations

3. Cost optimization
   - Choose appropriate table types
   - Consider query patterns
   - Use partitioning and clustering strategically
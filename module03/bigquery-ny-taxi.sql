-- Query the public citibike stations dataset as a reference
SELECT station_id, name 
FROM bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

-- Create an external table pointing to Parquet files in Google Cloud Storage
-- This table definition only stores the schema, actual data remains in GCS
CREATE OR REPLACE EXTERNAL TABLE `persuasive-net-450610-v7.nytaxi.external_yellow_tripdata`
OPTIONS (
 format = 'PARQUET',
 uris = ['gs://dataengg_hw3_2025/trip-data/*.parquet']
);

-- Create a materialized copy of the external table in BigQuery storage
-- This copies all data into BigQuery's optimized format for better query performance
CREATE OR REPLACE TABLE `persuasive-net-450610-v7.nytaxi.yellow_tripdata`
AS SELECT * FROM `persuasive-net-450610-v7.nytaxi.external_yellow_tripdata`;

-- Compare number of unique pickup locations between external and materialized tables
-- These should return the same count as they contain the same data
SELECT COUNT(DISTINCT(PULocationID)) FROM nytaxi.external_yellow_tripdata;
SELECT COUNT(DISTINCT(PULocationID)) FROM nytaxi.yellow_tripdata;

-- Examine pickup locations 
SELECT PULocationID FROM nytaxi.yellow_tripdata;

-- Look at both pickup and dropoff locations together
SELECT PULocationID, DOLocationID FROM nytaxi.yellow_tripdata;

-- Count trips with zero fare amount (potential data quality issue)
SELECT COUNT(*) FROM nytaxi.yellow_tripdata WHERE fare_amount = 0;

-- Create an optimized table with partitioning and clustering
-- Partitioned by dropoff date for efficient time-based queries
-- Clustered by VendorID for better performance on vendor-specific analysis
CREATE OR REPLACE TABLE `persuasive-net-450610-v7.nytaxi.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `persuasive-net-450610-v7.nytaxi.yellow_tripdata`;

--- Compare number of unique vendors between partitioned and non-partitioned tables
-- These should return the same count as they contain the same data
SELECT COUNT(DISTINCT(VendorID)) FROM nytaxi.yellow_tripdata
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(DISTINCT(VendorID)) FROM `nytaxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Compare total row counts between tables
-- These should be identical as they contain the same data
SELECT COUNT(*) FROM nytaxi.yellow_tripdata;
SELECT COUNT(*) FROM nytaxi.external_yellow_tripdata;
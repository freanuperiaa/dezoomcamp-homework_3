-- DE ZOOMCAMP: Module 3 
-- Solution and Code to Homework 3




-- !!!!! Setup !!!!!
--------------------------------------------------------------------------------

-- Create a table using the Green Taxi Trip Records Data for 2022
-- using the code from the week 3 module

CREATE OR REPLACE EXTERNAL TABLE `model-caldron-411807.ny_taxi.green_taxi_data_2022_external`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://mage-zoomcamp-coy-peria/green_taxi/green_tripdata_2022-*.parquet']
)

-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).
CREATE OR REPLACE TABLE ny_taxi.green_taxi_data_2022 AS 
SELECT * FROM ny_taxi.green_taxi_data_2022_external



-- !!!!! Questions !!!!!
--------------------------------------------------------------------------------

-- Question 1: What is the count of records for the 2022 Green Taxi Data?
SELECT COUNT(1) FROM ny_taxi.green_taxi_data_2022

-- ANSWER: 840402



-- Question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT DISTINCT PULocationID FROM ny_taxi.green_taxi_data_2022_external; -- 0 mb
SELECT DISTINCT PULocationID FROM ny_taxi.green_taxi_data_2022; -- 6.41 mb

-- ANSWER: 0mb for external table, 6.41 mb for materialized table



-- Question 3: How many records have a fare_amount of 0?
SELECT COUNT(1) FROM ny_taxi.green_taxi_data_2022
WHERE fare_amount = 0;

-- ANSWER: 1622



-- Question 4: What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- ANSWER: Partition by lpep_pickup_datetime Cluster on PUlocationID
CREATE TABLE ny_taxi.green_taxi_data_2022_partitioned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID 
AS
SELECT * FROM ny_taxi.green_taxi_data_2022


-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

-- materialized table
SELECT DISTINCT PULocationID FROM ny_taxi.green_taxi_data_2022
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
-- 12.82 MB 2022-06-01

-- partitioned_clustered table
SELECT DISTINCT PULocationID FROM ny_taxi.green_taxi_data_2022_partitioned_clustered
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30'
-- 1.12 MB when run

-- ANSWER: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table



-- Question 6: Where is the data stored in the external Table you created?

-- ANSWER: in GCP Bucket. it is in the details of the table.
-- The details page of my external table has this info:

/*

 External Data Configuration
Source URI(s):
gs://mage-zoomcamp-c********a/green_taxi/green_tripdata_2022-*.parquet

*/



-- Question 7: It is best practice in Big Query to always cluster your data:
-- ANSWER: True

-- (source: https://cloud.google.com/bigquery/docs/clustered-tables#:~:text=You%20should%20therefore%20always%20consider,blocks%20that%20match%20the%20filter.)




-- Question 8: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

SELECT COUNT(*) FROM ny_taxi.green_taxi_data_2022;

-- ANSWER: it estimates 0B to be processed. Probably because it is already cached?



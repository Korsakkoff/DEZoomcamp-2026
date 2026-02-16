# Module 4 Homework: Analytics Engineering with dbt

## Setup: Loading the data
1.Set up your dbt project following the setup guide

<p align="center">
  <img src="../../assets/homework/module_4/setup_dbt.jpg">
</p>

2.Load the Green and Yellow taxi data for 2019-2020 into your warehouse.
Data in GCS.
<p align="center">
  <img src="../../assets/homework/module_4/setup_gcs.jpg">
</p>

Tables in BigQuery.
<p align="center">
  <img src="../../assets/homework/module_4/setup_bq.jpg">
</p>

## BigQuery Setup
Create an external table using the Yellow Taxi Trip Records.<br>
Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).

<p align="center">
  <img src="../../assets/homework/module_3/rides_dataset_tables.jpg">
</p>


## Question 1. Counting records. 
What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT 
  COUNT(*) 
FROM 
  `de-zoomcamp-2026-484918.rides_dataset.rides_materialized`;
```
<p align="center">
  <img src="../../assets/homework/module_3/question1.jpg">
</p>
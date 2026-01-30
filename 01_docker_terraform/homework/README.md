# Module 1 Homework: Docker & SQL

# Question 1. Understanding Docker images
```bash
# Go to homework directory
cd 01_docker_terraform/homework/

# Run docker with the python:3.13 image. Use an entrypoint bash to interact with the container.
docker run -it \
    --rm \
    --entrypoint=bash \
    python:3.13

# Check pip version in the image
pip -V

# Exit docker
exit
```


# Question 2. Understanding Docker networking and docker-compose
```bash
# Create the docker-compose.yaml file inside homework folder, then run it.
docker-compose up
```
<!--
Access pgAdmin via webBrowser
	http://127.0.0.1:8080/browser/
	Username: pgadmin@pgadmin.com
	Password: pgadmin


Register Server
	Name: Postgres
	Host name/address: db
	Port: 5432
	Username: postgres
	Password: postgres 
-->

Prepare the Data
```bash
# Download the data
wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet 
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

# Run docker compose
docker-compose up

# Go to homework directory and go into docker bash
cd 01_docker_terraform/homework/
docker compose exec notebook bash

# Jupyter notebook to python script.
jupyter nbconvert \
  --to=script \
  --output=dim_taxi_zone_ingest \
  notebook.ipynb

# Run the Script to ingest taxi zone data.
python dim_taxi_zone_ingest.py

# Jupyter notebook to python script 
jupyter nbconvert \
  --to=script \
  --output=facts_green_tripdata \
  notebook_parquet.ipynb

# Run the Script to ingest taxi green data.
python facts_green_tripdata.py
```

<!-- Run SQL Queries on pgAdmin. -->
# Question 3. Counting short trips
```sql
SELECT 
    COUNT(*)
FROM 
	public.green_tripdata
WHERE 
	lpep_pickup_datetime >= '2025-11-01' AND lpep_pickup_datetime < '2025-12-01' AND trip_distance <=1
```

# Question 4. Longest trip for each day
```sql
SELECT
	DATE(lpep_pickup_datetime) AS "date",
	MAX(trip_distance) AS max_trip_distance
FROM 
	public.green_tripdata
WHERE 
	trip_distance < 100
GROUP BY 
	DATE(lpep_pickup_datetime)
ORDER BY 
	MAX(trip_distance) DESC
LIMIT 1
```

# Question 5. Biggest pickup zone
```sql
SELECT 
	taxi_zone."Zone", 
	SUM(total_amount) AS sum_total_amount
FROM
	public.green_tripdata trip_data 
	LEFT JOIN public.dim_taxi_zone taxi_zone ON trip_data."PULocationID" = taxi_zone."LocationID"
WHERE
	DATE(lpep_pickup_datetime) = '2025-11-18'
GROUP BY
	taxi_zone."Zone"
ORDER BY 
	SUM(total_amount) DESC
LIMIT 1
```

# Question 6. Largest tip
```sql
SELECT 
	dropoff_zone."Zone", 
	MAX(tip_amount)
FROM
	public.green_tripdata trip_data 
	LEFT JOIN public.dim_taxi_zone pickup_zone ON trip_data."PULocationID" = pickup_zone."LocationID"
	LEFT JOIN public.dim_taxi_zone dropoff_zone ON trip_data."DOLocationID" = dropoff_zone."LocationID"
WHERE
	pickup_zone."Zone" = 'East Harlem North'
	AND DATE_PART('month', lpep_pickup_datetime) = 11
	AND DATE_PART('year', lpep_pickup_datetime) = 2025
GROUP BY 
	dropoff_zone."Zone"
ORDER BY
	MAX(tip_amount) DESC
LIMIT 1
```
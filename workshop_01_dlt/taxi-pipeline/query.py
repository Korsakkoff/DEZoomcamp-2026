import duckdb

# Connect to the DuckDB database created by the taxi_pipeline in read-only mode
con = duckdb.connect("taxi_pipeline.duckdb", read_only=True)
# Switch to the dlt dataset schema used by taxi_pipeline
con.execute("SET search_path = nyc_taxi_data")

# Compute the total amount of money generated in tips
result = con.execute(
    "SELECT SUM(tip_amt) AS total_tips FROM nyc_taxi_trips"
).fetchone()

print(f"Total tip amount: {result[0]}")

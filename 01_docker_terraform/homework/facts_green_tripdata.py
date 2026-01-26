# Import libraries
import pandas as pd
import pyarrow
from sqlalchemy import create_engine

# Read parquet file into DataFrame
df = pd.read_parquet(
   'green_tripdata_2025-11.parquet'
)

# Define data types for each column
df["store_and_fwd_flag"] = (
    df["store_and_fwd_flag"]
    .map({"Y": True, "N": False})
    .astype("boolean")
)

df = df.astype({
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "payment_type": "Int64",
    "trip_type": "Int64"
})

# Create a SQLAlchemy engine to connect to the PostgreSQL database
engine = create_engine('postgresql://postgres:postgres@db:5432/ny_taxi')

# Read parquet file into DataFrame
df = pd.read_parquet(
   'green_tripdata_2025-11.parquet'
)

# Load DataFrame into PostgreSQL table
df.to_sql(
    name="green_tripdata",
    con=engine,
    if_exists="replace",
    index=False
)

# Print the number of rows loaded
print("Loaded ", len(df), " rows.")


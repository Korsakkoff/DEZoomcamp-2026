# Import libraries
import pandas as pd
from sqlalchemy import create_engine

# Read CSV file into DataFrame
df = pd.read_csv('taxi_zone_lookup.csv', dtype=dtype)

# Define data types for each column
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string"
}

# Create a SQLAlchemy engine to connect to the PostgreSQL database
engine = create_engine('postgresql://postgres:postgres@db:5432/ny_taxi')

# Load DataFrame into PostgreSQL table
df.to_sql(
    name="dim_taxi_zone",
    con=engine,
    if_exists="replace",
    index=False
)

# Print the number of rows loaded
print("Loaded ", len(df), " rows.")
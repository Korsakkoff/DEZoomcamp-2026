import pandas as pd
import json
import dataclasses
import time

from kafka import KafkaProducer
from dataclasses import dataclass
from models import Ride, ride_from_row, ride_serializer, ride_deserializer


url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet'

columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server], 
    value_serializer=ride_serializer
)

topic_name = 'green-trips'

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')
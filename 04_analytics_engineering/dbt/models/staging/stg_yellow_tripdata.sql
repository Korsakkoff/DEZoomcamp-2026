select
        -- identifiers
        cast(vendor_id as integer) as vendor_id,
        {{ safe_cast('ratecode_id', 'integer') }} as rate_code_id,
        cast(pulocation_id as integer) as pickup_location_id,
        cast(dolocation_id as integer) as dropoff_location_id,

        -- timestamps
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime, -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

        -- trip info
        cast(store_and_fwd_flag as string) as store_and_fwd_flag,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,

        -- payment info
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        cast(total_amount as numeric) as total_amount,
        {{ safe_cast('payment_type', 'integer') }} as payment_type
from {{ source("raw_data", "yellow_tripdata")}}
    -- Filter out records with null vendor_id (data quality requirement)
    where vendor_id is not null
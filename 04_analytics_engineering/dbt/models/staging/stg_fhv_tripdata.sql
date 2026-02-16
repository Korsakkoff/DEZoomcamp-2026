select 
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocation_id as pickup_location_id,
    dolocation_id as dropoff_location_id,
    sr_flag as sr_flag
from 
    {{ source("raw_data", "fhv_tripdata") }}
where 
    dispatching_base_num is not null
    and extract(year from pickup_datetime) = 2019
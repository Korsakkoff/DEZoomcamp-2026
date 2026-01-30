# Module 2 Homework: Workflow orchestration

## Question 1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
I added a task within the flow so I can see the file size in the flow logs in Kestra.

```yaml
- id: check_file_size
    type: io.kestra.plugin.scripts.shell.Commands
    commands:
      - BYTES=$(stat -c "%s" "{{ render(vars.data) }}")
      - echo "Size in MiB:"
      - awk -v b="$BYTES" 'BEGIN { printf "%.1f MiB\n", b/1024/1024 }'
```

##  Question 2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
Just checked the logs.

##  Question 3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
```sql
SELECT 
  COUNT(*) 
FROM 
  `de-zoomcamp-2026-484918.zoomcamp.yellow_tripdata`
WHERE
  filename LIKE '%2020%'
```

##  Question 4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
```sql
SELECT 
  COUNT(*) 
FROM 
  `de-zoomcamp-2026-484918.zoomcamp.green_tripdata`
WHERE
  filename LIKE '%2020%'
```

##  Question 5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
```sql
SELECT 
  COUNT(*) 
FROM 
  `de-zoomcamp-2026-484918.zoomcamp.yellow_tripdata`
WHERE
  filename = 'yellow_tripdata_2021-03.csv'
```

##  Question 6. How would you configure the timezone to New York in a Schedule trigger?
Asked Kestra AI because I couldn't find the answer in the trigger documentation page. Add the property timezone to the trigger.

```yaml
triggers:
  - id: my_trigger
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: America/New_York
```
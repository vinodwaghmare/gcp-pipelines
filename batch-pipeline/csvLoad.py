from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = "helpful-cat-324414.dataset1.flighttable"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("YEAR", "INTEGER"),
        bigquery.SchemaField("MONTH", "INTEGER"),
        bigquery.SchemaField("DAY", "INTEGER"),
        bigquery.SchemaField("DAY_OF_WEEK", "INTEGER"),
        bigquery.SchemaField("AIRLINE", "STRING"),
        bigquery.SchemaField("FLIGHT_NUMBER", "INTEGER"),
        bigquery.SchemaField("TAIL_NUMBER", "STRING"),
        bigquery.SchemaField("ORIGIN_AIRPORT", "STRING"),
        bigquery.SchemaField("DESTINATION_AIRPORT", "STRING"),
        bigquery.SchemaField("SCHEDULED_DEPARTURE", "INTEGER"),
        bigquery.SchemaField("DEPARTURE_TIME", "INTEGER"),
        bigquery.SchemaField("DEPARTURE_DELAY", "INTEGER"),
        bigquery.SchemaField("TAXI_OUT", "INTEGER"),
        bigquery.SchemaField("WHEELS_OFF", "INTEGER"),
        bigquery.SchemaField("SCHEDULED_TIME", "INTEGER"),
        bigquery.SchemaField("ELAPSED_TIME", "INTEGER"),
        bigquery.SchemaField("AIR_TIME", "INTEGER"),
        bigquery.SchemaField("DISTANCE", "INTEGER"),
        bigquery.SchemaField("WHEELS_ON", "INTEGER"),
        bigquery.SchemaField("TAXI_IN", "INTEGER"),
        bigquery.SchemaField("SCHEDULED_ARRIVAL", "INTEGER"),
        bigquery.SchemaField("ARRIVAL_TIME", "INTEGER"),
        bigquery.SchemaField("ARRIVAL_DELAY", "INTEGER"),
        bigquery.SchemaField("DIVERTED", "INTEGER"),
        bigquery.SchemaField("CANCELLED", "INTEGER"),
        bigquery.SchemaField("CANCELLATION_REASON", "STRING"),
        bigquery.SchemaField("AIR_SYSTEM_DELAY", "INTEGER"),
        bigquery.SchemaField("SECURITY_DELAY", "INTEGER"),
        bigquery.SchemaField("AIRLINE_DELAY", "INTEGER"),
        bigquery.SchemaField("LATE_AIRCRAFT_DELAY", "INTEGER"),
        bigquery.SchemaField("WEATHER_DELAY", "INTEGER"),
    ],
    skip_leading_rows=1,
    range_partitioning=bigquery.RangePartitioning(
    # To use integer range partitioning, select a top-level REQUIRED /
    # NULLABLE column with INTEGER / INT64 data type.
    field="MONTH",
    range_=bigquery.PartitionRange(start=1, end=12, interval=1),
    ),
)
uri = "gs://dezyre-test-bucket1/data/flights.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Wait for the job to complete.

table = client.get_table(table_id)
print("Loaded {} rows to table {}".format(table.num_rows, table_id))
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Set table_id to the ID of the table to create.
table_id = "poc-analytics-ai.dataset_vw.airports_partitioning_clustering"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("IATA_CODE", "STRING"),
        bigquery.SchemaField("AIRPORT", "STRING"),
        bigquery.SchemaField("CITY", "STRING"),
        bigquery.SchemaField("STATE", "STRING"),
        bigquery.SchemaField("COUNTRY", "STRING"),
        bigquery.SchemaField("LATITUDE", "FLOAT"),
        bigquery.SchemaField("LONGITUDE", "FLOAT"),

    ],
    time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY,
    ),
    skip_leading_rows=1,
    clustering_fields=["IATA_CODE"],
)
uri = "gs://test-bucket-vw/csv/airports_1.2.csv"

load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  # Make an API request.

load_job.result()  # Wait for the job to complete.

table = client.get_table(table_id)
print("Loaded {} rows to table {}".format(table.num_rows, table_id))

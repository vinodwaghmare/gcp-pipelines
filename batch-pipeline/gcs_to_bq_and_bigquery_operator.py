# import dependencies
import os
from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from google.cloud import bigquery


# python logic to derive yestarday date 
yestarday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# default arguments
default_args = {
    'owner' : 'dataflow',
    'start_date' : yestarday,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

# Defining DAG
with DAG('gcs_to_bq_and_bq_operator',
        catchup=False,
        default_args = default_args,
        schedule_interval=timedelta(days=1)
        ) as dag:

    # Dummy start task
    start = DummyOperator(
        task_id= 'start', 
        dag=dag,
         )
    
    # gcs to bigquery task
    gcs_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket = 'test-bucket-vw',
        source_objects ='csv/airports_1.2.csv',
        destination_project_dataset_table = 'poc-analytics-ai.dataset_vw.gcs_to_bigquery',
        schema_fields = [
                    {'name': 'IATA_CODE', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'AIRPORT', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'CITY', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'STATE', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'COUNTRY', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'LATITUDE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'LONGITUDE', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag = dag
    )

    # Performing Transformation on top of the bigquery table 
    Bigquery_operations = BigQueryOperator(
        task_id ='Bigquery_operations',
        sql = " create or replace table dataset_vw.state_per_country \
            as \
            select \
            COUNTRY, \
            count(STATE) as no_of_states \
            from dataset_vw.gcs_to_bigquery \
            group by COUNTRY ",
            use_legacy_sql=False,
            dag = dag
            )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# tasks dependencies
start >> gcs_to_bigquery >> Bigquery_operations >> end
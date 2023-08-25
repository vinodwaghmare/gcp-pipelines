# import dependencies
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# python logic to derive yestarday date 
yestarday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# default arguments 

default_arguments = {
    'start_date' : yestarday,
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

# python custom logic/function

def print_hello():
    print("I'm python operator")

# DAG Definitions 

with DAG(dag_id='Python_Bash_Operator',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_arguments) as dag :
    
    # Tasks start here
    
    # Dummy start task
    start = DummyOperator(
        task_id= 'start', 
        dag=dag,
         )

    # Bash operator task
    bash_task = BashOperator(
    task_id='bash_task',
    bash_command="date;echo Hey, I'm bash operator",
    )

    # Python operator task
    python_task = PythonOperator(task_id='python_task',
    python_callable=print_hello,
    dag=dag,
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Setting up task dependencies using Airflow standard notation
start >> bash_task >> python_task >> end

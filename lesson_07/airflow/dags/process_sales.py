import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from requests import post

# Define default arguments
default_args = {
    'owner': 'user',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id='process_sales',
        default_args=default_args,
        description='DAG to process sales data by extracting and converting to Avro',
        schedule_interval='0 1 * * *',  # Run daily at 1 AM UTC
        start_date=datetime(2022, 8, 9),
        end_date=datetime(2022, 8, 11),
        max_active_runs=1,
        catchup=True,
) as dag:
    # Task 1: Extract data from API
    def extract_data_from_api(**kwargs):
        execution_date = kwargs['ds']
        raw_dir = os.path.join("C:", "Users", "small", "PycharmProjects", "FlaskServerForJobs", "lesson_02", "fetched_data", "raw", execution_date)

        response = post(
            url="http://host.docker.internal:8081",  # Assuming job_1 runs on port 8081
            json={'raw_dir': raw_dir, 'dates': [execution_date]}
        )

        if response.status_code != 201:
            raise Exception(f"Failed to extract data for {execution_date}. Status code: {response.status_code}")
        print(f"Data extraction successful for {execution_date}")


    extract_data_from_api = PythonOperator(
        task_id='extract_data_from_api',
        python_callable=extract_data_from_api
    )


    # Task 2: Convert JSON to Avro
    def convert_to_avro(**kwargs):
        execution_date = kwargs['ds']
        raw_dir = f"C:\\Users\\small\\PycharmProjects\\FlaskServerForJobs\\lesson_02\\fetched_data\\raw\\{execution_date}"
        stg_dir = f"C:\\Users\\small\\PycharmProjects\\FlaskServerForJobs\\lesson_02\\fetched_data\\stg\\{execution_date}"


        response = post(
            url="http://host.docker.internal:8082",  # Assuming job_2 runs on port 8082
            json={'raw_dir': raw_dir, 'stg_dir': stg_dir}
        )

        if response.status_code != 201:
            raise Exception(f"Failed to convert data to Avro for {execution_date}. Status code: {response.status_code}")
        print(f"Conversion to Avro successful for {execution_date}")


    convert_to_avro = PythonOperator(
        task_id='convert_to_avro',
        python_callable=convert_to_avro
    )

    # Set task dependencies
    extract_data_from_api >> convert_to_avro

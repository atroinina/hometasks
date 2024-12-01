from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

# ініціалізація DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'catchup': False,
}

dag = DAG(
    'process_customers_to_bronze_and_silver',
    default_args=default_args,
    description='Process customer data to bronze and silver',
    schedule_interval=None,  # для ручного запуску
)
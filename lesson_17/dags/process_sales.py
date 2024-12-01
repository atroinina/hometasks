from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ініціалізація DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'catchup': False,
}

dag = DAG(
    'process_sales_to_bronze',
    default_args=default_args,
    description='Process sales data to bronze',
    schedule_interval=None,  # для ручного запуску
)

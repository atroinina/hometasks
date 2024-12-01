from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

# ініціалізація DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'catchup': False,
}

dag = DAG(
    'process_user_profiles_to_silver',
    default_args=default_args,
    description='Process user profiles data to silver and enrich customers',
    schedule_interval=None,  # для ручного запуску
)


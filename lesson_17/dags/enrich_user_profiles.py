from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
import os

# ініціалізація DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'catchup': False,
}

dag = DAG(
    'process_user_profiles_to_gold',
    default_args=default_args,
    description='Process user profiles to gold and enrich customers',
    schedule_interval=None,  # для ручного запуску
)


def process_user_profiles_to_gold():
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

    # Initialize Spark session
    spark = SparkSession.builder.appName("EnrichCustomerData").getOrCreate()

    # Define the paths to the customers CSV files and the user profiles JSON
    customers_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\customers"
    user_profiles_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\proccesed_user_profiles"

    # Combine all customers
    df_customers = spark.read.csv(customers_path, header=True, inferSchema=False)
    df_customers = df_customers.drop('first_name')
    df_customers = df_customers.drop('last_name')
    df_customers = df_customers.drop('state')
    df_user_profiles = spark.read.json(user_profiles_path)
    df_enriched = df_customers.join(
        df_user_profiles,
        df_customers.email == df_user_profiles.email,
        how='left'
    )

    df_enriched.show()
    # Закриваємо сесію Spark
    spark.stop()

process_user_profiles_to_gold_task = PythonOperator(
    task_id='enrich_user_profiles',
    python_callable=process_user_profiles_to_gold,
    dag=dag,
)

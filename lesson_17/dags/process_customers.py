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
    'process_customers',
    default_args=default_args,
    description='Process customer data to bronze and silver',
    schedule_interval=None,
)

def process_customers_data():
    # Set Hadoop-related environment variables
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

    # Шлях до вхідних даних (raw data) та вихідних каталогів (output)
    input_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\data\customers\2022-08-5"
    output_path_bronze = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\bronze\customers"
    output_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\customers"

    # Initialize Spark session
    spark = SparkSession.builder.appName('FetchCustomersData').getOrCreate()

    # For bronze
    for dirpath, dirnames, filenames in os.walk(input_path):
        for filename in filenames:
            if filename.endswith(".csv"):  # Filter for CSV files
                filepath = os.path.join(dirpath, filename)
                df = spark.read.csv(filepath, header=True, inferSchema=False)
                result = os.path.join(output_path_bronze, filename)
                df.toPandas().to_csv(result, index=False, header=True)

    for dirpath, dirnames, filenames in os.walk(input_path):
        for filename in filenames:
            if filename.endswith(".csv"):  # Filter for CSV files
                filepath = os.path.join(dirpath, filename)
                df = spark.read.csv(filepath, header=True, inferSchema=False)

                # Clean and rename columns for the silver layer
                df_silver = (df
                .withColumn("Id", trim(col("Id")))
                .withColumn("FirstName", trim(col("FirstName")))
                .withColumn("LastName", trim(col("LastName")))
                .withColumn("Email", trim(col("Email")))
                .withColumn("RegistrationDate", trim(col("RegistrationDate")))
                .withColumn("State", trim(col("State")))
                .select(
                    col("Id").alias('client_id'),
                    col("FirstName").alias('first_name'),
                    col("LastName").alias('last_name'),
                    col("Email").alias('email'),
                    col("RegistrationDate").alias('registration_date'),
                    col("State").alias('state'),
                ))

                # Save to the silver layer (cleaned data)
                result_silver = os.path.join(output_path_silver, filename)
                df_silver.toPandas().to_csv(result_silver, index=False, header=True)
                print(f"Saved cleaned data to silver: {result_silver}")
    # Закриваємо сесію Spark
    spark.stop()

process_customers_task = PythonOperator(
    task_id='process_customers',
    python_callable=process_customers_data,
    dag=dag,
)
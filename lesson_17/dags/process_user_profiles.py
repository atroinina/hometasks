from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import os

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

def process_user_profiles():
    # Set Hadoop-related environment variables
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

    # Шлях до даних user_profiles
    user_profiles_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\data\user_profiles"
    output_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\customers"

    # Ініціалізація сесії Spark
    spark = SparkSession.builder.appName('ProcessUserProfiles').getOrCreate()

    # Читання даних з файлів JSONLine
    user_profiles_df = spark.read.json(user_profiles_path)

    # Переглянемо структуру даних
    user_profiles_df.printSchema()

    # Трансформація даних з user_profiles: очищення та заповнення необхідних колонок
    user_profiles_cleaned = (
        user_profiles_df.withColumn("phone_number",
                                    regexp_replace(col("phone_number"), "[^0-9]", "")))

    # Define the output path where you want to save the cleaned data
    output_path_json = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\cleaned_customers.json"

    # Save the cleaned DataFrame to JSON format (overwriting any existing files)
    user_profiles_cleaned.write.mode("overwrite").json(output_path_json)
    print(f"Cleaned data saved to: {output_path_json}")
    # Закриваємо сесію Spark
    spark.stop()


process_user_profiles_task = PythonOperator(
    task_id='process_user_profiles',
    python_callable=process_user_profiles,
    dag=dag,
)

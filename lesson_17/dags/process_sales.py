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

def process_sales_data():
    # Шлях до вхідних та вихідних даних
    input_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\data\sales"
    output_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\bronze"

    # Створення Spark сесії
    spark = SparkSession.builder.appName("SalesProcessing").getOrCreate()

    # Перелік всіх файлів в папці
    files = os.listdir(input_path)

    # Обробка кожного файлу
    for file in files:
        if file.endswith(".csv"):
            # Отримання дати та номера розрізу
            date, split_number = file.split('__')[0], file.split('__')[1].split('.csv')[0]
            input_file_path = os.path.join(input_path, file)

            # Читання CSV файлу
            df = spark.read.csv(input_file_path, header=True, inferSchema=False)

            # Перетворюємо всі колонки в STRING
            df = df.select([col(c).cast("string").alias(c) for c in df.columns])

            # Шлях для збереження результатів
            output_file_path = os.path.join(output_path, date, f"{date}__{split_number}__sales.csv")

            # Записуємо дані в CSV в папку `bronze`
            df.write.option("header", "true").csv(output_file_path, mode="overwrite")
            print(f"Processed and saved: {output_file_path}")

    # Закриваємо сесію Spark
    spark.stop()

process_sales_task = PythonOperator(
    task_id='process_sales_to_bronze',
    python_callable=process_sales_data,
    dag=dag,
)

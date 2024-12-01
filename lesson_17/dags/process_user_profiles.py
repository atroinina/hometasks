from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

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
    # Шлях до вхідних та вихідних даних
    input_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\data\user_profiles"
    output_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver"

    # Створення Spark  сесії
    spark = SparkSession.builder.appName("UserProfileProcessing").getOrCreate()

    # Читання JSONLine фаqлів
    df = spark.read.json(input_path, multiLine=False)

    # Перетворення даних відповідно до схеми `silver`

    df_cleaned = df.withColumnRenamed("user_id", "client_id") \
        .withColumnRenamed("first_name", "first_name") \
        .withColumnRenamed("last_name", "last_name") \
        .withColumnRenamed("email", "email") \
        .withColumnRenamed("state", "state") \
        .withColumnRenamed("age", "age")

    # Зберігаємо дані
    df_cleaned.write.option("header", "true").json(output_path_silver, mode="overwrite")
    print(f"Processed and saved user profiles to silver: {output_path_silver}")

    # Закриваємо сесію Spark
    spark.stop()


process_user_profiles_task = PythonOperator(
    task_id='process_user_profiles',
    python_callable=process_user_profiles,
    dag=dag,
)

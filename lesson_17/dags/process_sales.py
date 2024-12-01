from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

# ініціалізація DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': days_ago(1),
    'catchup': False,
}

dag = DAG(
    'process_sales',
    default_args=default_args,
    description='Process sales data to bronze and silver',
    schedule_interval=None,
)

def process_sales_data():
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
        schedule_interval=None,
    )

    def process_customers_data():
        # Шлях до вхідних та вихідних даних
        input_path = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\data\customers"
        output_path_bronze = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\bronze"
        output_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver"

        # Створення Spark сесії
        spark = SparkSession.builder.appName("CustomerProcessing").getOrCreate()

        # Перелік всіх файлів в папці
        files = os.listdir(input_path)

        # Обробка кожного файлу
        for file in files:
            if file.endswith(".csv"):
                # Отримання дати з імені файлу (якщо є)
                date = file.split('__')[0]  # Дата, якщо файл має таку структуру
                input_file_path = os.path.join(input_path, file)

                # Читання CSV файлу
                df = spark.read.csv(input_file_path, header=True, inferSchema=False)

                # Перетворюємо всі колонки в STRING для бронзи
                df_bronze = df.select([col(c).cast("string").alias(c) for c in df.columns])

                # Шлях для збереження результатів у бронзу
                output_file_path_bronze = os.path.join(output_path_bronze, date, f"{date}__customers.csv")

                # Записуємо дані в CSV в папку `bronze`
                df_bronze.write.option("header", "true").csv(output_file_path_bronze, mode="overwrite")
                print(f"Processed and saved to bronze: {output_file_path_bronze}")

                # Трансформація даних для silver:
                # 1. Видалення рядків з порожніми значеннями в критичних колонках
                df_cleaned = df.dropna(
                    subset=['CustomerId', 'FirstName', 'LastName', 'Email', 'RegistrationDate', 'State'])

                # 2. Перейменування колонок згідно з правилами компанії
                df_cleaned = df_cleaned.withColumnRenamed("CustomerId", "client_id") \
                    .withColumnRenamed("FirstName", "first_name") \
                    .withColumnRenamed("LastName", "last_name") \
                    .withColumnRenamed("Email", "email") \
                    .withColumnRenamed("RegistrationDate", "registration_date") \
                    .withColumnRenamed("State", "state")

                # 3. Очищення пробілів на початку/в кінці значень
                df_cleaned = df_cleaned.select([trim(col(c)).alias(c) for c in df_cleaned.columns])

                # Шлях для збереження результатів у silver
                output_file_path_silver = os.path.join(output_path_silver, date, f"{date}__customers.csv")

                # Записуємо очищені дані в CSV в папку `silver`
                df_cleaned.write.option("header", "true").csv(output_file_path_silver, mode="overwrite")
                print(f"Processed and saved to silver: {output_file_path_silver}")

        # Закриваємо сесію Spark
        spark.stop()

    process_customers_task = PythonOperator(
        task_id='process_customers',
        python_callable=process_customers_data,
        dag=dag,
    )

process_customers_task = PythonOperator(
    task_id='process_sales',
    python_callable=process_sales_data,
    dag=dag,
)
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce

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
    # Шлях до даних `customers` та `user_profiles` на рівні silver
    customers_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\customers"
    user_profiles_path_silver = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\silver\user_profiles"
    output_path_gold = r"C:\Users\small\PycharmProjects\hometasks\lesson_17\output\gold"

    # Створення Spark сесії
    spark = SparkSession.builder.appName("EnrichCustomersToGold").getOrCreate()

    # Читання даних з `silver`
    df_customers = spark.read.json(customers_path_silver)
    df_user_profiles = spark.read.json(user_profiles_path_silver)

    # Об'єднуємо таблиці `customers` та `user_profiles` за `client_id`
    df_enriched = df_customers.join(df_user_profiles, on="client_id", how="left")

    # Заповнення відсутніх значень:
    # - Імена та прізвища з профілю користувача
    df_enriched = df_enriched.withColumn(
        "first_name", coalesce(col("first_name"), col("user_profiles.first_name"))
    ).withColumn(
        "last_name", coalesce(col("last_name"), col("user_profiles.last_name"))
    ).withColumn(
        "state", coalesce(col("state"), col("user_profiles.state"))
    )

    # Додаємо всі відсутні колонки з `user_profiles` в таблицю `customers`
    user_profile_columns = [col(c) for c in df_user_profiles.columns]
    df_enriched = df_enriched.select("*", *user_profile_columns)

    # Зберігаємо результат у `gold` як таблицю `user_profiles_enriched`
    df_enriched.write.option("header", "true").json(f"{output_path_gold}/user_profiles_enriched", mode="overwrite")
    # Закриваємо сесію Spark
    spark.stop()

process_user_profiles_to_gold_task = PythonOperator(
    task_id='enrich_user_profiles',
    python_callable=process_user_profiles_to_gold,
    dag=dag,
)

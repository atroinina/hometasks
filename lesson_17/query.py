from pyspark.sql import SparkSession
import os

os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + r'C:\hadoop\bin'

# Initialize Spark session
spark = SparkSession.builder.appName("EnrichCustomerData").getOrCreate()
sales_path = r"/lesson_17/output/silver/sales"
enriched_path = r"/lesson_17/output/silver/proccesed_user_profiles/enriched.json"

df_sales = spark.read.csv(sales_path, header=True, inferSchema=True)
df_enriched = spark.read.json(enriched_path)

df_tv = df_sales.filter(df_sales.product_name == 'TV')
df_joined = df_tv.join(df_enriched, df_tv.client_id == df_enriched.client_id, how="left")
print(df_joined.count())
df_joined.show()
#~14000
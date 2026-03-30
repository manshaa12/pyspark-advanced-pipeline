from pyspark.sql import SparkSession
from transform import run_pipeline

spark = SparkSession.builder \
    .appName("Advanced PySpark Pipeline") \
    .getOrCreate()

sales_df = spark.read.csv("data/sales.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

final_df = run_pipeline(sales_df, customers_df, products_df)

final_df.show(10)

final_df.write.mode("overwrite").parquet("output/final_data")

spark.stop()

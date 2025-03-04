from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('firstnameendingwutha').getOrCreate()
workers_df = worker.filter(col("first_name").like('%a'))
workers_df.show()
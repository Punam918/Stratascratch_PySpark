from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('departmentcontainingfirstnamea').getOrCreate()
workers_df = worker.filter(col("first_name") == "a")
workers_df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('SortWorkers').getOrCreate()
sorted_df = worker.orderBy("first_name", col("department").desc())
sorted_df.show()
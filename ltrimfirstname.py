from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('whitespaceremoved').getOrCreate()

result_df = worker_ws.select(ltrim("first_name").alias("trimmed_first_name"))

result_df.show()

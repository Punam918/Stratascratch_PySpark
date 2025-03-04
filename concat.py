from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('replaceawithA').getOrCreate()

result_df = worker.select(concat_ws(" ", "first_name", "last_name").alias("full_name"))

result_df.show()
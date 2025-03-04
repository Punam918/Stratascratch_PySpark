from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('replaceawithA').getOrCreate()

result_df = worker.select(regexp_replace("first_name", "a", "A").alias("modified_first_name"))

result_df.show()

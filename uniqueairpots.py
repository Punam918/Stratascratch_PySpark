from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('CountUniqueairpot').getOrCreate()

data = us_flights.select(countDistinct("origin").alias("num_origin_airports"))
data.show()
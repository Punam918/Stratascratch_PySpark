from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('Highest Market Value').getOrCreate()

data = forbes_global_2010_2014.groupBy("sector").agg(max("marketvalue").alias("highest_market_value")).orderBy(col("highest_market_value").desc())
data.show()
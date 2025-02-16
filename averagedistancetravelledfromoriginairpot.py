from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('Average Distance Travelled').getOrCreate()
data = us_flights.groupBy('origin').agg(avg('distance')).distinct()
data.show()
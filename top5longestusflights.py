from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Top 5 longest US flights').getOrCreate()

top_5_longest_flights = (
    us_flights.select("origin", "dest", "distance")
    .orderBy(col("distance").desc())
    .limit(5)
)
top_5_longest_flights.show(truncate=False)
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Find unique IATA codes').getOrCreate()
unique_origins = us_flights.select("origin").distinct().orderBy("origin")
unique_origins.show(truncate=False)
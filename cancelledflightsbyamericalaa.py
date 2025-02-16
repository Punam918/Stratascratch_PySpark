from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('NumberOfCancelledFlights').getOrCreate()
num_cancelled_flights = us_flights.filter(col('cancelled') != 0).count()
print(f"Number of cancelled flights: {num_cancelled_flights}")
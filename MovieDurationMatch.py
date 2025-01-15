# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Create a Spark session
spark = SparkSession.builder.appName("Movie Duration Match") .getOrCreate()
# filtering for flight_id =101
flight_schedule = flight_schedule.filter(col("flight_id") == 101)
# Join and filter movies by flight duration
result = flight_schedule.join(
    entertainment_catalog,
    entertainment_catalog["duration"] <= flight_schedule["flight_duration"]
).select(
    flight_schedule["flight_id"].alias("flight_id"),
    entertainment_catalog["movie_id"].alias("movie_id"),
    entertainment_catalog["duration"].alias("movie_duration")
)
result.show()
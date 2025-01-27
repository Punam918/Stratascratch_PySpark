# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Session Type Duration").getOrCreate()

# Calculate session duration in seconds
df = twitch_sessions.withColumn("session_duration", col("session_end") - col("session_start"))



# Calculate the average session duration for each session type
result = df.groupBy("session_type") \
           .agg(avg("session_duration").alias("average_session_duration_seconds"))

# Show the result
result.show()
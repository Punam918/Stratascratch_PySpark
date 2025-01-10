# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Aggregate Listening Data").getOrCreate()
# Calculate total listening time (in minutes) and count unique songs for each user
result = listening_habits.groupBy("user_id").agg(
    round(sum(col("listen_duration")) / 60).alias("total_listen_duration"), 
    countDistinct("song_id").alias("unique_song_count")                       
)

# To validate your solution, convert your final pySpark df to a pandas df
result_df = result.toPandas()
print(result_df)
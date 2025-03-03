from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('avgInspection').getOrCreate()

df = los_angeles_restaurant_health_inspections

filtered_df = df.filter((col("score") >= 91) & (col("score") <= 100))

mean_score = filtered_df.agg(mean("score").alias("mean_score")).collect()

print(f"The mean of inspection scores between 91 and 100 is: {mean_score[0]['mean_score']}")
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Average_Scores_for_grade_A_B_C').getOrCreate()

data = los_angeles_restaurant_health_inspections.filter(col("grade").isin("A", "B", "C")) 
datad = data.groupBy("grade").agg(avg("score").alias("avg_score"))
datad.show()
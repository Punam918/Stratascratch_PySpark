from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Uniquefaciltyandinspection').getOrCreate()

df = los_angeles_restaurant_health_inspections
bakeries_df = df.filter(col("owner_name") == "BAKERY")
grade_counts = bakeries_df.groupBy("grade").agg(count("grade").alias("count"))

most_common_grade = grade_counts.orderBy(col("count").desc()).first()
print(most_common_grade)

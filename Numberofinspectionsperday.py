from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Inspectionperday').getOrCreate()

df = los_angeles_restaurant_health_inspections

inspections_per_day = df.groupBy("activity_date") \
    .agg(count("serial_number").alias("num_inspections")) \
    .orderBy("activity_date") 

inspections_per_day.show()

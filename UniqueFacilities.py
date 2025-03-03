from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Uniquefaciltyandinspection').getOrCreate()
df = los_angeles_restaurant_health_inspections

result_df = df.groupBy("facility_zip") \
    .agg(
        countDistinct("facility_id").alias("unique_facilities"),
        count("serial_number").alias("num_inspections")
    ) \
    .orderBy(col("num_inspections").desc())

result_df.show()
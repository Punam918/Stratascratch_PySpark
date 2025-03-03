from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('CheckingUniqueRecordID').getOrCreate()
df = los_angeles_restaurant_health_inspections

filtered_df = df.filter((col("owner_name") == "BAKERY") & (col("pe_description") == "Low Risk"))


result_df = filtered_df.select("owner_name", "pe_description")

result_df.show(truncate=False)
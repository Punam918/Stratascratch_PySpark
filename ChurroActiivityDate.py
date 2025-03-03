from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Churro Activity Date').getOrCreate()

data = los_angeles_restaurant_health_inspections.filter((col('facility_name')=='STREET CHURROS') & (col("score") < 95))
result_df = data.select("activity_date", "pe_description")
result_df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Facility zip Codes').getOrCreate()

data =  los_angeles_restaurant_health_inspections.filter(col("facility_zip").isin("90049", "90034", "90045"))
data.show()
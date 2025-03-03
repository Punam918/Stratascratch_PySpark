from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Inspection For Coffee').getOrCreate()

data =  los_angeles_restaurant_health_inspections.filter(col("owner_name") == "GLASSELL COFFEE SHOP LLC")
data.show()
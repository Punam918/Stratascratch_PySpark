from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('Routine_Inspections').getOrCreate()

data = los_angeles_restaurant_health_inspections.filter((col("program_name") == "Routine") & (col("pe_description") == "High Risk"))

data.show()
# Import your libraries
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("Inspections") .getOrCreate()

data = los_angeles_restaurant_health_inspections.filter(col("program_status")=="INACTIVE")


# Start writing code
datad = data.select("serial_number","activity_date","facility_name","score","grade","program_status").orderBy("serial_number")
datad.show()
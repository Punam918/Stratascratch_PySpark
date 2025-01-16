# Import your libraries
# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
# Create a Spark session
spark = SparkSession.builder.appName("Finding Doctors") .getOrCreate()
#filtering Johnson
employee_list = employee_list.filter((col("last_name") == "Johnson") & (col("profession") == "Doctor"))
#selecting
employee_list = employee_list.select('first_name', "last_name","profession")
employee_list.show()

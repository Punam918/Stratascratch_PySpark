from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('Top 5 longest US flights').getOrCreate()
data = us_flights.filter(col('arr_delay')<=0)
datad = data.select('flight_date',	'unique_carrier',	'flight_num')
datad.show()
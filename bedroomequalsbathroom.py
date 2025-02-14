from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('find all searches').getOrCreate()

data = airbnb_search_details.filter(col("bedrooms") == col("bathrooms"))
data.show()
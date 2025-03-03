from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Cheapest Properties').getOrCreate()

data =  airbnb_search_details.groupBy('city').agg(min(col('price')))

data.show()
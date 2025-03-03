from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('All_Neighborhoods').getOrCreate()
data =  airbnb_search_details.select("neighbourhood").distinct()
data.show()
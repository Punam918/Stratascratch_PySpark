from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('HighestMarketValue').getOrCreate()

data =  forbes_global_2010_2014.groupBy('sector').agg(max('marketvalue').alias('Highest_market_value'))
data.show()
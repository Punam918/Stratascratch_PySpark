from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('HighestMarketValue').getOrCreate()

data = forbes_global_2010_2014.groupBy('industry').agg(avg('profits').alias("averageprofit"), min("sales").alias("lowest_sale"))

data = data.filter(col('averageprofit') > 0)

datad = data.select('industry', 'averageprofit', 'lowest_sale').orderBy(col('lowest_sale').asc())

datad.show()
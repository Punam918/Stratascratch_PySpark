from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Mostprofitablecompany').getOrCreate()

data = forbes_global_2010_2014.filter(col('sector')=='Financials')

most_profitable = data.orderBy(col("profits").desc()).select("company", "profits", "continent").limit(5)

# Show result
most_profitable.show()
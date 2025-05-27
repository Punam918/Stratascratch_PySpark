from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('NumberOfUscompanies').getOrCreate()
data = forbes_global_2010_2014.filter(col('country')=='United States').count()
print(data)
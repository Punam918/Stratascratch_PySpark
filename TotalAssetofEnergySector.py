from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('totalassetssofenergysector').getOrCreate()

data = forbes_global_2010_2014.filter(col('sector')=='Energy').agg(sum("assets").alias("total_assets"))
data.show()
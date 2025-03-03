from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('profittosalesratio').getOrCreate()

df = forbes_global_2010_2014

result_df = df.filter(col("company") == "Royal Dutch Shell")
data = result_df.select(col("company"), (col("profits") / col("sales")).alias("profit_to_sales_ratio"))

data.show()
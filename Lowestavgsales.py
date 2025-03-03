from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('Industrywithlowestaveragesales').getOrCreate()

df = forbes_global_2010_2014
industry_sales_df = df.groupBy("industry") .agg(avg("sales").alias("avg_sales"))

lowest_avg_sales_df = industry_sales_df.orderBy(col("avg_sales").asc()).limit(1)
lowest_avg_sales_df.show()
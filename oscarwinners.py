from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('DetailsOfOscarWinner').getOrCreate()

data = oscar_nominees.filter((col("year") >= 2001) & (col("year") <= 2009) & (col("winner") == True))

data.show()
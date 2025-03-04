from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Companieswithmorethan10employees').getOrCreate()

data = google_adwords_earnings.filter(col("n_employees") > 10)
data.show()
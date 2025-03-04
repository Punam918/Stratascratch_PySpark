from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('BusinessType').getOrCreate()

data = google_adwords_earnings.select(col('business_type')).distinct()
data.show()
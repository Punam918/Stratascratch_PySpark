from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('DraftcontainingOptimism').getOrCreate()

data =  google_file_store.filter(col("contents").like("%optimism%"))
data.show()
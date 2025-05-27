from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('FirstThreecharacters').getOrCreate()
data = worker.select(substring("first_name", 1, 3).alias("first_three_chars"))
data.show()
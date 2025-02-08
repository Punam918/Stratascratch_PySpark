from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("find non-hs sat scores").getOrCreate()

data = sat_scores.filter(~col("school").endswith("HS"))

datare = data.select('average_sat','school','student_id')
datare.show()
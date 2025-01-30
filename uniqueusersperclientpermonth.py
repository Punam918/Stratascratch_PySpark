from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("Unique Users Per Client").getOrCreate()

monthdata = fact_events.withColumn("month",month(fact_events["time_id"]))

data = monthdata.groupBy("client_id", "month").agg(countDistinct("user_id").alias("unique_users"))


data.show()



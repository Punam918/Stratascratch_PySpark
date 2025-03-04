from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Positionoflettera').getOrCreate()
result_df = worker.filter(worker.first_name == "Amitah") \
                  .select(instr("first_name", "a").alias("position_of_a"))

result_df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Third Heaviest Shipment").getOrCreate()

# Compute total weight for each shipment_id
df_weight = amazon_shipment.groupBy("shipment_id").agg(sum("weight").alias("total_weight"))

# Define window specification to rank shipments by weight (descending)
window_spec = Window.orderBy(col("total_weight").desc())

# Assign rank based on total weight using dense_rank()
df_ranked = df_weight.withColumn("rank", dense_rank().over(window_spec))

# Filter for the third heaviest shipment
df_result = df_ranked.filter(col("rank") == 3).select("shipment_id", "total_weight")

# Show the result
df_result.show()
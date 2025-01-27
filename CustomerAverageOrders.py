# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Customer Average Orders").getOrCreate()


# Calculate the number of unique customers and the average order amount
result = postmates_orders.agg(
    countDistinct("customer_id").alias("unique_customers"),
    avg("amount").alias("average_order_amount")
)

# Show the result
result.show()

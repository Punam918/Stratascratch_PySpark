# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Weekly Orders Report").getOrCreate()

# Filter for first quarter of 2023
filtered_data = orders_analysis.filter(
    (col("week") >= "2023-01-01") & (col("week") <= "2023-03-31")
)

# Group by week and calculate total quantity
result = (
    filtered_data.groupBy("week")
    .agg(sum("quantity").alias("total_quantity"))
    .orderBy("week")
)
# To validate your solution, convert your final pySpark df to a pandas df
reportorders = result.toPandas()
print(reportorders)
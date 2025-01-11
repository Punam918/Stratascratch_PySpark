from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize SparkSession
spark = SparkSession.builder.appName("Top Monthly Sellers").getOrCreate()

# Assuming sales_data is already loaded, filter for January 2024
january_data = sales_data.filter((col("sales_date") >= "2024-01-01") & (col("sales_date") <= "2024-01-31"))

# Define a window partitioned by product_category and ordered by total_sales in descending order
window_spec = Window.partitionBy("product_category").orderBy(col("total_sales").desc())

# Add a rank column to determine the ranking of sellers within each category
ranked_data = january_data.withColumn("rank", rank().over(window_spec))

# Filter the top 3 sellers in each product category
top_3_sellers = ranked_data.filter(col("rank") <= 3)

# Select the required columns
result = top_3_sellers.select("seller_id", "total_sales", "product_category", "market_place", "sales_date")

# Show the result
result.show()

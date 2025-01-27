from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_date

# Initialize Spark session
spark = SparkSession.builder.appName("Inactive Free Users").getOrCreate()

# Join the rc_calls and rc_users tables
joined_df = rc_calls.join(rc_users, on="user_id", how="inner")

# Filter for free users with calls NOT in April 2020
filtered_df = joined_df.filter(
    (col("status") == "free") & 
    (month(to_date(col("date"))) != 4) & 
    (year(to_date(col("date"))) != 2020)
)

# Show the result
filtered_df.show()

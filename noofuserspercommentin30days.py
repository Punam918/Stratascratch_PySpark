from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Number of Comments").getOrCreate()

# Filter data for the last 30 days before 2020-02-10
filtered_df = fb_comments_count.filter(
    (col("created_at") >= "2020-01-11") & 
    (col("created_at") < "2020-02-10") & 
    (col("number_of_comments") > 0)
)

# Aggregate the total number of comments per user
result_df = (
    filtered_df.groupBy("user_id")
    .agg(sum("number_of_comments").alias("total_comments"))
)

# Show the result
result_df.show()
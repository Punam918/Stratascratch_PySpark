from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("User Activity Per Month Day").getOrCreate()

# Extract the day of the month from post_date and calculate the distribution
result_df = (
    facebook_posts.groupBy(dayofmonth("post_date").alias("day_of_month"))
    .agg(count("post_id").alias("number_of_posts"))
    .orderBy("day_of_month")
)

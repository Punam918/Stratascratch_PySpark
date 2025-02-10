from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('User Growth Rate').getOrCreate()

# Filter for December 2020
dec_users = (
    sf_events.filter((col("date") >= "2020-12-01") & (col("date") <= "2020-12-31"))
    .groupBy("account_id")
    .agg(countDistinct("user_id").alias("dec_users"))
)

# Filter for January 2021
jan_users = (
    sf_events.filter((col("date") >= "2021-01-01") & (col("date") <= "2021-01-31"))
    .groupBy("account_id")
    .agg(countDistinct("user_id").alias("jan_users"))
)

# Join December and January user counts on account_id
growth_df = (
    jan_users.join(dec_users, "account_id", "left_outer")
    .withColumn("growth_rate", col("jan_users") / col("dec_users"))
    .select("account_id", "growth_rate")
)

growth_df.show()
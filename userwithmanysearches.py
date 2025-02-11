from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('users with many searches').getOrCreate()
data = fb_searches.filter((col("date") >= '2021-08-01') & (col("date") <= '2021-08-31'))
user_search_counts = data.groupBy('user_id').agg(count('search_id').alias('search_count'))
users_with_many_searches = user_search_counts.filter(col('search_count') > 5)
num_users = users_with_many_searches.count()
print(f"Number of users who made more than 5 searches in August 2021: {num_users}")
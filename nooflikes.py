from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Post Likes').getOrCreate()

data = fb_posts.groupBy(col("post_id")).agg(avg(col("no_of_likes"))).alias("avg_likes")

data.show()
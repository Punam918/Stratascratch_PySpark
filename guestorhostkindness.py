from pyspark.sql.functions import *
from pyspark.sql import SparkSession

avg_scores = airbnb_reviews.groupBy("from_type") \
    .agg(round(avg("review_score"), 2).alias("avg_review_score"))

avg_scores.show()

max_score = avg_scores.agg({"avg_review_score": "max"}).collect()[0][0]
print(f"Higher average review score: {max_score}")
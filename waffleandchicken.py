from pyspark.sql.functions import col, count
filtered_df = yelp_reviews.filter(col("business_name") == "Lo-Lo's Chicken & Waffles")
result_df = filtered_df.groupBy("stars").agg(count("*").alias("review_count"))
result_df = result_df.orderBy("stars")
result_df.show()

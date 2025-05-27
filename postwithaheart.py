from pyspark.sql.functions import *
heart_reactions_df = facebook_reactions.filter(col("reaction") == "heart")
joined_df = heart_reactions_df.join(facebook_posts, on="post_id", how="inner")
result_df = joined_df.select(facebook_posts["*"])
unique_result_df = result_df.dropDuplicates(["post_id"])
unique_result_df.show()

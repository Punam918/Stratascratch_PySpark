# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,countDistinct, when,lit

spark = SparkSession.builder.appName("User Activity Count").getOrCreate()

joined_df =user_profiles.join(activity_log,on = 'user_id',how = 'left')

# Count distinct activity types for each user
activity_count = joined_df.groupBy("user_id").agg(
    countDistinct("activity_type").alias("unique_activity_count")
)

# Replace null counts with zero for users with no activities
result = activity_count.withColumn(
    "unique_activity_count",
    when(col("unique_activity_count").isNull(), lit(0)).otherwise(col("unique_activity_count"))
)
result_df = result.toPandas()
print(result_df)
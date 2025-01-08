from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Initialize Spark session
spark = SparkSession.builder.appName("Average Score Calculation").getOrCreate()

result = (
    project_data.groupBy("project_id")
    .agg(
        count("team_member_id").alias("member_count"),
        avg("score").alias("average_score")
    )
    .filter(col("member_count") > 1)
    .select("project_id", "average_score","member_count")
)

# Convert the result to Pandas DataFrame for validation or display
result_df = result.toPandas()

# Show the result
print(result_df)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, row_number
from pyspark.sql.window import Window
# Create a Spark session
spark = SparkSession.builder.appName("Peak Online Time") .getOrCreate()

# Create time_period column by concatenating start_timestamp and end_timestamp
user_activity_df = user_activity.withColumn(
    "time_period",
    concat_ws(" to ", col("start_timestamp"), col("end_timestamp"))
)

# Define a window partitioned by device_type and ordered by user_count descending
window_spec = Window.partitionBy("device_type").orderBy(col("user_count").desc())

# Add a row number to rank user_count for each device_type
ranked_df = user_activity_df.withColumn("rank", row_number().over(window_spec))

# Filter to get only the highest user_count for each device_type
result_df = ranked_df.filter(col("rank") == 1).select("user_count", "time_period", "device_type")

# Show the result
result_df.show(truncate=False)
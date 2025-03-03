from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName('CountcertainLanguage').getOrCreate()

df = playbook_users.filter(col("language").isin(["English", "German", "French", "Spanish"]))

unique_users_count = df.select("user_id").distinct().count()

print(f"Number of unique users speaking English, German, French, or Spanish: {unique_users_count}")

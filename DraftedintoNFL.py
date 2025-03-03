from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("Drafted_Into_NFL").getOrCreate()
df = nfl_combine

drafted_athletes_count = (df.filter((col("year") == 2013) & (col("pickround") > 0)).count())

print(f"Number of athletes drafted into the NFL from the 2013 Combine: {drafted_athletes_count}")
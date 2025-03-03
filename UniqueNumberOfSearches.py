from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("No_of_UsersdoingaSearch").getOrCreate()
df = airbnb_searches
unique_users_count = df.select("id_user").distinct().count()
print(f"Number of unique users who performed a search: {unique_users_count}")
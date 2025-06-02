from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark  = SparkSession.builder.appName('fb_active_users').getOrCreate()

df_us = fb_active_users.filter((col('country')=='USA')& (col('status')=='open'))
total_count = fb_active_users.count()
count_df = df_us.count()

# us_open_percentage = round((count_df / total_count) * 100, 2)

if total_count > 0:
    us_open_percentage = round((count_df / total_count) * 100, 2)
else:
    us_open_percentage = 0.0
print("Percentage of users from USA with 'open' status:", us_open_percentage, "%")




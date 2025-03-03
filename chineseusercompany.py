from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('ChineseUserCompany').getOrCreate()

df = playbook_users
chinese_speaking_df = df.filter(col("language") == "Chinese")
company_chinese_users_df = chinese_speaking_df.groupBy("company_id") \
                                              .agg(count("user_id").alias("chinese_speaking_users"))

# Filter companies with at least 2 Chinese-speaking users
result_df = company_chinese_users_df.filter(col("chinese_speaking_users") >= 2)

# Show the result
result_df.show()
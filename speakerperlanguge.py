from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

Spark = SparkSession.builder.appName(' playbook_users').getOrCreate()

df = playbook_users.groupBy('language').agg(count('user_id').alias('usercount'))
df = df.orderBy(col('usercount').desc())
df.show()
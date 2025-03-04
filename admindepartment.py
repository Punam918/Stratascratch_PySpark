from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('detailsadmindepartment').getOrCreate()
admin_workers_df = worker_ws.filter(col("department") == "Admin")

admin_workers_df.show()
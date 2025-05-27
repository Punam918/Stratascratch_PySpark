from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

sub = SparkSession.builder.appName('submissions').getOrCreate()

rate_type_window = Window.partitionBy("rate_type")

df_with_total = submissions.withColumn("total_balance_by_rate_type", sum("balance").over(rate_type_window))

result_df = df_with_total.withColumn(
    "balance_percentage",
    round(100 * col("balance") / col("total_balance_by_rate_type"), 2)
).select(
    "rate_type", 
    "loan_id", 
    col("balance").alias("loan_balance"),
    "balance_percentage"
).orderBy("rate_type", "loan_id")

result_df.show()

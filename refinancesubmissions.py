from pyspark.sql.functions import col, row_number, sum as _sum
from pyspark.sql.window import Window

loans_df = loans.alias("loans")
subs_df = submissions.alias("subs")

joined_df = subs_df.join(loans_df, col("subs.loan_id") == col("loans.id"))

refinance_df = joined_df.filter(col("loans.type") == "Refinance")

window_spec = Window.partitionBy("loans.user_id").orderBy(col("subs.id").desc())

latest_subs = refinance_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1)

result_df = latest_subs.groupBy(col("loans.user_id")).agg(_sum("subs.balance").alias("total_balance"))

result_df.show()

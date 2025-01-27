from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Paid Users In April 2020").getOrCreate()

data = rc_calls.join(rc_users, on = "user_id",how = "inner")
 
# Filter for paid users and calls in April 2020
filtered_df = data.filter(
    (col("status") == "paid") & 
    (month(to_date(col("date"))) == 4) & 
    (year(to_date(col("date"))) == 2020)
)

filtered_df.show()
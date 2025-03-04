from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('firstnamewith6lettersendingwithh').getOrCreate()

filtered_df = worker.filter(
    (length("first_name") == 6) & (col("first_name").like("%h"))
)

filtered_df.show()
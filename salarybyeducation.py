from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Salary by Education").getOrCreate()

data = google_salaries.groupby(col("education")).agg(avg(col("salary"))).alias("averagesalary")

data.show()
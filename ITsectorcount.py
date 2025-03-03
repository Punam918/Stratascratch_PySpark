from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("ITcompanyCount").getOrCreate()

datad = forbes_global_2010_2014.filter(col("sector") == "Information Technology")
data = datad.groupBy("country").agg(count("company").alias("num_companies"))
data = data.orderBy(col("num_companies").desc())
data.show()
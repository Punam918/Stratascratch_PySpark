from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("Avg_Searches").getOrCreate()
df = airbnb_searches.groupBy(col('id_user')).agg(avg(col('n_searches')).alias("avg_searces"))
df.show()

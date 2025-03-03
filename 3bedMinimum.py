from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("AirbnbAnalysis").getOrCreate()
df = airbnb_search_details

neighbourhood_beds = (
    df.groupBy("neighbourhood")
    .agg(
        avg("beds").alias("avg_beds"), 
        sum("beds").alias("total_beds")
    )
    .filter(col("total_beds") >= 3)  
    .orderBy(col("avg_beds").desc()) 
)

neighbourhood_beds.select("neighbourhood", "avg_beds").show(truncate=False)
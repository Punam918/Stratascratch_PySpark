from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, count

spark = SparkSession.builder.appName("RoxanneCafeViolations").getOrCreate()
df = sf_restaurant_health_violations

roxanne_cafe_violations = df.filter(col("business_name") == "Roxanne Cafe") \
                            .withColumn("year", year(col("inspection_date"))) \
                            .groupBy("year") \
                            .agg(count("violation_id").alias("total_violations")) \
                            .orderBy("year") 

roxanne_cafe_violations.show(truncate=False)

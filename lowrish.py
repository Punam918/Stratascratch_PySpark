from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("LowRiskViolations").getOrCreate()

df = sf_restaurant_health_violations
low_risk_businesses = df.filter(col("risk_category") == "Low Risk")

low_risk_businesses.show()
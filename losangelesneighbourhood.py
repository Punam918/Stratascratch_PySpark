from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Los Angeles").getOrCreate()

# Assuming airbnb_search_details is already a DataFrame
# Correcting column selection and filter condition
datad = airbnb_search_details.filter(col("city") == "Los Angeles")  # Fix column reference

# Select distinct neighborhoods
data = datad.select("neighbourhood").distinct()  # Select only necessary column

# Show results
data.show(truncate=False)

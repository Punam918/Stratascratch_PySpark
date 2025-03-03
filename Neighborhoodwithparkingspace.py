from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, col

spark = SparkSession.builder.appName("Properties_With_Parking_Space").getOrCreate()
df = airbnb_search_details

neighbourhoods_with_parking = (
    df.filter(
        (col("amenities").contains("parking")) & (col("cleaning_fee") == False))
    .select("neighbourhood")
    .distinct() 
)

neighbourhoods_with_parking.show(truncate=False)

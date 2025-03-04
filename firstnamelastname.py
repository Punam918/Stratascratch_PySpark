from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('namechaiyekokhojni').getOrCreate()
filtered_df = worker.filter(
    (~col("first_name").isin("Vipul", "Satish")) & (~col("last_name").contains("c"))
)
filtered_df.show()
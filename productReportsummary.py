from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("products report summary")

# Join transactions with products on product_id
df = wfm_transactions.join(wfm_products, "product_id")
# Filter transactions for the year 2017
df_2017 = df.filter(year(col("transaction_date")) == 2017)
# Aggregate number of unique transactions and total sales for each product category
result = df_2017.groupBy("product_category") \
                .agg(countDistinct("transaction_id").alias("num_transactions"),
                     sum("sales").alias("total_sales")) \
                .orderBy(col("total_sales").desc())

# Show the result
result.show()
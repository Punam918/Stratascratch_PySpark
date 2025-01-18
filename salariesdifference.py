# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Salaries Differences") .getOrCreate()
# Join two dataframe
# Join employee and department tables
joined_df = db_employee.join(db_dept, db_employee.department_id == db_dept.id)

# Filter for marketing and engineering departments
filtered_df = joined_df.filter(col("department").isin("marketing", "engineering"))

# Get the maximum salary for each department
max_salaries = filtered_df.groupBy("department").agg(max("salary").alias("max_salary"))

# Pivot the result to separate columns for marketing and engineering
pivoted_salaries = max_salaries.groupBy().pivot("department").max("max_salary")

# Calculate the absolute difference
result = pivoted_salaries.withColumn(
    "absolute_difference",
    abs(col("marketing") - col("engineering"))
)

result.show()
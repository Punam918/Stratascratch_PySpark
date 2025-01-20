# Import your libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Salaries Differences") .getOrCreate()

#  Define a window specification
window_spec = Window.partitionBy("id").orderBy(col("salary").desc())

# Add a rank column to identify the latest salary record for each employee
df_with_rank = ms_employee_salary.withColumn("rank", row_number().over(window_spec))

# Step 5: Filter to keep only the rows with rank = 1 (latest salary)
current_salaries = df_with_rank.filter(col("rank") == 1)

# Step 6: Select the required columns and sort by employee ID
result = current_salaries.select(
    "id", "first_name", "last_name", "department_id", "salary"
).orderBy("id")

# Step 7: Show the result
result.show()
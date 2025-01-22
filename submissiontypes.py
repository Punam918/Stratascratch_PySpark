# Import necessary libraries
from pyspark.sql.functions import col

# Assuming `loans` DataFrame is already created and available
# Filter for 'Refinance' and 'InSchool' submissions
refinance_df = loans.filter(col("type") == "Refinance").select("user_id").distinct()
inschool_df = loans.filter(col("type") == "InSchool").select("user_id").distinct()

# Find users present in both DataFrames
result_df = refinance_df.join(inschool_df, on="user_id", how="inner")

# Show the result
result_df.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
joined_df = facebook_employees.join(facebook_hack_survey, facebook_employees.id == facebook_hack_survey.employee_id)
result_df = joined_df.groupBy("location").agg(avg("popularity").alias("average_popularity"))
result_df.show()
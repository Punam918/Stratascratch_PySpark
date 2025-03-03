from pyspark.sql import SparkSession
from pyspark.sql.functions import *

Spark = SparkSession.builder.appName('CheckingUniqueRecordID').getOrCreate()
totalcount =  los_angeles_restaurant_health_inspections.count()
unique_record_ids =  los_angeles_restaurant_health_inspections.select("record_id").distinct().count()
print(f"Total record ids: {totalcount}")
print(f"Total unique record ids: {unique_record_ids}")
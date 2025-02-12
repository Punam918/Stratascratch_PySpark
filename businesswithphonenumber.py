from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession 

spark = SparkSession.builder.appName('All business with phone number').getOrCreate()

data =  sf_restaurant_health_violations.filter(col('business_phone_number').isNotNull())
datad = data.select('business_id','business_name','business_phone_number')
datad.show()
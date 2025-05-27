from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.appName('LetterStartingwithg').getOrCreate()
data =  google_word_lists.filter((col("words1").like("g%")) |  (col("words2").like("g%")))
data.show()
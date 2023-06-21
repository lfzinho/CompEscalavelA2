# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder\
    .appName("Datacamp Pyspark Tutorial")\
    .config("spark.memory.offHeap.enabled","true")\
    .config("spark.memory.offHeap.size","10g")\
    .getOrCreate()

df = spark.read.csv('spark_tests\datacamp_ecommerce.csv',header=True,escape="\"")

df.show(5,0)

print(df.count())

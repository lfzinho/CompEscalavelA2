import redis
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RedisSparkIntegration") \
    .getOrCreate()


def read_data_from_redis(key):
    redis_client = redis.Redis(host='localhost', port=6379)
    return redis_client.get(key).decode('utf-8')

key = ''
data = read_data_from_redis(key)

# Create a Spark DataFrame from the Redis data
df = spark.createDataFrame([(data,)], ["data"])

# Perform further transformations or analysis on the DataFrame
df.show()

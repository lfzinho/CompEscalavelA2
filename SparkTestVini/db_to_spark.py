import redis
from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.functions import split

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class Reader:

    def __init__(self) -> None:
        self.spark =  SparkSession.builder \
            .appName("RedisSparkIntegration") \
            .getOrCreate()
        
    
    def read_data_from_redis(self):
        redis_client = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=1,
            decode_responses = True,
        )

        data = []

        for key in redis_client.keys("*"):
            value = redis_client.get(key)
            data.append((key, value))

        return data

    def get_all_data(self):
        redis_client = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=1,
            decode_responses = True,
        )
        all_keys = redis_client.keys('*')
        for key in all_keys:
            value = redis_client.get(key)
            print(f"Key: {key} Value: {value}")
        #     all_data[key] = value
        # return all_data

    def get_df(self):
        data = self.read_data_from_redis()
        df = self.spark.createDataFrame(data, ["key", "value"])
        df = df.withColumn('time', split(df['key'], ' ').getItem(0)) \
            .withColumn('car_plate', split(df['key'], ' ').getItem(1)) \
            .withColumn('road_name', split(df['value'], ' ').getItem(0)) \
            .withColumn('car_lane', split(df['value'], ' ').getItem(1)) \
            .withColumn('car_length', split(df['value'], ' ').getItem(2)) \

        # df = self.spark.createDataFrame([(data[b"time"], data[b"road_name"], data[b"car_plate"], data[b"car_lane"], data[b"car_length"],)], ["time", "road_name", "car_plate", "car_lane", "car_length",])
        df.show(n=5)

r = Reader()
# r.get_all_data()
r.get_df()
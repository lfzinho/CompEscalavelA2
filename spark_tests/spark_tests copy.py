# Import SparkSession from pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import redis

spark = SparkSession.builder\
    .appName("Exemplo Redis") \
    .config("spark.redis.host", "10.22.160.187") \
    .config("spark.redis.port", "6381") \
    .config("spark.redis.auth", "1234") \
    .config("spark.redis.db", "0") \
    .getOrCreate()


r = redis.Redis(
    host='10.22.160.187',
    port=6381,
    password='1234',
    db=0,
    decode_responses=True,
)

# Obtenha todas as chaves de hash do Redis
keys = r.keys("*")

# Crie um RDD do Spark com as chaves
rdd = spark.sparkContext.parallelize(keys)

# Ler os dados de cada chave de hash no Redis
data_rdd = rdd.map(lambda key: {
    'key': key.decode(),
    'data': r.hgetall(key)
})

# Criar um DataFrame a partir do RDD
df = spark.createDataFrame(data_rdd)

# Exibir os dados lidos
df.show()

# # df = spark.read.csv('spark_tests\datacamp_ecommerce.csv',header=True,escape="\"")
# df = spark.read \
#     .format("org.apache.spark.sql.redis") \
#     .option("table", "*") \
#     .option("key.column", "datetime:plate") \
#     .option("key.pattern", "*") \
#     .load()

# df.show(5,0)

# print(df.count())

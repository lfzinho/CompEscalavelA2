import os
import sys
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import count
from pyspark.sql.functions import col
from pyspark.sql.functions import lag, expr
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class Transformer:

    def __init__(self) -> None:
        # simply creates the spark session
        self.spark =  SparkSession.builder \
            .appName("RedisSparkIntegration") \
            .getOrCreate()
        
        # Get roads' data
        self.roads_data = self.spark.read.csv('./Simulator/world.txt', sep=" ", header=False)
        self.roads_data.show()

    def read_data_from_redis(self):
        # connects to redis
        redis_client = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=1,
            decode_responses = True,
        )

        # creates a container for tuples of (data, value)
        data = []

        # creates a tuple for every tuple
        for key in redis_client.keys("*"):
            value = redis_client.get(key)
            data.append((key, value))

        return data

    def get_df(self):
        # gets the data
        data = self.read_data_from_redis()

        # create a spark dataframe with coumns key and value
        self.df = self.spark.createDataFrame(data, ["key", "value"])

        # now splits the columns key into time and plate and laue into road, lane and length
        self.df = self.df.withColumn('time', split(self.df['key'], ' ').getItem(0)) \
            .withColumn('car_plate', split(self.df['key'], ' ').getItem(1)) \
            .withColumn('road_name', split(self.df['value'], ' ').getItem(0)) \
            .withColumn('car_lane', split(self.df['value'], ' ').getItem(1)) \
            .withColumn('car_length', split(self.df['value'], ' ').getItem(2))

    def base_transform(self):
        pass

    def individual_analysis(self):
        self.add_analysis1()
        self.add_analysis2()
        self.add_analysis3()
        self.add_analysis4()
        self.add_analysis5()
        self.add_analysis6()

    def add_analysis1(self):
        # n rodovias
        self.distinct_road_names_count = self.df.select("road_name").distinct().count()

    def add_analysis2(self):
        # n veiculos
        self.distinct_road_names_count = self.df.select("car_plate").distinct().count()

    def add_analysis3(self):
        # n veiculos risco colisao
        pass
        self.number_of_cars_in_risk_of_collision = self.cars_in_risk_of_collision.count()

    def add_analysis4(self):
        # n veiculos acima velocidade limite
        pass
        self.number_of_cars_above_speed_limit = self.cars_above_speed_limit.count()

    def add_analysis5(self):
        # lista veiculos acima limite velocidade
        # Get cars above speed limit
        pass
        joined_df = self.df.join(self.roads_data, self.df["road_name"] == self.roads_data["_c0"], "inner")
        self.cars_above_speed_limit = joined_df.filter(col("speed") > col("_c4"))

    def add_analysis6(self):
        # lista veiculos risco de colisao
        pass
        # Ordenar o DataFrame por rodovia, faixa e posição
        df_analysis = self.df.orderBy("road_name", "car_lane", "car_lenght")

        # Criar colunas para a posição e velocidade do carro da frente
        df_analysis = df_analysis.withColumn("prev_lenght", lag("car_lenght").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_lenght")
        ))
        df_analysis = df_analysis.withColumn("prev_speed", lag("car_speed").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_lenght")
        ))

        # Calcular o risco de colisão usando a fórmula dada
        df_analysis = df_analysis.withColumn("colision_risk", expr(
            "(car_lenght + 2 * speed) >= prev_speed"
        ))

    def historical_analysis(self):
        self.add_analysis7()
        self.add_analysis8()
        self.add_analysis9()

    def add_analysis7(self):
        # ranking top 100 veiculos
        pass

    def add_analysis8(self):
        # carros proibidos de circular
        pass

    def add_analysis9(self):
        # estatistica de cada rodovia
        pass

    def add_analysis10(self):
        # lista de carros com direcao perigosa
        pass

    

    

    
        

t = Transformer()
t.get_df()
t.individual_analysis()
import os
import sys
import redis
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, count, lit, lag, col, expr, countDistinct
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
        self.dashboard_db = redis.Redis(
            host='192.168.0.79',
            port=6381,
            password='1234',
            db=3,
            decode_responses = True,
        )        

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

        # splits the columns key into time and plate and laue into road, lane and length
        # ! CHANGE THIS ONCE THE DATA IS UPDATED !
        time = split(self.df['key'], ' ').getItem(0).cast('float')
        plate = split(self.df['key'], ' ').getItem(1)
        road = split(self.df['value'], ' ').getItem(0)
        lane = split(self.df['value'], ' ').getItem(1).cast('int')
        length = split(self.df['value'], ' ').getItem(2).cast('int')

        self.df = self.df.withColumn('time', time) \
            .withColumn('car_plate', plate) \
            .withColumn('road_name', road) \
            .withColumn('car_lane', lane) \
            .withColumn('car_length', length) \
            .drop('key') \
            .drop('value')
        
        self.base_transform()

    def base_transform(self):
        # Gets all the cars
        windowSpec = Window.partitionBy("car_plate").orderBy("time")

        # Uses lag to get the previous time, length and lane
        self.df = self.df.withColumn("prev_time", lag("time", 1).over(windowSpec))
        self.df = self.df.withColumn("prev_length", lag("car_length", 1).over(windowSpec))
        self.df = self.df.withColumn("prev_lane", lag("car_lane", 1).over(windowSpec))
        # Uses lag to get previous previous time and length
        self.df = self.df.withColumn("prev_prev_time", lag("time", 2).over(windowSpec))
        self.df = self.df.withColumn("prev_prev_length", lag("car_length", 2).over(windowSpec))

        # Calculates the speed
        speeds = self.df.withColumn("speed", (self.df["car_length"] - self.df["prev_length"]) / (self.df["time"] - self.df["prev_time"] + 0.1))
        speeds = speeds.select("car_plate", "time", "speed")

        # Calculates the acceleration
        accs = self.df.withColumn("acceleration", (self.df["car_length"] - 2 * self.df["prev_length"] + self.df["prev_prev_length"]) / ((self.df["time"] - self.df["prev_time"] + 0.1) * (self.df["time"] - self.df["prev_time"] + 0.1)))
        accs = accs.select("car_plate", "time", "acceleration")

        # Calculates the change of lane
        lane_changes = self.df.withColumn("lane_change", (self.df["car_lane"] == self.df["prev_lane"]))
        lane_changes = lane_changes.select("car_plate", "time", "lane_change")
        
        # Joins the dataframes and save it to class variable
        self.df_base = speeds.join(accs, "time", "outer") \
            .join(lane_changes, "time", "outer")

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
        self.dashboard_db.set("n_roads", self.distinct_road_names_count)

    def add_analysis2(self):
        # n veiculos
        self.distinct_car_names = self.df.select("car_plate").distinct()
        self.distinct_car_names_count = self.distinct_car_names.count()
        self.dashboard_db.set("n_cars", self.distinct_car_names_count)

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
        # forma burra:
        df_top = self.df.groupBy("car_plate")
        df_top = df_top.agg(countDistinct("road_name"))
        df_top = df_top.sort(df_top['count(road_name)'].desc())
        df_top.show(10)
        return df_top

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
# t.individual_analysis()
t.base_transform()

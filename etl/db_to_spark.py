import os
import sys
import redis
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import split, count, lit, lag, col, expr, countDistinct, avg, when
from pyspark.sql.functions import sum as spark_sum

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

T = 3000 

class Transformer:

    def __init__(self) -> None:
        # simply creates the spark session
        self.spark =  SparkSession.builder \
            .appName("RedisSparkIntegration") \
            .getOrCreate()
        
        # Get roads' data
        self.roads_data = self.spark.read.csv('./Simulator/world.txt', sep=" ", header=False)
        self.dashboard_db = redis.Redis(
            host='10.125.129.51',
            port=6381,
            password='1234',
            db=3,
            decode_responses = True,
        )        

        self.fined_cars = {}
        # structure: {car_plate: 
        #               { n_fines: fines number,
        #                 time: time of the last fine
        #               }

        self.forbidden_cars = {}

    def read_data_from_redis(self):
        # connects to redis
        redis_client = redis.Redis(
            host='10.125.129.51',
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

        # if there is no data, return None
        if len(data) == 0:
            return None

        # create a spark dataframe with coumns key and value 
        self.df = self.spark.createDataFrame(data, ['key', 'value'])

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

        df = self.df
        # Uses lag to get the previous time, length and lane
        df = df.withColumn("prev_time", lag("time", 1).over(windowSpec))
        df = df.withColumn("prev_length", lag("car_length", 1).over(windowSpec))
        df = df.withColumn("prev_lane", lag("car_lane", 1).over(windowSpec))
        # Uses lag to get previous previous time and length
        df = df.withColumn("prev_prev_time", lag("time", 2).over(windowSpec))
        df = df.withColumn("prev_prev_length", lag("car_length", 2).over(windowSpec))

        # Calculates the speed
        speeds = df.withColumn("speed", (df["car_length"] - df["prev_length"]) / (df["time"] - df["prev_time"] + 0.1))
        speeds = speeds.select("time", "speed")

        # Calculates the acceleration
        accs = df.withColumn("acceleration", (df["car_length"] - 2 * df["prev_length"] + df["prev_prev_length"]) / ((df["time"] - df["prev_time"] + 0.1) * (df["time"] - df["prev_time"] + 0.1)))
        accs = accs.select("time", "acceleration")

        # Calculates the change of lane
        lane_changes = df.select("time", (col("car_lane") == col("prev_lane")).alias("lane_change"))
        # lane_changes = lane_changes.select("time", "lane_change")

        # Joins the dataframes and save it to class variable
        self.df_base = df.join(speeds, "time", "outer") \
            .join(accs, "time", "outer") \
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
        self.distinct_road_names_count = self.df.select("road_name").na.drop().distinct().count()
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("n_roads", self.distinct_road_names_count)
        self.dashboard_db.set("time_n_roads", min_time)

    def add_analysis2(self):
        # n veiculos
        self.distinct_car_names = self.df.select("car_plate").na.drop().distinct()
        self.distinct_car_names_count = self.distinct_car_names.count()
        self.dashboard_db.set("n_cars", self.distinct_car_names_count)
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_n_cars", min_time)

    def add_analysis3(self):
        # n veiculos risco colisao
        self.number_of_cars_in_risk_of_collision = self.cars_in_risk_of_collision.count()

    def add_analysis4(self):
        # n veiculos acima velocidade limite
        self.number_of_cars_above_speed_limit = self.cars_above_speed_limit.count()

    def add_analysis5(self):
        # lista veiculos acima limite velocidade
        # Get cars above speed limit
        joined_df = self.df_base.join(self.roads_data, 'road_name', "inner")
        cars_above_speed_limit = joined_df.filter(col("speed") > col("speed_limit"))

        # Agrupa os dados pelo par único de 'car_plate' e 'road_name' e seleciona a primeira linha de cada grupo
        unique_cars = cars_above_speed_limit.dropDuplicates(['car_plate', 'road_name'])

        unique_cars.select("time","road_name", "car_plate", "speed", "speed_limit").show()

        self.cars_above_speed_limit = unique_cars

    def add_analysis6(self):
        # lista veiculos risco de colisao
        CONSTANT_TO_COLISION_RISK = 2

        # Ordenar o DataFrame por rodovia, faixa e posição
        df_analysis = self.df_base.orderBy("road_name", "car_lane", "car_length")

        # Criar colunas para a posição e velocidade do carro da frente
        df_analysis = df_analysis.withColumn("prev_length", lag("car_length").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_length")
        ))

        df_analysis = df_analysis.withColumn("prev_speed", lag("speed").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_length")
        ))

        # Calcular o risco de colisão usando a fórmula dada
        df_analysis = df_analysis.withColumn(
            "colision_risk", 
            expr(
                f"(car_length + {CONSTANT_TO_COLISION_RISK} * speed) >= prev_speed"
        ))

        self.cars_in_risk_of_collision = df_analysis

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
        df_top.show(100)
        return df_top

    def add_analysis8(self):
        # carros proibidos de circular

        import time
        delta_T = 10 # Variação do tempo para considerar os excessos de velocidade
        T = time.time() - delta_T
        # T = 1687802414.381332 - delta_T # testes

        # Consulta inicial para selecionar os dados em (time - T)
        df_filtered = self.df_base.select("time", "road_name", "car_plate", "speed").filter(col("time") >= T)

        # Faz join entre as duas tabelas
        joined_df = df_filtered.join(self.roads_data.select("road_name", "speed_limit"), 'road_name', "inner")

        # Calcula os carros acima do limite de velocidade (1 para sim e 0 para não)
        df_speeding = joined_df.withColumn("is_speeding", when(col("speed") > col("speed_limit"), 1).otherwise(0))

        # Utiliza ordenação e lag para verificar se o carro está acima da velocidade sequencialmente
        df_speeding = df_speeding.withColumn("prev_speed", lag(col("is_speeding")).over(Window.partitionBy("car_plate").orderBy("time")))
        df_speeding = df_speeding.withColumn("is_first_speeding", when(col("prev_speed").isNull(), col("is_speeding")).otherwise(col("is_speeding") - col("prev_speed")))

        # Filtra apenas os carros com resultado igual a 1
        df_filtered_speeding = df_speeding.filter(col("is_first_speeding") == 1)

        # Conta quantas vezes cada carro esteve acima da velocidade (count por car_plate)
        df_count_speeding = df_filtered_speeding.groupBy("car_plate").agg(spark_sum("is_first_speeding").alias("count_speeding"))

        # Filtra apenas os carros com 10 ou mais excessos de velocidade
        df_final_result = df_count_speeding.filter(col("count_speeding") >= 10)

        # Exibe o resultado final
        df_final_result.show()

    def add_analysis9(self):
        # estatistica de cada rodovia
        df_complete = self.df_base.join(self.roads_data, "road_name")
        self.road_stats = df_complete.groupBy("road_name", "time").agg(
            avg("speed").alias("mean_speed"),
            count(when(df_complete.speed == 0, True)).alias("n_accidents"),
            (avg("road_length") / avg("speed")).alias("avg_traversal_time")
        )


    def add_analysis10(self):
        # Define the safety variables
        safe_speed = 100
        safe_acc = 100
        t = 1000
        n = 6

        # Define the conditions for risk events
        speed_condition = col("speed") > safe_speed
        acceleration_condition = col("acceleration") > safe_acc
        lane_change_condition = (col("lane_change") == True) & (lag(col("lane_change")).over(
            Window.partitionBy("car_plate").orderBy("time")) == True
        )

        # Create the "risk_counter" column
        df_risk = self.df_base.withColumn("risk_counter", when(speed_condition, 1).otherwise(0)
                                    + when(acceleration_condition, 1).otherwise(0)
                                    + when(lane_change_condition, 1).otherwise(0)
        )

        # Calculate the "dangerous_driving" column based on the "risk_count" column
        self.df_risk = df_risk.withColumn("dangerous_driving", when(spark_sum(col("risk_counter")).over(
            Window.partitionBy("car_plate").orderBy("time").rowsBetween(Window.currentRow - t, Window.currentRow - 1)
        ) >= n, True).otherwise(False))


# quit()
t = Transformer()
if t.get_df() is None:
    # Sem dados no banco
    pass
else:
    t.add_analysis1()
    t.add_analysis2()
    t.base_transform()
import os
import sys
import redis
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import split, count, lit, lag, col, expr, countDistinct, avg, when
from pyspark.sql.functions import sum as spark_sum
import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

T = 3000

class Transformer:

    def __init__(self) -> None:
        # simply creates the spark session
        self.spark =  SparkSession.builder \
            .appName("RedisSparkIntegration") \
            .getOrCreate()
            
        column_names = ['road_name', 'lanes_f', 'lanes_b', 'length', 'speed_limit', 'prob_of_new_car', 'prob_of_changing_lane', 'prob_of_collision', 'car_speed_min', 'car_speed_max', 'car_acc_min', 'car_acc_max', 'collision_fix_time']

        # Cria um DataFrame do Pandas
        pandas_df = pd.read_csv('./Simulator/world.txt', sep=" ", header=None, names=column_names)

        # Converte o datatype das colunas para int
        colums_to_convert = pandas_df.columns[1:]
        for column_name in colums_to_convert:
            pandas_df[column_name] = pandas_df[column_name].astype(int)
        
        # Get roads' data
        self.roads_data = self.spark.createDataFrame(pandas_df)
        self.dashboard_db = redis.Redis(
            host='localhost',
            port=6379,
            db=3,
            decode_responses = True,
        )        

        self.fined_cars = {}
        # structure: {car_plate: 
        #               { n_fines: fines number,
        #                 time: time of the last fine
        #               }

        self.forbidden_cars = {}
        
    def send_to_redis(self, data_name, data):
        # Converter o DataFrame do Spark para um Pandas DataFrame
        pandas_df = data.toPandas()

        # Converter o Pandas DataFrame para uma representação CSV em forma de string
        csv_string = pandas_df.to_csv(index=False)
        
        # Envia para o redis
        self.dashboard_db.set(data_name, csv_string)

    def read_data_from_redis(self):
        # connects to redis
        redis_client = redis.Redis(
            host='localhost',
            port=6379,
            db=1,
            decode_responses = True,
        )

        # creates a container for tuples of (data, value)
        data = []

        # creates a tuple for every tuple
        for key in redis_client.keys("*"):
            value = redis_client.get(key)
            data.append((key, value))

        print(f"Read {len(data)} data from redis")
        return data

    def get_df(self):
        # gets the data
        data = self.read_data_from_redis()

        # if there is no data, return None
        if len(data) == 0:
            return 0

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
        df = df.withColumn(
            "speed", 
            (df["car_length"] - df["prev_length"]) / (df["time"] - df["prev_time"] + 0.1),  
        )

        # Calculates the acceleration
        df = df.withColumn(
            "acceleration", 
            (df["car_length"] - 2 * df["prev_length"] + df["prev_prev_length"]) / ((df["time"] - df["prev_time"] + 0.1) * (df["time"] - df["prev_time"] + 0.1))
        )

        # Calculates the change of lane
        df = df.select("time", "road_name", "car_lane", "car_length", "car_plate", "acceleration", "speed", (col("car_lane") == col("prev_lane")).alias("lane_change"))

        # Joins the dataframes and save it to class variable
        self.df_base = df
        
        print("Base transform done")


    def individual_analysis(self, print_data=False):
        self.add_analysis1()
        print("Analysis 1 done")
        self.add_analysis2()
        print("Analysis 2 done")
        self.add_analysis5()
        print("Analysis 5 done")
        self.add_analysis6()
        print("Analysis 6 done")
        self.add_analysis3()
        print("Analysis 3 done")
        self.add_analysis4()
        print("Analysis 4 done")


    def add_analysis1(self):
        # n rodovias
        self.distinct_road_names_count = self.df.select("road_name").na.drop().distinct().count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_roads", self.distinct_road_names_count)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_n_roads", min_time)


    def add_analysis2(self):
        # n veiculos
        self.distinct_car_names = self.df.select("car_plate").na.drop().distinct()
        self.distinct_car_names_count = self.distinct_car_names.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_cars", self.distinct_car_names_count)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_n_cars", min_time)


    def add_analysis3(self):
        # n veiculos risco colisao
        self.number_of_cars_in_risk_of_collision = self.cars_in_risk_of_collision.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_collisions_risk", self.number_of_cars_in_risk_of_collision)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_n_collisions_risk", min_time)
        
        

    def add_analysis4(self):
        # n veiculos acima velocidade limite
        self.number_of_cars_above_speed_limit = self.cars_above_speed_limit.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_over_speed", self.number_of_cars_above_speed_limit)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_n_over_speed", min_time)
        

    def add_analysis5(self):
        # lista veiculos acima limite velocidade
        # Get cars above speed limit
        joined_df = self.df_base.join(self.roads_data, 'road_name', "inner")
        cars_above_speed_limit = joined_df.filter(col("speed") > col("speed_limit"))

        # Agrupa os dados pelo par único de 'car_plate' e 'road_name' e seleciona a primeira linha de cada grupo
        unique_cars = cars_above_speed_limit.dropDuplicates(['car_plate', 'road_name']).select("time","road_name", "car_plate", "speed", "speed_limit")

        self.cars_above_speed_limit = unique_cars
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_list_over_speed", min_time)
        
        # Envia os dados para o redis
        self.send_to_redis("list_over_speed", unique_cars)

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

        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_list_collisions_risk", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_collisions_risk", df_analysis)

    def historical_analysis(self, print_results=False):
        self.add_analysis7()
        print("Análise 7 finalizada")
        self.add_analysis8()
        print("Análise 8 finalizada")
        self.add_analysis9()
        print("Análise 9 finalizada")

    def add_analysis7(self):
        # ranking top 100 veiculos
        df_top = self.df.groupBy("car_plate")
        df_top = df_top.agg(countDistinct("road_name"))
        df_top = df_top.sort(df_top['count(road_name)'].desc())
        df_top = df_top.limit(100)
    
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_top_100", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("top_100", df_top)

    def add_analysis8(self):
        # carros proibidos de circular

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

        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_list_banned_cars", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_banned_cars", df_final_result)

    def add_analysis9(self):
        # estatistica de cada rodovia
        df_complete = self.df_base.join(self.roads_data, "road_name")
        self.road_stats = df_complete.groupBy("road_name", "time").agg(
            avg("speed").alias("mean_speed"),
            count(when(df_complete["speed"] == 0, True)).alias("n_accidents"),
            (avg("length") / avg("speed")).alias("avg_traversal_time")
        )
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_list_roads", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_roads", self.road_stats)


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
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df.select("time").agg({"time": "min"}).collect()[0][0]
        self.dashboard_db.set("time_list_dangerous_cars", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_dangerous_cars", self.df_risk)

if __name__ == "__main__":
    t = Transformer()
    if t.get_df() == 0:
        print("No data to transform")
    else:
        t.base_transform()
        t.individual_analysis(True)
        t.historical_analysis(True)
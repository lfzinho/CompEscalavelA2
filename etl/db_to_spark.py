import os
import sys
import redis
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import split, count, lit, lag, col, expr, countDistinct, avg, when
from pyspark.sql.functions import row_number
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import max as spark_max

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
            db=1,
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
        self.time_before_read_data = time.time()
        # connects to redis
        redis_client = redis.Redis(
            host='localhost',
            port=6379,
            db=0,
            decode_responses = True,
        )

        # creates a container for tuples of (data, value)
        data = []

        # creates a tuple for every tuple
        for key in redis_client.keys("*"):
            value = redis_client.get(key)
            data.append((key, value))

        # print(f"Read {len(data)} data from redis")
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
        road_length = split(self.df['value'], ' ').getItem(3).cast('int')
        speed_limit = split(self.df['value'], ' ').getItem(4).cast('int')

        self.df = self.df.withColumn('time', time) \
            .withColumn('car_plate', plate) \
            .withColumn('road_name', road) \
            .withColumn('car_lane', lane) \
            .withColumn('car_length', length) \
            .withColumn('road_length', road_length) \
            .withColumn('speed_limit', speed_limit) \
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
        df = df.select("time", "road_name", "car_lane", "car_length", "car_plate", "acceleration", "speed", "road_length", "speed_limit", (col("car_lane") == col("prev_lane")).alias("lane_change"))

        # Joins the dataframes and save it to class variable
        self.df_base = df
        
        # Define uma janela de particionamento por "car_plate" ordenada por "time" em ordem decrescente
        window_spec = Window.partitionBy("car_plate").orderBy(col("time").desc())

        # Adiciona uma coluna de número de linha para cada registro dentro da janela
        df = df.withColumn("row_number", row_number().over(window_spec))

        # Filtra o DataFrame para obter apenas as leituras mais recentes de cada carro
        df = df.filter(col("row_number") == 1).drop("row_number")
        
        # Remove os carros que não enviam dados há mais de 1 minuto
        self.df_base_curr = df.filter(col("time") > (self.time_before_read_data - 60))
        


    def individual_analysis(self, print_log=False):
        self.add_analysis1()
        if print_log: print("Finished analysis 1")
        self.add_analysis2()
        if print_log: print("Finished analysis 2")
        self.add_analysis6()
        if print_log: print("Finished analysis 6")
        self.add_analysis5()
        if print_log: print("Finished analysis 5")
        self.add_analysis3()
        if print_log: print("Finished analysis 3")
        self.add_analysis4()
        if print_log: print("Finished analysis 4")


    def add_analysis1(self):
        # n rodovias
        self.distinct_road_names_count = self.df_base_curr.select("road_name").na.drop().distinct().count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_roads", self.distinct_road_names_count)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_n_roads", min_time)


    def add_analysis2(self):
        # n veiculos
        self.distinct_car_names = self.df_base_curr.select("car_plate").na.drop().distinct()
        self.distinct_car_names_count = self.distinct_car_names.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_cars", self.distinct_car_names_count)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_n_cars", min_time)


    def add_analysis3(self):
        # n veiculos risco colisao
        self.number_of_cars_in_risk_of_collision = self.cars_in_risk_of_collision.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_collisions_risk", self.number_of_cars_in_risk_of_collision)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_n_collisions_risk", min_time)
        
        

    def add_analysis4(self):
        # n veiculos acima velocidade limite
        self.number_of_cars_above_speed_limit = self.cars_above_speed_limit.count()
        
        # Envia os dados da análise para o dashboard
        self.dashboard_db.set("n_over_speed", self.number_of_cars_above_speed_limit)
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_n_over_speed", min_time)
        

    def add_analysis5(self):
        # lista veiculos acima limite velocidade        
        # Filtra os carros acima do limite de velocidade
        cars_above_speed_limit = self.cars_with_risk_of_collision \
            .filter(spark_abs(col("speed")) > col("speed_limit")) \
                .drop("prev_length", "prev_speed")

        # Agrupa os dados pelo par único de 'car_plate' e 'road_name' e seleciona a primeira linha de cada grupo
        unique_cars = cars_above_speed_limit \
            .dropDuplicates(['car_plate', 'road_name']) \
                .select(
                    "time",
                    "road_name", 
                    "car_plate", 
                    "speed", 
                    "speed_limit", 
                    "collision_risk"
                )

        self.cars_above_speed_limit = unique_cars
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_list_over_speed", min_time)
        
        # Envia os dados para o redis
        self.send_to_redis("list_over_speed", unique_cars)
        

    def add_analysis6(self):
        # lista veiculos risco de colisao
        CONSTANT_TO_collision_risk = 0.000002   # Constante para o cálculo do risco de colisão
                                                # A Polícia Rodoviária Federal considera que o risco de colisão é alto quando um carro está a menos de 2 segundos do carro da frente (considerando a velocidade atual do carro, com o carro da frente parado)

        # Filtra os carros que têm velocidade faltante (null)
        df_analysis = self.df_base_curr.filter(col("speed").isNotNull())

        # Ordenar o DataFrame por rodovia, faixa e posição
        df_analysis = df_analysis.orderBy("road_name", "car_lane", "car_length")

        # Criar colunas para a posição e velocidade do carro anterior
        df_analysis = df_analysis.withColumn("prev_length", lag("car_length").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_length")
        ))

        df_analysis = df_analysis.withColumn("prev_speed", lag("speed").over(
            Window.partitionBy("road_name", "car_lane").orderBy("car_length")
        ))

        # Calcular o risco de colisão usando a fórmula dada
        df_analysis = df_analysis.withColumn(
            "collision_risk", 
            when(expr("prev_speed > 0"), (expr("prev_length") + CONSTANT_TO_collision_risk * expr("prev_speed")) >= expr("car_length"))
            .when(expr("prev_speed < 0"), (expr("prev_length") + CONSTANT_TO_collision_risk * expr("prev_speed")) <= expr("car_length"))
            .otherwise(False)
        )
        
        # Salva o resultado parcial para ser usado na análise 5
        self.cars_with_risk_of_collision = df_analysis
        
        # Filtra os carros em risco de colisão
        df_final = df_analysis.filter(col("collision_risk") == True).drop("prev_length", "prev_speed")
        
        # Salva o resultado parcial para ser usado na análise 5
        self.cars_in_risk_of_collision = df_final

        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_list_collisions_risk", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_collisions_risk", df_final)

    def historical_analysis(self, print_log=False):
        self.add_analysis7()
        if print_log: print("Finished analysis 7")
        self.add_analysis8()
        if print_log: print("Finished analysis 8")
        self.add_analysis9()
        if print_log: print("Finished analysis 9")
        self.add_analysis10()
        if print_log: print("Finished analysis 10")

    def add_analysis7(self):
        # ranking top 100 veiculos
        df_top = self.df.groupBy("car_plate")
        df_top = df_top.agg(countDistinct("road_name").alias("n_roads"))
        df_top = df_top.sort(df_top['n_roads'].desc())
        df_top = df_top.limit(100)
    
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_top_100", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("top_100", df_top)

    def add_analysis8(self):
        # carros proibidos de circular

        delta_T = 10 # Variação do tempo para considerar os excessos de velocidade
        T = time.time() - delta_T
        # T = 1687802414.381332 - delta_T # testes

        # Consulta inicial para selecionar os dados em (time - T)
        df_filtered = self.df_base.select("time", "road_name", "car_plate", "speed", "speed_limit").filter(col("time") >= T)

        # Calcula os carros acima do limite de velocidade (1 para sim e 0 para não)
        df_speeding = df_filtered.withColumn("is_speeding", when(col("speed") > col("speed_limit"), 1).otherwise(0))

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
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_list_banned_cars", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_banned_cars", df_final_result)
        

    def add_analysis9(self):
        # estatistica de cada rodovia
        df = self.df_base.select("road_name", "time", "speed", "road_length")
        self.road_stats = df.groupBy("road_name").agg(
            avg(spark_abs("speed")).alias("mean_speed"),
            count(when(df["speed"] == 0, True)).alias("n_accidents"),
            (avg("road_length") / avg(spark_abs("speed"))).alias("avg_traversal_time")
        )
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_list_roads", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_roads", self.road_stats)


    def add_analysis10(self):
        # Define as variaveis de seguranca
        safe_speed = 100
        safe_acc = 100
        t = 10000
        i = 100
        n = 6

        # Define o intervalo de tempo i
        # risk_i_window = Window.partitionBy('car_plate').orderBy('time')#.rangeBetween(-i, -1)
        t = time.time() - t
        df_t = self.df_base.filter(col('time') > t)
        
        from pyspark.sql.functions import max, when

        # Define o período de tempo i
        dangerous_driving_window = Window.partitionBy('car_plate').orderBy('time').rangeBetween(-i, -1)

        # Cria a coluna contadora de risco
        df = df_t.withColumn('risk_counter', 
                            when(col('speed') > safe_speed, 1).otherwise(0) +
                            when(col('acceleration') > safe_acc, 1).otherwise(0) +
                            when((col('lane_change') == 1) & (max(when(col('lane_change') == 1, 1)).over(dangerous_driving_window) == 1), 1).otherwise(0)
                            )

        # Calcula a coluna de risco sobre o intervalo
        df = df.withColumn('risk_i', spark_sum('risk_counter').over(dangerous_driving_window))

        # Calcula a coluna de direcao perigosa
        df = df.withColumn('dangerous_driving', when(col('risk_i') >= n, True).otherwise(False))
        
        # Define uma janela de particionamento por "car_plate" ordenada por "time" em ordem decrescente
        window_spec = Window.partitionBy("car_plate").orderBy(col("time").desc())

        # Adiciona uma coluna de número de linha para cada registro dentro da janela
        df = df.withColumn("row_number", row_number().over(window_spec))

        # Filtra o DataFrame para obter apenas as leituras mais recentes de cada carro
        df = df.filter(col("row_number") == 1).drop("row_number")
        
        # Remove os carros que não enviam dados há mais de 1 minuto
        df = df.filter(col("time") > (self.time_before_read_data - 60))
        
        # Coleta o tempo gasto e envia para o dashboard
        min_time = self.df_base_curr.select("time").agg({"time": "max"}).collect()[0][0]
        if min_time is not None: self.dashboard_db.set("time_list_dangerous_cars", min_time)
        
        # Envia os dados para o dashboard
        self.send_to_redis("list_dangerous_cars", df)

if __name__ == "__main__":
    t = Transformer()
    while True:
        if t.get_df() == 0:
            print("No data to transform")
        else:
            t.base_transform()
            t.individual_analysis()
            t.historical_analysis()
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Verificação do Spark").getOrCreate()
print("Versão do Spark:", spark.version)
spark.stop()
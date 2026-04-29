#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, sum, avg
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName("Ventas").getOrCreate()

df = spark.read.csv('hdfs://localhost:9000/Tarea3/ventas.csv', header=True, inf>
print("=== Datos originales ===")
df.show()

df_clean = df.dropna()
print("=== Datos limpios ===")
df_clean.show()

df_transformed = df_clean.withColumn("total", col("precio") * col("cantidad"))
print("=== Datos transformados ===")
df_transformed.show()

ventas_ciudad = df_transformed.groupBy("ciudad").agg(sum("total").alias("total_ventas"))
print("=== Ventas por ciudad ===")
ventas_ciudad.show()

promedio_categoria = df_transformed.groupBy("categoria").agg(avg("precio").alias("precio_promedio"))
print("=== Promedio por categoría ===")
promedio_categoria.show()

top_productos = df_transformed.groupBy("producto").agg(sum("cantidad").alias("total_vendido")).orderBy(col("total_vendido").desc())
print("=== Top productos ===")
top_productos.show()

spark.stop()


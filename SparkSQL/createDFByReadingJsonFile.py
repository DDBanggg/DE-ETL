from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

schemaJson = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True)
    ]), True)
])

jsonData = spark.read.schema(schemaJson).json("C:/Users/ddb11/PycharmProjects/DE-ETL/Data/2015-03-01-17.json")
# jsonData.show(truncate=False)

# show cac cot trong actor
jsonData.select(col("id"), col("type"), col("actor.id").alias("actor_id"), col("actor.login").alias("actor_login"), col("actor.gravatar_id").alias("actor_gravatar_id"), col("actor.url").alias("actor_url"), col("actor.avatar_url").alias("actor_avatar_url")).show(truncate=False)


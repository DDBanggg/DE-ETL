from pyspark.sql import SparkSession

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

textFile = spark.read.text("C:/Users/ddb11/PycharmProjects/DE-ETL/Data/Data.txt")

textFile.show(truncate=False)
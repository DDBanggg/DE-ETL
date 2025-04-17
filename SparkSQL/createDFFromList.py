from pyspark.sql import SparkSession

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

data = [
    ("Bang", "SV", 2005),
    ("Test", "HS", 2010)
]

df = spark.createDataFrame(data, ["Name", "Job", "Year"])
# df.show()
df.printSchema()
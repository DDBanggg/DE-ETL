from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = sc.parallelize(["one", 1, 2, "two", 1, 2, 3, 4, 5, "one", "three", "two"])

transData = data.distinct()
print(transData.collect()) # 1
print(transData.count()) # 2
print(transData.first()) # 3

sc.stop()

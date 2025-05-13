from pyspark import SparkContext, SparkConf
import time
from random import Random

# hợp nhất các bản ghi có giá trị gioosgn nhau

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = sc.parallelize(["one", 1, 2, "two", 1, 2, 3, 4, 5, "one", "three", "two"])

transData = data.distinct()
print(transData.collect())

sc.stop()

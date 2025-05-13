from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 5)

def sum(x1 : int, x2 : int) -> int:
    print(f"x1 : {x1}, x2 : {x2} => ({x1 + x2})")
    return x1 + x2

print(data.reduce(sum))

sc.stop()

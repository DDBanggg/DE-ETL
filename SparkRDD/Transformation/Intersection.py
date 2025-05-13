from pyspark import SparkContext, SparkConf
import time
from random import Random

# Tim ptu giong nhau

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

rdd1 = sc.parallelize([1,2,3,4,5,6])
rdd2 = sc.parallelize([1,2,7,8,9,10])

rdd3 = rdd1.intersection(rdd2)
print(rdd3.collect())

sc.stop()

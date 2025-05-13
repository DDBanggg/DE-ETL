from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = ['Bang', 'TrAnh', 'MAnh', 'Thinh', 'Truong']

rdd = sc.parallelize(data,2)
# print(rdd.glom().collect())

# def process_partition(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0, 1000))
#     return [f"{name} : {rand.randint(0, 1000)}" for name in iterator]
#
# results = rdd.mapPartitions(process_partition)
# print(results.collect())

results = rdd.mapPartitions(
    lambda iterator : map(
        lambda name : f"{name}:{Random(int(time.time() * 1000) + Random().randint(0, 1000)).randint(0, 1000)}",
        iterator
    )
)

print((results.collect()))

sc.stop()

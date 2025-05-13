from pyspark import SparkContext, SparkConf
import time
from random import Random

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = sc.parallelize([("vietanh", 15),("huyquang", 20),
                       ("vietanh", 3), ("dat", 30),
                       ("huyquang", 19)])

results = data.reduceByKey(lambda key, value : key + value)
results1 = results.map(lambda x : (x[1], x[0])).sortByKey()
print(results.collect())


# data2 = sc.parallelize([("vietanh", "thongminh"),("huyquang", "depzai"),
#                        ("vietanh", "it"), ("dat", "tester"),
#                        ("huyquang", "backend")])
#
# data3 = data.join(data2).sortByKey(ascending=True)
# for i in data3.collect():
#     print(i)

from pyspark import SparkContext, SparkConf
import time
from random import Random

# Xóa các hàng trùng lặp

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

text = sc.parallelize(["Nguoi ta ThICh aNH AnH kHONg tHe CAN duoc"])\
    .map(lambda x : x.lower())\
    .flatMap(lambda x : x.split(" "))

# removeText = sc.parallelize(["thich", "anh", "can", "duoc"])
removeText = sc.parallelize("thich anh can duoc".split(" "))
#     .flatMap(lambda x : x.split(" "))

result = text.subtract(removeText)
print(result.collect())
# print(removeText.collect())

from pyspark import SparkContext, SparkConf

# Hợp nhất dữ liệu

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

data = [
    {"id" : 1, "name" : "Bang"},
    {"id" : 2, "name" : "Truong Anh"},
    {"id" : 3, "name" : "Minh Anh"}
]

rdd1 = sc.parallelize(data)
rdd2 = sc.parallelize([1,2,3,4,5,6])

rdd3 = rdd1.union(rdd2)
print(rdd3.collect())

sc.stop()

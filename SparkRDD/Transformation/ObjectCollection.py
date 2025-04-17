from pyspark import SparkContext

sc = SparkContext("local[*]", "103-Spark")

data = [
    {"id" : 1, "name" : "Bang"},
    {"id" : 2, "name" : "Truong Anh"},
    {"id" : 3, "name" : "Minh Anh"}
]

rdd = sc.parallelize(data)

#action
print(rdd.collect()) # in ra xâu data
print(rdd.count()) # đếm data
print(rdd.first()) # in ra data đầu tiên
print(rdd.getNumPartitions()) # kiểm tra số lượng core CPU sử dụng
print(rdd.glom().collect()) # in ra phân vùng nào chứa data gì
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

fileRdd = sc.textFile(r"C:\Users\ddb11\PycharmProjects\DE-ETL\Data\Data.txt") \
    .map(lambda x : x.replace(":", "")) \
    .map(lambda x : x.lower()) \
    .flatMap(lambda x : x.split(" ")) \
    .map(lambda x : (x, 1)) \
    .reduceByKey(lambda key, value : key + value) \
    .map(lambda x : (x[1], x[0])) \
    .sortByKey(ascending=False)

fileRdd2 = fileRdd.map(lambda x : (x[1], x[0]))
rdd2 = sc.parallelize([("or", "find")])
results1 = fileRdd2.join(rdd2)
print(results1.collect())
#actions
# print(fileRdd.count())
# print(fileRdd.collect())
# for i in fileRdd.collect():
#     print(i)

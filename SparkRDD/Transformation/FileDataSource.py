from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

fileRdd = sc.textFile(r"C:\Users\ddb11\PycharmProjects\DE-ETL\SparkRDD\Transformation\Data\Data.txt")

#actions
print(fileRdd.count())

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("DE-ETL").setMaster("local[*]").set("spark.executor.memory",'4g')

sc = SparkContext(conf=conf)

numbers = [1,2,3,4,5,6,7,8,9,10]

numbersRdd = sc.parallelize(numbers)
# print(numbersRdd.collect())
squareRdd = numbersRdd.map(lambda x : x*x)
print(squareRdd.collect())
filterRdd = numbersRdd.filter(lambda x : x > 3)
print(filterRdd.collect())
flatMapRdd = numbersRdd.flatMap(lambda x : [x, x * 2])
print(flatMapRdd.collect())

sc.stop()

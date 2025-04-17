import random

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from datetime import datetime

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

# data = spark.sparkContext.parallelize([
#     Row(1, "TrAnh", 20),
#     Row(2, "MAnh", 19),
#     Row(3, "Bang", 20)
# ])
#
# schema = StructType([
#     StructField("id",LongType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", LongType(), True)
# ])
#
# df = spark.createDataFrame(data, schema)
# df.show()

"""
from pyspark.sql.types import *
StringType : bieu dien cac gia tri chuoi
IntegerType : bieu dien cac gia tri so nguyen 32bit
LongType : bieu dien cac gia tri so nguyen 64bit
FloatType : so thap phan 32bit
DoubleType : so thap phan 64bit
BooleanType : True/False
ByteType : so nguyen 8bit
ShortType : so nguyen 16bit
BinaryType : nhi phan
TimestampType : gia tri thoi gian (ngay va gio)
DateType : gia tri nam, thang, ngay

ArrayType(elementType) : gia tri cua cac array, list
- elementType bieu dien cac gia tri cua cac phan tu trong ArrayType(IntergerType(), StringType())

MapType(keyType, valueType) : bieu dien cac gia tri cua key-value (dict)
- keyType : gia tri key
- valueType : gia tri value

StructType : cau truc cua cac StructField
- chua danh sach cac doi tuong StructField

StructField(name, dataType, nullable) : gia tri 1 column trong StructType
- name : ten cua truong (column)
- dataType 
- nullable : gia tri Boolean(True, False)
"""

data = [
    Row(
        name = "bang",
        age = 20,
        id = 1,
        salary = 11000.0,
        bonus = 1823.00,
        is_active = True,
        scores = [11, 18, 23],
        attributes = {"dept" : "DE", "role" : "Developer"},
        hire_date = datetime.strptime("2023-09-05","%Y-%m-%d").date(), #convert string to date
        last_login = datetime.strptime("2025-04-16 10:30:00","%Y-%m-%d %H:%M:%S") #convert string to date time
        # tax_rate = 11.03
    ),
Row(
        name = "zin",
        age = 15,
        id = 2,
        salary = 18000.0,
        bonus = 6.00,
        is_active = False,
        scores = [18, 6, 10],
        attributes = {"dept" : "HS", "role" : "hoc"},
        hire_date = datetime.strptime("2023-08-15","%Y-%m-%d").date(), #convert string to date
        last_login = datetime.strptime("2025-01-01 1:23:45","%Y-%m-%d %H:%M:%S") #convert string to date time
        # tax_rate = 18.06
    )
]

schemaPeople = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField('id', LongType(), True),
    StructField("salary", FloatType(), True),
    StructField("bonus", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("scores", ArrayType(IntegerType()), True),
    StructField("attributes", MapType(StringType(), StringType()), True),
    StructField("hire_date", DateType(), True),
    StructField("last_login", TimestampType(), True)
])
df = spark.createDataFrame(data, schemaPeople)
df.show(truncate=False)
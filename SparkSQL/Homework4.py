import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType

# Create SparkSession
spark = SparkSession.builder \
    .appName("DE-ETL") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Sample data
data = [
    ["23/04.2025"],
    ["~11-01//2021"],
    ["2021--/.09*30"]
]

# Create DataFrame
df = spark.createDataFrame(data, ["date"])


def extract_date(date_str):
    # Thay ký tự bằng space
    delCharacter = re.sub(r'[^0-9]', ' ', date_str)

    # Tách các số
    separate = delCharacter.split()

    # Kiểm tra định dạng
    if len(separate) != 3:
        return ("", "", "")

    # Lấy year
    year = ""
    for p in separate:
        if len(p) == 4:
            year = p
            break

    # Xóa year khỏi separate
    if year in separate:
        separate.remove(year)

    # Lấy day, month
    day = ""
    month = ""
    if len(separate) == 2 and len(separate[0]) == 2 and len(separate[1]) == 2:
        num0 = int(separate[0])
        num1 = int(separate[1])

        # Trường hợp cả 2 phần tử < 13
        if num0 < 13 and num1 < 13:
            # Kiểm tra vị trí của year
            if delCharacter.startswith(year):
                month = separate[0]
                day = separate[1]
            elif delCharacter.endswith(year):
                month = separate[1]
                day = separate[0]
        # Trường hợp 1 phần tử < 13, 1 phần tử >= 13
        else:
            if num0 < 13 <= num1:
                month = separate[0]
                day = separate[1]
            elif num1 < 13 <= num0:
                month = separate[1]
                day = separate[0]

    # Định dạng 2 chữ số
    return (day, month, year)


# Định nghĩa schema cho UDF
schema = StructType([
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", StringType(), True)
])

extract_dateUDF = udf(extract_date, schema)

result = df.withColumn("date_parts", extract_dateUDF(col("date"))) \
    .withColumn("day", col("date_parts.day")) \
    .withColumn("month", col("date_parts.month")) \
    .withColumn("year", col("date_parts.year")) \
    .drop("date_parts")

result.show()




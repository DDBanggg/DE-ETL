from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("DataValidationPipeline") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0") \
        .getOrCreate()

    schemaData = StructType([
        StructField("message_id", LongType(), True),  # Sửa thành LongType để khớp với SERIAL KEY
        StructField("user_id", IntegerType(), True),
        StructField("login", StringType(), True),
        StructField("gravatar_id", StringType(), True),
        StructField("avatar_url", StringType(), True),
        StructField("url", StringType(), True),
        StructField("state", StringType(), True),
        StructField("log_timestamp", StringType(), True),
        StructField("batch_id", StringType(), True)
    ])

    schemaControl = StructType([
        StructField("batch_id", StringType(), True),
        StructField("expected_count", IntegerType(), True),
        StructField("message_id_start", LongType(), True),
        StructField("message_id_end", LongType(), True)
    ])


    # Luồng dữ liệu chính từ topic 'bangg'
    df_data_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bangg") \
        .option("startingOffsets", "latest") \
        .load()

    # Luồng kiểm soát từ topic 'bangg_control'
    df_control_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bangg_control") \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_data_raw.select(from_json(col("value").cast("string"), schemaData).alias("data")).select("data.*")

    condition_is_complete = col("user_id").isNotNull() & col("message_id").isNotNull()

    df_complete_records = df_parsed.filter(condition_is_complete)
    df_missing_fields = df_parsed.filter(~condition_is_complete)

    df_actual_counts = df_complete_records \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("batch_id") \
        .agg(count("*").alias("actual_count"))

    df_control_parsed = df_control_raw \
        .select(from_json(col("value").cast("string"), schemaControl).alias("control")) \
        .select("control.*") \
        .withWatermark("timestamp", "10 minutes")

    df_validation_result = df_control_parsed.join(
        df_actual_counts,
        expr("""
            batch_id = batch_id AND
            timestamp >= timestamp AND
            timestamp <= timestamp + interval 10 minutes
        """),
        "leftOuter"
    ).withColumn("is_count_valid", col("actual_count") == col("expected_count"))

    # --- 5. Ghi các kết quả ra các đích khác nhau ---
    validation_query = df_validation_result.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .queryName("BatchValidation_Output") \
        .start()

    # Bỏ comment khối này và comment khối console bên dưới để ghi vào MongoDB
    # mongo_stream =  df_complete_records.writeStream \
    #     .format("mongodb") \
    #     .option("checkpointLocation", "/tmp/spark/checkpoint/mongodb") \
    #     .option("forceDeleteTempCheckpointLocation", "true") \
    #     .option('spark.mongodb.connection.uri', 'mongodb://bangg:123@localhost:27017') \
    #     .option('spark.mongodb.database', 'github_data') \
    #     .option('spark.mongodb.collection', 'users') \
    #     .outputMode("append") \
    #     .start()

    complete_records_query = df_complete_records.writeStream \
        .format("console") \
        .outputMode("append") \
        .queryName("CompleteRecords_Sink") \
        .start()

    missing_fields_query = df_missing_fields.writeStream \
        .outputMode("append") \
        .format("console") \
        .queryName("MissingFields_ALERT") \
        .start()


    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
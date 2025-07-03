from pyspark.sql.functions import col
from pyspark.sql.types import *

from config.spark_config import SparkConnect

from src.spark.spark_write_data import SparkWriteDatabases
from config.database_config import get_spark_config

def main():

    jars = [
        "mysql:mysql-connector-java:8.0.33"
    ]

    spark_connect = SparkConnect(
        app_name="bangg",
        master_url="local[*]",
        executor_memory="2g",
        executor_cores=1,
        driver_memory="2g",
        num_executors=1,
        jar_packages= jars,
        log_level="INFO"
    )

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("type", StringType(), True),
        StructField("actor", StructType([
            StructField("id", LongType(), True),
            StructField("login", StringType(), True),
            StructField("gravatar_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("avatar_url", StringType(), True)
        ]), True),
        StructField("repo", StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("url", StringType(), True),
        ]), True),
        StructField("payload", StructType([
            StructField("action", StringType(), True),
            StructField("issue", StructType([
                StructField("url", StringType(), True),
                StructField("labels_url", StringType(), True),
                StructField("comments_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("id", LongType(), True),
                StructField("number", LongType(), True),
                StructField("title", StringType(), True),
                StructField("user", StructType([
                    StructField("login", StringType(), True),
                    StructField("id", LongType(), True),
                    StructField("avatar_url", StringType(), True),
                    StructField("gravatar_id", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                    StructField("followers_url", StringType(), True),
                    StructField("following_url", StringType(), True),
                    StructField("gists_url", StringType(), True),
                    StructField("starred_url", StringType(), True),
                    StructField("subscriptions_url", StringType(), True),
                    StructField("organizations_url", StringType(), True),
                    StructField("repos_url", StringType(), True),
                    StructField("events_url", StringType(), True),
                    StructField("received_events_url", StringType(), True),
                    StructField("type", StringType(), True),
                    StructField("site_admin", BooleanType(), True)
                ]), True),
                StructField("labels", ArrayType(StructType([
                    StructField("url", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("color", StringType(), True)
                ])), True),
                StructField("state", StringType(), True),
                StructField("locked", BooleanType(), True),
                StructField("assignee", StringType(), True),
                StructField("milestone", StringType(), True),
                StructField("comments", LongType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
                StructField("closed_at", StringType(), True),
                StructField("body", StringType(), True)
            ]), True)
        ]), True),
        StructField("public", BooleanType(), True),
        StructField("created_at", StringType(), True)
    ])

    df = spark_connect.spark.read.schema(schema).json("C:/Users/ddb11/PycharmProjects/DE-ETL/Data/2015-03-01-17.json")

    df_write_table = df.select(
        col("actor.id").alias("users_id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.url").alias("url"),
        col("actor.avatar_url").alias("avatar_url")
    )

    spark_configs = get_spark_config()

    df_write = SparkWriteDatabases(spark_connect.spark, spark_configs)

    df_write.spark_write_mysql(df_write_table, spark_configs["mysql"]["table"])

if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

schemaJson = StructType([
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

jsonData = spark.read.schema(schemaJson).json("C:/Users/ddb11/PycharmProjects/DE-ETL/Data/2015-03-01-17.json")
# jsonData.show(truncate=False)

# show cac cot trong actor
jsonData.select(col("id"), col("type"), col("actor.id").alias("actor_id"), col("actor.login").alias("actor_login"), col("actor.gravatar_id").alias("actor_gravatar_id"), col("actor.url").alias("actor_url"), col("actor.avatar_url").alias("actor_avatar_url")).show(truncate=False)


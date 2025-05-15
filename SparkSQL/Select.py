from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, length
from pyspark.sql.types import *

#create SparkSession

spark = SparkSession.builder\
    .appName("DE-ETL")\
    .master("local[*]")\
    .config("spark.executor.memory","2g")\
    .getOrCreate()

schemaJson = StructType([
    StructField("id", StringType(), True),
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
# # jsonData.show()
# jsonData.select(col("id"), col("actor.id").alias("actor.id")).show()
jsonData.select(
    col("actor.id").alias("actor_id") * 2,
    upper(col("type")).alias("Upper_Type")
)

jsonData.select(
    col("actor.id"),
    (col("actor.id") > 9614759).alias("actor_id > 9614759")
)

jsonData.selectExpr(
    "count(distinct(id)) as user_id",
    "count(distinct(actor.id)) as actor_id"
)

jsonData.select(col("id").alias("user_id")).filter(length(col("id")) < 11)

jsonData.select(col("id")).distinct()

jsonData.select(col("payload.issue.id").alias("payload_issue_id")).distinct().selectExpr("count(payload_issue_id) as id")

jsonData.select(col("payload.issue.id").alias("id")).dropDuplicates(["id"])

jsonData.sort(col("id")).select(col("id"))

jsonData.orderBy(col("id").desc()).show()
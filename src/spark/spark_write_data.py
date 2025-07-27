from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

from databases.mongodb_connect import MongoDBConnect
from databases.mysql_connect import MySQLConnect

from pymongo import MongoClient

from databases.schema_manager import create_mongodb_schema


class SparkWriteDatabases:
    def __init__(self, spark : SparkSession, db_config : Dict):
        self.spark = spark
        self.db_config = db_config

    def spark_write_mysql(self, df : DataFrame, mode : str = "append"):
        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        # python cursor add column spark_temp into mysql
        try:
            with MySQLConnect(mysql_config["config"]["host"], mysql_config["config"]["port"], mysql_config["config"]["user"],
                              mysql_config["config"]["password"]) as mysql_client:
                connection = mysql_client.connection
                cursor = mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN spark_temp VARCHAR(255)")
                connection.commit()
                print("--add column spark_temp to mysql--")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--fail to connect mysql: {e}")

        # spark write dataframe to mysql
        # jdbc_url = "jdbc:mysql://{}:{}/{}".format(mysql_config.host, mysql_config.port, mysql_config.database)
        df.write \
            .format("jdbc") \
            .option("url", mysql_config["jdbc_url"]) \
            .option("dbtable", table_name) \
            .option("user", mysql_config["config"]["user"]) \
            .option("password", mysql_config["config"]["password"]) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .mode(mode) \
            .save()
        print(f"--spark write data to mysql table : {table_name} success--")


    def validate_spark_mysql(self, df_write: DataFrame):
        mysql_config = self.db_config["mysql"]
        table_name = mysql_config["table"]
        df_read = self.spark.read \
            .format("jdbc") \
            .option("url", mysql_config["jdbc_url"]) \
            .option("dbtable", f"(SELECT * FROM {table_name} WHERE spark_temp = 'spark write') AS subq") \
            .option("user", mysql_config["config"]["user"]) \
            .option("password", mysql_config["config"]["password"]) \
            .option("driver", "com.mysql.jdbc.Driver") \
            .load()
        # df_read.show()

        def subtract_dataframe(df_spark_write: DataFrame, df_read_database: DataFrame, mode: str = "append"):
            result = df_spark_write.exceptAll(df_read_database)
            print(f"--records missing: {result.count()}--")

            if not result.isEmpty():
                result.write \
                    .format("jdbc") \
                    .option("url", mysql_config["jdbc_url"]) \
                    .option("dbtable", table_name) \
                    .option("user", mysql_config["config"]["user"]) \
                    .option("password", mysql_config["config"]["password"]) \
                    .option("driver", "com.mysql.jdbc.Driver") \
                    .mode(mode) \
                    .save()
                print(f"--spark write missing records to mysql table : {table_name} success--")

        # check count of records
        if df_write.count() == df_read.count():
            print(f"--validate {df_read.count()} records in mysql success--")
            subtract_dataframe(df_write, df_read)
        else:
            print(f"--spark inserted missing records in mysql--")
            subtract_dataframe(df_write, df_read)

        # drop column spark_temp
        try:
            with MySQLConnect(mysql_config["config"]["host"], mysql_config["config"]["port"], mysql_config["config"]["user"],
                              mysql_config["config"]["password"]) as mysql_client:
                connection = mysql_client.connection
                cursor = mysql_client.cursor
                database = "github_data"
                connection.database = database
                cursor.execute(f"ALTER TABLE {table_name} DROP COLUMN spark_temp")
                connection.commit()
                print("--drop column spark_temp to mysql--")
                mysql_client.close()
        except Exception as e:
            raise Exception(f"--fail to connect mysql: {e}")

        print("--validate spark write to mysql success--")

    def spark_write_mongodb(self, df : DataFrame, mode : str = "append"):
        mongodb_config = self.db_config["mongodb"]
        collection = mongodb_config["collection"]
        df.write \
            .format("mongo") \
            .mode(mode) \
            .option("uri", mongodb_config["uri"]) \
            .option("database", mongodb_config["database"]) \
            .option("collection", collection) \
            .save()

        print(f"--spark write data to mongodb collection : {collection} success--")

    def validate_spark_mongodb(self, df_write: DataFrame):
        query = {"spark_temp": "spark write"}
        mongodb_config = self.db_config["mongodb"]
        collection = mongodb_config["collection"]
        df_read = self.spark.read \
            .format("mongo") \
            .option("uri", mongodb_config["uri"]) \
            .option("database", mongodb_config["database"]) \
            .option("collection", collection) \
            .option("pipeline", str([{"$match" : query}])) \
            .load()
        df_read = df_read.select(col('user_id'), col('login'), col('gravatar_id'), col('url'), col('avatar_url'), col('spark_temp'))

        def subtract_dataframe_mongo(df_spark_write: DataFrame, df_read_database: DataFrame, mode: str = "append"):
            result = df_spark_write.exceptAll(df_read_database)
            missing_count = result.count()
            print(f"--Records missing in MongoDB: {missing_count}--")

            if missing_count > 0:
                print(f"--Writing {missing_count} missing records to MongoDB collection: {collection}--")
                result.write \
                    .format("mongo") \
                    .option("uri", mongodb_config["uri"]) \
                    .option("database", mongodb_config["database"]) \
                    .option("collection", collection) \
                    .mode(mode) \
                    .save()
                print(f"--Spark write missing records to MongoDB collection: {collection} success--")
            else:
                print("--No missing records found. Data is consistent.--")

        def drop_spark_temp_field_mongo():
            try:
                client = MongoClient(mongodb_config["uri"])
                db = client[mongodb_config["database"]]
                collection_obj = db[collection]
                filter_query = {"spark_temp": {"$exists": True}}
                update_operation = {"$unset": {"spark_temp": ""}}

                update_result = collection_obj.update_many(filter_query, update_operation)

                print(
                    f"--Dropped 'spark_temp' field from {update_result.modified_count} documents in collection '{collection}'--")

                client.close()
            except Exception as e:
                raise Exception(f"--Failed to connect to MongoDB or drop field: {e}")


        # check count of records
        if df_write.count() == df_read.count():
            print(f"--Count check passed. Validated {df_read.count()} records in MongoDB.--")
            subtract_dataframe_mongo(df_write, df_read)
        else:
            print(f"--Count mismatch. Spark will insert missing records.--")
            subtract_dataframe_mongo(df_write, df_read)

        # drop column spark_temp
        try:
            drop_spark_temp_field_mongo()
        except Exception as e:
            print(str(e))

        print("--Validation and cleanup for MongoDB completed successfully.--")

    def spark_write_all_databases(self, df : DataFrame, mode : str = "append"):
        try:
            self.spark_write_mysql(df, mode)
        except Exception as e:
            print(f"--spark write to mysql table failed: {str(e)}--")

        # try:
        #     self.spark_write_mongodb(df, mode)
        # except Exception as e:
        #     print(f"--spark write to mongodb collection failed: {str(e)}--")

        print("--spark write all dbs(mysql, mongodb) success--")

    def validate_spark_all_databases(self, df_write : DataFrame):
        try:
            self.validate_spark_mysql(df_write)
        except Exception as e:
            print(f"--spark write to mysql table failed: {str(e)}--")

        # try:
        #     self.validate_spark_mongodb(df_write)
        # except Exception as e:
        #     print(f"--spark write to mongodb collection failed: {str(e)}--")

        print("--spark validate all dbs(mysql, mongodb) success--")

    # NOTE: Database has primary key
    def spark_write_mysql_primaryKey(self, df: DataFrame, jdbc_url: str, config: Dict, mode: str = "append"):
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "spark_table_temp") \
            .option("user", config["user"]) \
            .option("password", config["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()

        print(f"-----spark write data to mysql table: spark_table_temp successfully-------")

    def validate_spark_write_primaryKey(self, df_write: DataFrame, jdbc_url: str, config: Dict,
                                        mode: str = "append"):
        try:
            df_read = self.spark.read \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "spark_table_temp") \
                .option("user", config["user"]) \
                .option("password", config["password"]) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            # df_read.show()
            df_temp = df_write.exceptAll(df_read)
            # print(df_temp.count())
            if df_temp.count() != 0:
                df_temp.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "spark_table_temp") \
                    .option("user", config["user"]) \
                    .option("password", config["password"]) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .mode(mode) \
                    .save()
            print(f"-------validate spark write data to mysql table spark_table_temp successfully------")
        except Exception as e:
            raise Exception(f"----failed to write missing record to spark_table_temp in mysql----")

    def insert_data_mysql_primaryKey(self, config: Dict):
        try:
            # Giả sử MySQLConnect và get_database_config được định nghĩa ở nơi khác
            with MySQLConnect(config["host"], config["port"], config["user"],
                              config["password"]) as mysql_client:
                connection, cursor = mysql_client.connection, mysql_client.cursor
                database = get_database_config()["mysql"]["database"]
                connection.database = database
                cursor.execute(
                    "SELECT a.* FROM spark_table_temp a LEFT JOIN users b ON a.user_id = b.user_id WHERE b.user_id IS NULL;")

                # Lấy tất cả các bản ghi cùng một lúc
                records = cursor.fetchall()

                for rec in records:
                    try:
                        # Giả sử các cột trong 'rec' khớp với thứ tự trong câu lệnh INSERT
                        # và bảng 'users' có các cột tương ứng
                        cursor.execute(
                            "INSERT INTO users (user_id, login, gravatar_id, url, avatar_url) VALUES (%s, %s, %s, %s, %s)",
                            rec
                        )
                        connection.commit()
                        print(f"-----inserted record {rec[0]} into mysql successfully-----")
                    except Exception as e:
                        print(f"Error inserting record {rec}: {str(e)}")
                        connection.rollback()  # Rollback nếu có lỗi cho bản ghi cụ thể
                        continue

        except Exception as e:
            raise Exception(f"-----failed to connect to mysql: {e}------")

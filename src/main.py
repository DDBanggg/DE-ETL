from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config
from databases.schema_manager import create_mysql_schema, validate_mysql_schema, EXPECTED_SCHEMA
from databases.mongodb_connect import MongoDBConnect
from databases.schema_manager import create_mongodb_schema, validate_mongodb_schema

def main(config):
    # MySQL
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user, config["mysql"].password) as mysql_client:
        connection = mysql_client.connection
        cursor = mysql_client.cursor
        create_mysql_schema(connection, cursor)
        cursor.execute("INSERT INTO users(user_id, login, gravatar_id, url, avatar_url) VALUES(%s, %s, %s, %s, %s)",
                       (1, "test", "", "https://test.com", "https://avatar.com"))
        connection.commit()
        print("--inserted data to mysql--")
        validate_mysql_schema(cursor, EXPECTED_SCHEMA)

    # MongoDB
    # with MongoDBConnect(config["mongodb"].uri, config["mongodb"].db_name) as mongo_client:
    #     create_mongodb_schema(mongo_client.db)
    #     mongo_client.db.users.insert_one({
    #         "user_id" : 1,
    #         "login" : "bangg",
    #         "gravatar_id" : "test_gravatar_id",
    #         "url" : "https://testurl.url",
    #         "avatar_url" : "https://avatarurl.url"
    #     })
    #     print("--inserted 1 record in mongodb--")
    #
    #     validate_mongodb_schema(mongo_client.db)

if __name__ == "__main__":
    config = get_database_config()
    main(config)
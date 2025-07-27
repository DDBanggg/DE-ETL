from enum import unique
from pathlib import Path
from mysql.connector import Error

SQL_FILE_PATH = Path("../sql/schema.sql")

# ĐỊNH NGHĨA TOÀN BỘ CẤU TRÚC
EXPECTED_SCHEMA = {
    "users": [
        "user_id",
        "login",
        "url",
        "avatar_url"
    ],
    "repositories": [
        "repo_id",
        "name",
        "url"
    ]
}

# Tạo Bảng trong MYSQL
def create_mysql_schema(connection, cursor):
    database = "github_data"
    cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    connection.commit()
    connection.database = database
    try:
        with open(SQL_FILE_PATH, "r") as files:
            sql_script = files.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

            for cmd in sql_commands:
                cursor.execute(cmd)
                print(f"--Executed mysql commands--")
            connection.commit()
    except Error as e:
        connection.rollback()
        raise Exception(f"--Failed to create Mysql schema: {e}--") from e

# Kiểm tra dữ liệu MYSQL
def validate_mysql_schema(cursor, schema_definition):
    # === Bước 1 (Validate): Kiểm tra sự tồn tại của tất cả các bảng được định nghĩa ===
    required_tables = [table.lower() for table in schema_definition.keys()]

    cursor.execute("SHOW TABLES")
    all_rows_tables = cursor.fetchall()
    tables_in_db = [row[0].lower() for row in all_rows_tables]

    for table_name in required_tables:
        if table_name not in tables_in_db:
            raise ValueError(f"---Schema validation failed: Table '{table_name}' does not exist.---")

    print(f"--All required tables exist: {', '.join(required_tables)}.--")

    # === Bước 2 (Validate): Lặp qua từng bảng và kiểm tra các cột của nó ===
    for table_name, required_columns in schema_definition.items():

        cursor.execute(f"DESCRIBE {table_name}")
        all_columns_info = cursor.fetchall()  #
        columns_in_table = [col_info[0].lower() for col_info in all_columns_info]

        for column_name in required_columns:
            if column_name.lower() not in columns_in_table:
                raise ValueError(
                    f"---Schema validation failed: Column '{column_name}' does not exist in table '{table_name}'.---")

        print(f"--Table '{table_name}' has all required columns.--")

    print("--Schema validation successful.--")

# Tao bang MONGODB
def create_mongodb_schema(db):
    db.drop_collection("users")
    db.create_collection("users", validator={
        "$jsonSchema": {
            "bsonType" : "object",
            "required" : ["user_id", "login"],
            "properties" : {
                "user_id" : {
                    "bsonType" : "int"
                },
                "login" : {
                    "bsonType": "string"
                },
                "gravatar_id" : {
                    "bsonType": ["string", "null"]
                },
                "url" : {
                    "bsonType": ["string", "null"]
                },
                "avatar_url" : {
                    "bsonType": ["string", "null"]
                }
            }
        }
    })

    # db.users.create_index("user_id", unique=True)
    print("--created collection users in Mongodb--")

# Kiem tra MONGODB
def validate_mongodb_schema(db):
    collections = db.list_collection_names()
    # print(f"--collection: {collections}")
    if "users" not in collections:
        raise ValueError(f"--collections in Mongodb doesn't exist--")

    user = db.users.find_one({"user_id" : 1})
    # print(user)
    if not user:
        raise ValueError(f"--user_id not found in Mongodb--")
    print("--validate schema in Mongodb success--")
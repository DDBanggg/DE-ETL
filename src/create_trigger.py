import sys
from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config


CREATE_TABLE_USER_LOG_BEFORE = """
CREATE TABLE user_log_before(
    user_id BIGINT ,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY,
    message_id BIGINT SERIAL KEY
)
"""

CREATE_TABLE_USER_LOG_AFTER = """
CREATE TABLE user_log_after(
    user_id BIGINT,
    login VARCHAR(255) ,
    gravatar_id VARCHAR(255) ,
    url VARCHAR(255) ,
    avatar_url VARCHAR(255),
    state VARCHAR(255),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) PRIMARY KEY,
    message_id BIGINT SERIAL KEY
)
"""

# Câu lệnh tạo trigger
CREATE_TRIGGER_BEFORE_INSERT = """
CREATE TRIGGER before_insert_users
BEFORE INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id, NEW.url, NEW.avatar_url, "INSERT");
END
"""

CREATE_TRIGGER_BEFORE_UPDATE = """
CREATE TRIGGER before_update_users
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, "UPDATE");
END
"""

CREATE_TRIGGER_BEFORE_DELETE = """
CREATE TRIGGER before_delete_users
BEFORE DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_before(user_id, login, gravatar_id, url,avatar_url,state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id, OLD.url, OLD.avatar_url, "DELETE");
END
"""

CREATE_TRIGGER_AFTER_UPDATE = """
CREATE TRIGGER after_update_users
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id,NEW.avatar_url ,NEW.url, "UPDATE");
END
"""

CREATE_TRIGGER_AFTER_INSERT = """
CREATE TRIGGER after_insert_users
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (NEW.user_id, NEW.login, NEW.gravatar_id,NEW.avatar_url ,NEW.url, "INSERT");
END
"""

CREATE_TRIGGER_AFTER_DELETE = """
CREATE TRIGGER after_delete_users
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    INSERT INTO user_log_after(user_id,login, gravatar_id,  avatar_url, url, state)
    VALUES (OLD.user_id, OLD.login, OLD.gravatar_id,OLD.avatar_url ,OLD.url, "DELETE");
END
"""


def execute_statements_manually():
    config = get_database_config()
    try:
        with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:

            connection = mysql_client.connection
            cursor = mysql_client.cursor
            database = "github_data"
            connection.database = database
            print(f"--Đã kết nối tới database '{database}'--\n")


            cursor.execute(CREATE_TABLE_USER_LOG_BEFORE)
            print("--Tạo bảng user_log_before thành công--\n")

            cursor.execute(CREATE_TABLE_USER_LOG_AFTER)
            print("--Tạo bảng user_log_after thành công--\n")

            cursor.execute(CREATE_TRIGGER_BEFORE_INSERT)
            print("--Tạo trigger before_insert_users thành công--\n")

            cursor.execute(CREATE_TRIGGER_BEFORE_UPDATE)
            print("--Tạo trigger before_update_users thành công--\n")

            cursor.execute(CREATE_TRIGGER_BEFORE_DELETE)
            print("--Tạo trigger before_delete_users thành công--\n")

            cursor.execute(CREATE_TRIGGER_AFTER_UPDATE)
            print("--Tạo trigger after_update_users thành công--\n")

            cursor.execute(CREATE_TRIGGER_AFTER_INSERT)
            print("--Tạo trigger after_insert_users thành công--\n")

            cursor.execute(CREATE_TRIGGER_AFTER_DELETE)
            print("--Tạo trigger after_delete_users thành công--\n")

            connection.commit()
            print("-- HOÀN TẤT --\n")

    except Exception as e:
        print(f"-- Đã xảy ra lỗi: {e}--")
        sys.exit(1)


if __name__ == "__main__":
    execute_statements_manually()
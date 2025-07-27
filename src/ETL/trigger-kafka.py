import time
import json
import uuid
from kafka import KafkaProducer
from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config

TIMESTAMP_FILE = 'last_timestamp.txt'


def read_last_timestamp():
    try:
        with open(TIMESTAMP_FILE, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None


def write_last_timestamp(timestamp):
    with open(TIMESTAMP_FILE, 'w') as f:
        f.write(str(timestamp))


def get_data_trigger(mysql_client, last_timestamp):
    connection, cursor = mysql_client.connection, mysql_client.cursor
    connection.database = "github_data"

    query = ("SELECT message_id, user_id, login, gravatar_id, url, avatar_url, state, "
             "DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp "
             "FROM user_log_after")

    if last_timestamp:
        query += " WHERE log_timestamp > %s ORDER BY log_timestamp ASC"
        cursor.execute(query, (last_timestamp,))
    else:
        query += " ORDER BY log_timestamp ASC"
        cursor.execute(query)

    rows = cursor.fetchall()
    connection.commit()

    if not rows:
        return [], last_timestamp

    schema = ["message_id", "user_id", "login", "gravatar_id", "url", "avatar_url", "state", "log_timestamp"]
    data = [dict(zip(schema, row)) for row in rows]

    max_timestamp = data[-1]["log_timestamp"]
    return data, max_timestamp


def main():
    last_timestamp = read_last_timestamp()
    config = get_database_config()
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print("--Kafka Producer đã khởi tạo và sẵn sàng--")

    try:
        with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:
            while True:
                data, new_timestamp = get_data_trigger(mysql_client, last_timestamp)

                if data:
                    batch_id = str(uuid.uuid4())

                    for record in data:
                        record['batch_id'] = batch_id

                    for record in data:
                        producer.send("bangg", record)

                    control_message = {
                        "batch_id": batch_id,
                        "expected_count": len(data),
                        "message_id_start": data[0]['message_id'],
                        "message_id_end": data[-1]['message_id']
                    }
                    print(f"--Gửi tin nhắn kiểm soát cho lô {batch_id} vào bangg_controller--")
                    producer.send("bangg_control", control_message)

                    producer.flush()

                    last_timestamp = new_timestamp
                    write_last_timestamp(last_timestamp)
                    print(f"--Cập nhật thành công. Timestamp mới nhất: {last_timestamp}--")
                else:
                    print("--Không có dữ liệu mới--")

                time.sleep(5)
    except KeyboardInterrupt:
        print("\n--Đang dừng...--")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
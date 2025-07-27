import json
from kafka import KafkaConsumer

# Khởi tạo consumer với group_id
consumer = KafkaConsumer(
    "bangg",
    bootstrap_servers="localhost:9092",
    group_id="bangg"
)

print("Consumer đang lắng nghe topic 'bangg'...")

try:
    while True:
        msg_pack = consumer.poll(timeout_ms=1000)

        for tp, messages in msg_pack.items():
            for message in messages:
                print(message.value.decode('utf-8'))

except KeyboardInterrupt:
    print("\nĐang dừng consumer...")
finally:
    consumer.close()
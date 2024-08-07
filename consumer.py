import json
from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['test_topic'])

try:
    while True:
        msg = c.poll(1.0)  # 1.0秒的等待時間

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # 將接收的消息從 JSON 字符串反序列化為 Python 字典
        msg_data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {msg_data}")

except KeyboardInterrupt:
    pass
finally:
    c.close()

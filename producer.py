import json
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Kafka 配置
p = Producer({'bootstrap.servers': 'localhost:9092'})

# 要發送的數據
data = {'id': 5, 'name': 'John Doe'}

# 將字典轉換為 JSON 字符串
data_json = json.dumps(data)

# 發送消息
p.produce('test_topic', data_json.encode('utf-8'), callback=delivery_report)

# 等待所有消息發送完成
p.flush()
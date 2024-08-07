from confluent_kafka.admin import AdminClient, NewTopic

def create_topic(topic_name, num_partitions, replication_factor):
    admin_client = AdminClient({
        'bootstrap.servers': 'localhost:9092'
    })

    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    
    # Create the topic
    result = admin_client.create_topics([topic])
    for topic, f in result.items():
        try:
            f.result()  # Wait for the result
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

if __name__ == "__main__":
    create_topic("test_topic", 1, 1)

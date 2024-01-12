from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, bootstrap_servers, topic_name, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            group_id=group_id,
            enable_auto_commit=True,  # Set to False if you want to manually commit offsets
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def consume(self):
        for message in self.consumer:
            yield message.value
    
    def __close__(self):
        self.consumer.close()

if __name__ == '__main__':
    consumer = Consumer('localhost:29092', 'hanoi_weather_data', 'group_01')
    for data in consumer.consume():
        print(data)
        print('-----------------------')

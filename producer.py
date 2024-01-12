import requests
from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")

class Producer:
    def __init__(self, bootstrap_servers, topic_name):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def publish(self, data):
        for item in data:
            self.producer.send(KAFKA_TOPIC_NAME, value=item)

    def __close__(self):
        self.producer.close()

if __name__ == "__main__":
    movie_producer = Producer(KAFKA_BOOTSTRAP_SERVER, KAFKA_TOPIC_NAME)

    for i in range(10):
        data = [{'id': i}]    
        movie_producer.publish(data)
        time.sleep(2)

    movie_producer.__close__()

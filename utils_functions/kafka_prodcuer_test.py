from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

from flask import Flask, jsonify,request
from threading import Thread
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

KAFKA_BROKER = "localhost:9092"
TOPIC = "test-topic"

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def generate_interaction():
    return {
        "user_id": random.randint(1, 20),
        "item_id": random.randint(1, 5),
        "interaction_type": random.choice(["click", "view", "purchase"]),
        "timestamp": datetime.utcnow().isoformat()
    }

def produce_messages():
    while True:
        data = generate_interaction()
        producer.produce(TOPIC, key=str(data["user_id"]), value=json.dumps(data))
        producer.flush()
        print(f"Data Sent To Kafka Topic : {data}")
        time.sleep(5)

if __name__ == "__main__":
    produce_messages()

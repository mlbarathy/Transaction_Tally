from confluent_kafka import Producer
import json
import time
import uuid
import os

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'test-topic'
SAMPLE_FILE = 'sample_json.txt'  # Path to your sample file

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def load_json_from_file():
    try:
        with open(SAMPLE_FILE, 'r') as file:
            content = file.read()
            data = json.loads(content)
            if isinstance(data, dict):
                return [data]
            return data  # assume list of messages
    except Exception as e:
        print(f"❌ Error reading/parsing '{SAMPLE_FILE}': {e}")
        return []
sec = 60
def produce_messages_from_file():
    print(f"🔁 Producing messages to Kafka from file every {sec} seconds...")
    while True:
        messages = load_json_from_file()  # re-read every time

        if not messages:
            print("⚠️ No messages found in file.")
        for message in messages:
            msg_id = (
                message.get('BkToCstmrStmt', {})
                .get('GrpHdr', {})
                       .get('MsgId', f"MSG{uuid.uuid4().hex[:10].upper()}")
            )
            producer.produce(
                topic=TOPIC,
                key=msg_id,
                value=json.dumps(message),
                callback=delivery_report
            )
            producer.poll(0)

        time.sleep(sec)

if __name__ == "__main__":
    produce_messages_from_file()

from confluent_kafka import Producer
import os
import json
import time
from mongo_connection import mycollection

kafka_k = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = os.getenv("KAFKA_TOPIC")

producer_config = {"bootstrap.servers": kafka_k}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(
            f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}"
        )


def read_from_db(collection):

    db_length = collection.count_documents({})

    for skip in range(0, db_length, 40):
        data = list(collection.find({}, {"_id": 0}).skip(skip).limit(40))

        for user in data:
            value = json.dumps(user).encode("utf-8")

            producer.produce(topic=kafka_topic, value=value, callback=delivery_report)

            producer.poll(0)

            time.sleep(0.5)

    producer.flush()


read_from_db(mycollection)
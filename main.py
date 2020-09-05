import json
import logging
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = "localhost:9092"
PAGES = ("foo", "bar")


def timestamp():
    """Generate timestamp in military ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        while True:
            message = self.generate_message()
            # send message with updated timestamp
            producer.send("my-topic", {**message, "timestamp": timestamp()})
            time.sleep(random.randint(1, 5))

    def generate_message(self):
        return {
            "cookie": uuid.uuid4().hex,
            "campId": random.choice(PAGES),
            "isFake": random.getrandbits(1),
        }


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        consumer.subscribe(["my-topic"])
        for i, message in enumerate(consumer, start=1):
            print(message.value)
            # if message.value["isFake"]:
            #     print("bot")


def main():
    threads = (Producer(), Consumer())
    for t in threads:
        t.start()
    time.sleep(100)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    main()

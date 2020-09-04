import json
import logging
import random
import threading
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

KAFKA_SERVER = "localhost:9092"


class Producer(threading.Thread):
    daemon = True
    messages = (
        {
            "cookie": "299a0be4a5a79e6a59fdd251b19d78bb",
            "campId": "foo",
            "isFake": 1,
        },
        {
            "cookie": "fa85cca91963d8f301e34247048fca39",
            "campId": "bar",
            "isFake": 0,
        },
    )

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        while True:
            # pick a random message to send
            message = random.choice(self.messages)
            # send message with updated timestamp
            producer.send("my-topic", {**message, "timestamp": self.timestamp()})
            time.sleep(random.randint(1, 5))

    def timestamp(self):
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVER,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        # producer = KafkaProducer(
        #     bootstrap_servers=KAFKA_SERVER,
        #     value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        # )
        consumer.subscribe(["my-topic"])
        bots = 0
        for i, message in enumerate(consumer, start=1):
            print(message.value)
            if message.value["isFake"] == 1:
                bots += 1
                print("bot")
            print(bots / i)
            # producer.send("output-topic", {"campaign": message.value["campId"], "clickFraud": message.value})


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

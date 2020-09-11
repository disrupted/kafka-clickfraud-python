import json
import logging
import random
import time
import uuid
from dataclasses import asdict
from datetime import datetime, timezone

from kafka import KafkaProducer

from const import CAMPAIGNS, KAFKA_INPUT_TOPIC, KAFKA_SERVER_HOST
from faustapp import Click

_LOGGER = logging.getLogger(__name__)


def timestamp():
    """Generate timestamp in military ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Producer:
    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER_HOST,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )
        while True:
            message = asdict(self.generate_message())
            print(message)
            producer.send(KAFKA_INPUT_TOPIC, message)
            time.sleep(random.randint(1, 5))

    def generate_message(self):
        """Generate a new Click event with current timestamp."""
        click = Click(
            cookie=uuid.uuid4().hex,
            campId=random.choice(CAMPAIGNS),
            isFake=random.getrandbits(1),
            timestamp=timestamp(),
        )
        return click


def main():
    Producer().run()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    main()

import datetime
import random
import uuid
from dataclasses import asdict, dataclass

import faust

from const import (
    CAMPAIGNS,
    CLICK_INTERVAL,
    KAFKA_INPUT_TOPIC,
    KAFKA_OUTPUT_TOPIC,
    KAFKA_SERVER_HOST,
)


@dataclass
class Click(faust.Record):
    cookie: str
    campId: str
    isFake: bool
    timestamp: datetime.datetime


@dataclass
class Statistic(faust.Record):
    campaign: str
    clickFraud: float


app = faust.App(
    __name__,
    topic_partitions=1,
    broker="kafka://" + KAFKA_SERVER_HOST,
    value_serializer="json",
)
click_topic = app.topic(KAFKA_INPUT_TOPIC, value_type=Click, partitions=1)
destination_topic = app.topic(KAFKA_OUTPUT_TOPIC)

total_counts = app.Table("total_counts", partitions=1, default=int)
bot_counts = app.Table("bot_counts", partitions=1, default=int)


def timestamp():
    """Generate timestamp in military ISO 8601 format."""
    return (
        datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )


def generate_message():
    """Generate a new Click event with current timestamp."""
    click = Click(
        cookie=uuid.uuid4().hex,
        campId=random.choice(CAMPAIGNS),
        isFake=random.getrandbits(1),
        timestamp=timestamp(),
    )
    return click


@app.timer(CLICK_INTERVAL)
async def populate():
    message = asdict(generate_message())
    await click_topic.send(value=message)


@app.agent(click_topic, sink=[destination_topic])
async def count_click(clicks):
    async for click in clicks.group_by(Click.campId):
        print(click)
        total_counts[click.campId] += 1
        if click.isFake:
            bot_counts[click.campId] += 1
        message = Statistic(
            campaign=click.campId, clickFraud=calculate_click_fraud(click.campId)
        )
        print(message)
        yield message


def calculate_click_fraud(campId):
    return bot_counts[campId] / total_counts[campId]


if __name__ == "__main__":
    app.main()

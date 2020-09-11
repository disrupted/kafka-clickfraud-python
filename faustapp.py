import datetime
from dataclasses import dataclass

import faust

from const import KAFKA_INPUT_TOPIC, KAFKA_OUTPUT_TOPIC, KAFKA_SERVER_HOST


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

counts = app.Table("click_counts", partitions=1, default=int)
bot_counts = app.Table("bot_counts", partitions=1, default=int)


@app.agent(click_topic, sink=[destination_topic])
async def count_click(clicks):
    async for click in clicks.group_by(Click.campId):
        print(click)
        counts[click.campId] += 1
        if click.isFake:
            bot_counts[click.campId] += 1
        message = Statistic(
            campaign=click.campId, clickFraud=await calculate_click_fraud(click.campId)
        )
        print(message)
        yield message


async def get_total_count(campId):
    return counts[campId]


async def get_bot_count(campId):
    return bot_counts[campId]


async def calculate_click_fraud(campId):
    return await get_bot_count(campId) / await get_total_count(campId)


if __name__ == "__main__":
    app.main()

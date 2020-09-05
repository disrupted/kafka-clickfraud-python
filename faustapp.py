import datetime
from dataclasses import dataclass

import faust


@dataclass
class Click(faust.Record):
    cookie: str
    campId: str
    isFake: bool
    timestamp: datetime.datetime


app = faust.App(
    __name__,
    topic_partitions=1,
    broker="kafka://localhost:9092",
    value_serializer="json",
)
click_topic = app.topic("my-topic", value_type=Click, partitions=1)
destination_topic = app.topic("destination-topic")

counts = app.Table("click_counts", partitions=1, default=int)
bot_counts = app.Table("bot_counts", partitions=1, default=int)


@app.agent(click_topic)
async def count_click(clicks):
    async for click in clicks.group_by(Click.campId):
        print(click)
        counts[click.campId] += 1
        if click.isFake:
            bot_counts[click.campId] += 1
        new_message = {
            "campaign": click.campId,
            "clickFraud": await calculate_click_fraud(click.campId),
        }
        print(new_message)
        await destination_topic.send(new_message)


async def get_total_count(campId):
    return counts[campId]


async def get_bot_count(campId):
    return bot_counts[campId]


async def calculate_click_fraud(campId):
    return await get_bot_count(campId) / await get_total_count(campId)


if __name__ == "__main__":
    app.main()

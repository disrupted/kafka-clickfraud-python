from kafka import KafkaConsumer
from pprint import pprint

consumer = KafkaConsumer('quickstart-events')

# pprint(consumer.metrics())
for msg in consumer:
    print(msg)

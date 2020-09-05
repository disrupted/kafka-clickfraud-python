from kafka import KafkaProducer
from pprint import pprint
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send('quickstart-events', {'foo': 'bar'})

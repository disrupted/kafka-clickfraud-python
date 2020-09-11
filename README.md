# kafka-clickfraud-python
> Kafka App calculating a "click fraud" score based on an incoming stream of click events indicating fake clicks by bots

Built in Python using the [Faust](https://github.com/robinhood/faust) stream processing library.

## Example input topic

```json
{
  "cookie": "879e134ede5a46798187e655b5435c2d", 
  "campId": "foo", 
  "isFake": 0, 
  "timestamp": "2020-09-11T10:53:09.810150Z"
}
{
  "cookie": "10cb2ef94b9641aeb901976a1b4817da", 
  "campId": "bar", 
  "isFake": 1, 
  "timestamp": "2020-09-11T10:53:15.823862Z"
}
{
  "cookie": "0b4e5785c82740ef987bc985f2c9196c", 
  "campId": "bar", 
  "isFake": 0, 
  "timestamp": "2020-09-11T10:53:33.843298Z"
}
```

## Example output topic

```json
{
  "campaign": "foo",
  "clickFraud": 0.15
}
{
  "campaign": "bar",
  "clickFraud": 0.32
}
{
  "campaign": "bar",
  "clickFraud": 0.32
}
```

## Installation

Clone the repository, navigate inside it and install faust dependency

```sh
git clone https://github.com/disrupted/kafka-clickfraud-python
cd kafka-clickfraud-python
pip install -U faust
```

requires Python 3.7 or later

## Usage

Start the Zookeeper & Kafka server stack

```sh
docker-compose up
```

Open a separate shell and start the app

```sh
python3 faustapp.py worker -l info
```

Use `kafka-console-consumer` or another client to subscribe to the Kafka topics `streams-clickfraud-input` and `streams-clickfraud-output` to monitor the randomly generated Click events and the calculated Click fraud score as messages from the application.

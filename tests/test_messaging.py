# pylint: disable=missing-function-docstring,broad-exception-caught

"""
Test suite for the messaging producer functionality using pytest and Redpanda as the Kafka broker.
This module includes fixtures and test cases to validate message production and delivery reports.

Fixtures:
    kafka_container: A pytest fixture that sets up a Redpanda container for testing.

Tests:
    test_multiply: A simple test to validate multiplication logic.
    test_produce_message: Tests the message production functionality by
    producing a message to the Kafka topic.

Examples:
    To run the tests, use the pytest command in the terminal.
"""
import json
import os
from typing import Dict

import pytest
from confluent_kafka import Producer, Consumer, TopicPartition

os.environ["RYUK_CONTAINER_IMAGE"] = "testcontainers/ryuk:0.9.0"
from testcontainers.kafka import RedpandaContainer


@pytest.fixture(scope="module", name="shared_data")
def shared():
  # Setup code
  data = {"m0": (None, None, None)}
  print(data)
  yield data  # This is the data that will be shared  # Teardown code (if needed)


@pytest.fixture(scope="session", name="kafka_broker")
def kafka_container():
  """
  Test suite for the messaging producer functionality using pytest and Redpanda as the Kafka broker.
  This module includes fixtures and test cases to validate message production and delivery reports.

  Fixtures:
      kafka_container: A pytest fixture that sets up a Redpanda container for testing.

  Tests:
      test_multiply: A simple test to validate multiplication logic.
      test_produce_message: Tests the message production functionality
      by producing a message to the Kafka topic.

  Examples:
      To run the tests, use the pytest command in the terminal.
  """

  with RedpandaContainer(
      image="docker.redpanda.com/redpandadata/redpanda:v24.2.11") as redpanda:
    bootstrap_server = redpanda.get_bootstrap_server()
    yield {"bootstrap_servers": bootstrap_server}  #


def test_produce_message(kafka_broker, shared_data):
  # Create a producer
  bootstrap_servers = kafka_broker["bootstrap_servers"]
  produce_message(bootstrap_servers, "testcontainer", shared_data)


def produce_message(bootstrap_servers, name: str, data_holder: Dict):
  """
  Produce a message to a Kafka topic.

  This function produces a message to a Kafka topic using a producer. The
  message is a JSON object with a single key-value pair, where the key is
  "data" and the value is "1". The message is produced to the topic named by
  the 'topic' variable.

  The function takes a single argument, 'bootstrap_servers', which is a
  string that specifies the address of the Kafka broker to connect to.

  The function is used in the test suite to test the messaging producer
  functionality.

  :param name:
  :param data_holder:
  :param str bootstrap_servers: The address of the Kafka broker to connect
    to.
  """

  def message_info(err, message):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
      print(f'Message delivery failed: {err}')
    else:
      m = json.loads(message.value().decode('utf-8'))
      print(f'Message delivered to topic - {message.topic()} partition -'
            f' [{message.partition()}] - data {message.value()} - '
            f'offset is {message.offset()} - message is of type {type(message)}')
      data_holder[m.get("name", "m0")] = (
        message.topic(), message.partition(), message.offset())

  producer = Producer({'bootstrap.servers': bootstrap_servers})
  producer.poll(0)
  producer.produce("orders",
                   json.dumps({"data": "1", "name": name}).encode('utf-8'),
                   callback=message_info)
  producer.flush()
  print(data_holder)


def test_consume_message(kafka_broker, shared_data):
  bootstrap_servers = kafka_broker["bootstrap_servers"]
  assert consume_message(bootstrap_servers)["data"] == "1"


def consume_message(bootstrap_servers, topic="orders", offset=None,
    partition=0) -> Dict:
  consumer = Consumer({'bootstrap.servers': bootstrap_servers,
                       'group.id': 'doogle-test-consumer-group',
                       'auto.offset.reset': 'earliest'})
  if offset:
    # Assign the specific partition and offset
    consumer.assign([TopicPartition(topic, partition, offset), ])
  else:
    consumer.subscribe(["orders"])
  try:
    msg = consumer.poll(1.0)
    print(json.dumps(
        {"message": f'''Received message: {msg.value().decode('utf-8')},
      offset {msg.offset()}
      topic {msg.topic()}
      partition {msg.partition()}
      key {msg.key()}
      timestamp {msg.timestamp()}
      headers {msg.headers()}
'''}))
    return json.loads(msg.value().decode('utf-8'))
  except Exception as e:
    print(e)
    return {}
  finally:
    consumer.close()

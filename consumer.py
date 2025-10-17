import json
from confluent_kafka import Consumer

consumer_config ={
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print("Listening for messages on 'orders' topic...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Received message: {msg.value().decode('utf-8')}")


 
    value = msg.value().decode('utf-8')
    order = json.loads(value)
    print(f"Order received: {order['quantity']}")
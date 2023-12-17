from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

conf = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(conf)

try:
    producer.produce('hello_world_topic', key='hello', value='world', callback=delivery_report)
    producer.flush()
except Exception as e:
    print(f'An error occurred: {e}')

print("Message sent successfully!")

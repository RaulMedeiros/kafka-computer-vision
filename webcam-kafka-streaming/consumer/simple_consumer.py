from confluent_kafka import Consumer

def simple_kafka_consumer():
    # Consumer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'hello_world_group',
        'auto.offset.reset': 'earliest'
    }

    # Create the Consumer instance
    consumer = Consumer(conf)

    # Subscribe to the topic
    consumer.subscribe(['hello_world_topic'])

    try:
        while True:
            # Poll for a message
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Print the message
            print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        print("Consumer closed.")
    finally:
        # Close down the consumer
        consumer.close()

if __name__ == '__main__':
    simple_kafka_consumer()

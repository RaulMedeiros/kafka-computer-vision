# from confluent_kafka import Consumer, KafkaError
# import numpy as np
# import cv2
# import base64

# def create_consumer():
#     conf = {
#         'bootstrap.servers': 'localhost:9092',
#         'group.id': 'mygroup',
#         'auto.offset.reset': 'earliest'
#     }
#     return Consumer(conf)

# def consume_messages(consumer):
#     try:
#         consumer.subscribe(['webcam_topic'])

#         while True:
#             msg = consumer.poll(1.0)

#             if msg is None:
#                 continue
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     continue
#                 else:
#                     print(msg.error())
#                     break

#             # Decodificar a imagem
#             img = base64.b64decode(msg.value())
#             npimg = np.frombuffer(img, dtype=np.uint8)
#             frame = cv2.imdecode(npimg, 1)

#             # Inverter a imagem novamente
#             frame = cv2.flip(frame, 1)

#             # Exibir a imagem
#             cv2.imshow('Received Webcam', frame)

#             if cv2.waitKey(1) & 0xFF == ord('q'):
#                 break

#     finally:
#         consumer.close()
#         cv2.destroyAllWindows()

# consumer = create_consumer()
# consume_messages(consumer)

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

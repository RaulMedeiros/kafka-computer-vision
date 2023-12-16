from confluent_kafka import Consumer, KafkaError
import numpy as np
import cv2
import base64

def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def consume_messages(consumer):
    try:
        consumer.subscribe(['webcam_topic'])

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Decodificar a imagem
            img = base64.b64decode(msg.value())
            npimg = np.frombuffer(img, dtype=np.uint8)
            frame = cv2.imdecode(npimg, 1)

            # Inverter a imagem novamente
            frame = cv2.flip(frame, 1)

            # Exibir a imagem
            cv2.imshow('Received Webcam', frame)

            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

    finally:
        consumer.close()
        cv2.destroyAllWindows()

consumer = create_consumer()
consume_messages(consumer)

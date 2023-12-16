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


# import cv2
# from confluent_kafka import Producer
# import json
# import base64

# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Message delivery failed: {err}')
#     else:
#         print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# # Configuração do produtor Kafka
# p = Producer({'bootstrap.servers': 'localhost:9092'})

# def send_frame(frame):
#     _, buffer = cv2.imencode('.jpg', frame)
#     jpg_as_text = base64.b64encode(buffer).decode()
#     p.produce('webcam_topic', jpg_as_text, callback=delivery_report)

# def capture_and_send():
#     cap = cv2.VideoCapture(0)  # 0 é normalmente a webcam integrada

#     while True:
#         ret, frame = cap.read()
#         if not ret:
#             break

#         # Inverter a imagem
#         frame = cv2.flip(frame, 1)

#         # Enviar frame
#         send_frame(frame)

#         # Exibir a imagem localmente (opcional)
#         cv2.imshow('Webcam', frame)

#         # Break se 'q' for pressionado
#         if cv2.waitKey(1) & 0xFF == ord('q'):
#             break

#     cap.release()
#     cv2.destroyAllWindows()
#     # p.flush()

# if __name__ == "__main__":
#     capture_and_send()

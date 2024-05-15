import pika
import uuid
import json
import time

QUEUE_NAME = 'test'

class Publisher:

    channel = None
    conn = None

    def __init__(self):
        credentials = pika.PlainCredentials('guest', 'guest')
        parameters = pika.ConnectionParameters('rabbitmq',
                                       5672,
                                       '/',
                                       credentials)
        try:
            self.conn = pika.BlockingConnection(parameters)
        except Exception as e:
            time.sleep(10)
            self.conn = pika.BlockingConnection(parameters)

        self.channel = self.conn.channel()
        self.callback_queue = self.channel.queue_declare(queue='', exclusive=True).method.queue
        self.channel.basic_consume(queue=self.callback_queue, on_message_callback=self.on_response, auto_ack=True)
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = body

    def publish(self, request_type, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        message = {'request_type': request_type, 'data': data}
        self.channel.basic_publish(
            exchange='', routing_key=QUEUE_NAME,
            properties=pika.BasicProperties(reply_to=self.callback_queue, correlation_id=self.corr_id),
            body=json.dumps(message)
        )
        while self.response is None:
            self.connection.process_data_events()
        return self.response.decode()
    
    def close(self):
        self.conn.close()

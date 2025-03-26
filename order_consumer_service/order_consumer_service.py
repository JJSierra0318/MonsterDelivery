from flask import Flask, Response
from pymongo import MongoClient
import pika
import json
import threading
import queue
import time
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

order_queue = queue.Queue()

RABBITMQ_URL = "amqps://fojwrukx:ccIIphYJo1e0NTQ5olI1zSonirsuRAFs@fly.rmq.cloudamqp.com/fojwrukx"
params = pika.URLParameters(RABBITMQ_URL)

client = MongoClient("mongodb://localhost:27017/")
db = client['mi_base_de_datos']
coleccion = db['mi_coleccion']

def consume_orders():
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.queue_declare(queue='order')

            def callback(ch, method, properties, body):
                order_data = json.loads(body)
                logging.info(f"Order received: {order_data}")
                order_queue.put(order_data)
                try:
                    result = coleccion.insert_one(order_data)
                    logging.info(f"Order inserted in MongoDB with id: {result.inserted_id}")
                except Exception as e:
                    logging.error(f"Error inserting order into MongoDB: {e}")

            channel.basic_consume(queue='order', on_message_callback=callback, auto_ack=True)
            logging.info("Waiting for messages...")
            channel.start_consuming()

        except Exception as e:
            logging.error(f"RabbitMQ connection error: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)

threading.Thread(target=consume_orders, daemon=True).start()

@app.route('/orders/stream', methods=['GET'])
def stream_orders():
    def event_stream():
        while True:
            order = order_queue.get() 
            yield f"data: {json.dumps(order)}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(debug=True, threaded=True)




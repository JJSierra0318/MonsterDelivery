from flask import Flask, request, jsonify, json
import logging
import pika

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

def publish_order(order_data):
    logging.info(f"Order received: {order_data}")
    url = "amqps://fojwrukx:ccIIphYJo1e0NTQ5olI1zSonirsuRAFs@fly.rmq.cloudamqp.com/fojwrukx"
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='order')
    channel.basic_publish(exchange='',
                      routing_key='order',
                      body=json.dumps(order_data))
    print(" [x] Sent order data to queue broker")
    connection.close()
    return True

@app.route('/orders', methods=['POST'])
def create_order():
    """Endpoint para recibir nuevos pedidos."""
    try:
        data = request.get_json()
        
        if not data or 'order_id' not in data or 'items' not in data:
            return jsonify({'error': 'Invalid order data'}), 400

        success = publish_order(data) #rabbitmq function
        
        if success:
            return jsonify({'message': 'Order received', 'order': data}), 201
        else:
            return jsonify({'error': 'Failed to process order'}), 500

    except Exception as e:
        logging.error(f"Error processing order: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(debug=True)
from flask import Flask, request, jsonify
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

def publish_order(order_data):
    logging.info(f"Order received: {order_data}")
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
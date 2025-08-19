from flask import Flask, request, jsonify
from flask_cors import CORS
import json
from confluent_kafka import Producer
import os

app = Flask(__name__)
CORS(app)
import os

conf = {
    'bootstrap.servers': os.environ["BOOTSTRAP_SERVERS"],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': os.environ["SASL_USERNAME"],
    'sasl.password': os.environ["SASL_PASSWORD"],
}

# Initialize producer
producer = Producer(conf)
TOPIC_NAME = "clickStreamTopic1"

@app.route('/click-event', methods=['post'])
def click_event():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    # Process the click event data
    print("Received click event:", json.dumps(data, indent=2))
    event_str = json.dumps(data)
    producer.produce(TOPIC_NAME, key=data.get("user_id"), value=event_str)
    producer.flush()

    # Here you can add logic to handle the click event, e.g., store it in a database

    return jsonify({"status": "success", "message": "Click event processed"}), 200


from flask import Flask, request, jsonify
from flask_cors import CORS
import json
from confluent_kafka import Producer
import os
from google.cloud import pubsub_v1

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}}, supports_credentials=True)
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

@app.route('/event-for-pub-sub', methods=['post'])
def event-for-pub-sub():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400
    event_str = json.dumps(data)
    publisher = pubsub_v1.PublisherClient()
    publisher.publish(pubsub_topic, event_str)
    return jsonify({"status": "success", "message": "Click event processed"}), 200

if __name__ == '__main__':
    print("Server is running on http://localhost:8080")
    print("Listening for click events...")
    app.run(host='0.0.0.0', port=8080)


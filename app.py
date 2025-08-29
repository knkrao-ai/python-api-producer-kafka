from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import os
# from confluent_kafka import Producer
# from google.cloud import pubsub_v1

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": ["http://localhost:3000"]}}, supports_credentials=True)

producer = None  # Python uses None, not null


@app.route('/click-event', methods=['POST'])
def click_event():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    conf = {
        'bootstrap.servers': os.environ.get("BOOTSTRAP_SERVERS", ""),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.environ.get("SASL_USERNAME", ""),
        'sasl.password': os.environ.get("SASL_PASSWORD", ""),
    }

    global producer
    if producer is None:
        # producer = Producer(conf)
        pass

    TOPIC_NAME = "clickStreamTopic1"

    print("Received click event:", json.dumps(data, indent=2))
    event_str = json.dumps(data)
    # producer.produce(TOPIC_NAME, key=data.get("user_id"), value=event_str)
    # producer.flush()

    return jsonify({"status": "success", "message": "Click event processed"}), 200


@app.route('/event-for-pub-sub', methods=['POST'])
def event_for_pub_sub():  # <-- fixed name (underscores, no dashes)
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    event_str = json.dumps(data)
    # publisher = pubsub_v1.PublisherClient()
    # publisher.publish("projects/stoked-harbor-444616-v8/topics/click-stream-1", event_str.encode("utf-8"))

    return jsonify({"status": "success", "message": "Click event processed"}), 200


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))  # Cloud Run expects PORT env var
    print(f"Server is running on http://0.0.0.0:{port}")
    app.run(host='0.0.0.0', port=port)

import os
import json
import time
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        security_protocol=os.environ.get("KAFKA_SECURITY_PROTOCOL", "SSL"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

@app.route("/submit", methods=["POST"])
def submit():
    data = request.get_json(force=True)
    if not data or "image_key" not in data:
        return jsonify({"error": "Missing image_key"}), 400

    image_key = data["image_key"].strip()
    if not image_key:
        return jsonify({"error": "image_key cannot be empty"}), 400

    msg = {"image_key": image_key, "ts": time.time()}

    producer = get_producer()
    producer.send(KAFKA_TOPIC, msg)
    producer.flush()

    return jsonify({"queued": True, "image_key": image_key, "topic": KAFKA_TOPIC})

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "service": "submit-api"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
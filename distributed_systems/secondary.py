from flask import Flask, request, jsonify
import logging
import os
import time

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [SECONDARY] %(levelname)s - %(message)s')

# in-memory log on Secondary
messages = []

# simulate delay (seconds) to demonstrate blocking replication on Master
SECONDARY_DELAY = float(os.environ.get("SECONDARY_DELAY", "0"))

@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(messages), 200

@app.route("/replicate", methods=["POST"])
def replicate():
    """
    Endpoint for Master to POST replicated entries.
    Responds with 200 on success (ACK).
    """
    payload = request.get_json(silent=True)
    if payload is None or "message" not in payload:
        return jsonify({"error": "missing 'message' in JSON body"}), 400

    # simulate processing delay if configured
    if SECONDARY_DELAY > 0:
        logging.info("Simulating processing delay: %s seconds", SECONDARY_DELAY)
        time.sleep(SECONDARY_DELAY)

    messages.append(payload)
    logging.info("Appended replicated message: %s", payload)
    return jsonify({"status": "ack"}), 200

if __name__ == "__main__":
    host = "0.0.0.0"
    port = int(os.environ.get("PORT"))
    logging.info("Starting Secondary on %s:%s delay=%s", host, port, SECONDARY_DELAY)
    app.run(host=host, port=port)

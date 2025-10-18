from flask import Flask, request, jsonify
import logging
import os
import time
import heapq
import threading

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s [SECONDARY] %(levelname)s - %(message)s')

replication_lock = threading.Lock()

# in-memory log on Secondary
class UniqueMinHeap:
    def __init__(self):
        self.heap = []
        self.seen = set()

    def push(self, priority, item):
        if priority not in self.seen:
            self.seen.add(priority)
            heapq.heappush(self.heap, (priority, item))

    def __len__(self):
        return len(self.heap)

    def __iter__(self):
        # iterate from min to max without modifying the heap
        # heapq.nsmallest returns a sorted list
        for priority, item in heapq.nsmallest(len(self.heap), self.heap):
            yield item

messages = UniqueMinHeap()

# simulate delay (seconds) to demonstrate blocking replication on Master
SECONDARY_DELAY = float(os.environ.get("SECONDARY_DELAY", "0"))

@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(list(messages)), 200

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

    with replication_lock:
        messages.push(payload['id'], payload)

    logging.info("Appended replicated message: %s", payload)
    return jsonify({"status": "ack"}), 200

if __name__ == "__main__":
    host = "0.0.0.0"
    port = int(os.environ.get("PORT"))
    logging.info("Starting Secondary on %s:%s delay=%s", host, port, SECONDARY_DELAY)
    app.run(host=host, port=port, threaded=True)

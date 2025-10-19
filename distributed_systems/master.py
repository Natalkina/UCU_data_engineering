from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify
import logging
import os
import requests
import time
import threading
from itertools import count

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MASTER] %(levelname)s - %(message)s")

# list of secondary endpoints
secondaries_env = os.environ.get("SECONDARIES", "")
secondaries = [s.strip().rstrip("/") for s in secondaries_env.split(",") if s.strip()]

# in-memory replicated log
state = {
    "messages": [],
}

message_id_count = count()

REPLICATION_TIMEOUT = float(os.environ.get("REPLICATION_TIMEOUT", "10"))

# lock to enforce message ordering
append_lock = threading.Lock()

@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(state['messages']), 200

@app.route("/messages", methods=["POST"])
def append_message():
    payload = request.get_json(silent=True)
    if payload is None or "message" not in payload:
        return jsonify({"error": "missing 'message' in JSON body"}), 400

    msg = payload["message"]
    expected_write_concern = 1 if "write_concern" not in payload else int(payload["write_concern"])
    if expected_write_concern < 1 or expected_write_concern > len(secondaries) + 1:
        return jsonify({"error": "invalid write_concern"}), 400

    # Acquire lock to enforce strict ordering
    with append_lock:
        timestamp = time.time()
        message_id = next(message_id_count)

        entry = {"id": message_id, "message": msg, "timestamp": timestamp}
        state['messages'].append(entry)

        expected_write_concern -= 1
        logging.info("Appended message locally: %s", entry)

    futures = []
    executor = ThreadPoolExecutor(max_workers=len(secondaries))
    for secondaryUrl in secondaries:
        future = executor.submit(replicate, secondaryUrl, message_id)
        futures.append(future)

    if expected_write_concern == 0:
        return jsonify({"status": "ok", "entry": entry}), 201

    for fut in as_completed(futures):
        try:
            ok = fut.result()
            expected_write_concern -= 1

            if expected_write_concern == 0:
                return jsonify({"status": "ok", "entry": entry}), 201
        except:
            logging.info("Failed replication")

    return jsonify({"status": "error", "entry": entry}), 500

def replicate(secondaryUrl, message_id):
    message = state['messages'][message_id]
    logging.info("Replicating message %s...", message['message'])

    replicate_url = f"{secondaryUrl}/replicate"
    logging.info("Replicating to %s ...", replicate_url)

    try:
        resp = requests.post(replicate_url, json=message, timeout=REPLICATION_TIMEOUT)
        if resp.status_code == 200:
            logging.info("ACK from %s", secondaryUrl)
        else:
            logging.error("Non-OK response from %s: %s - %s", secondaryUrl, resp.status_code, resp.text)
            raise Exception("Non ok response")
    except requests.RequestException as e:
        logging.exception("Failed to replicate to %s: %s", secondaryUrl, e)
        raise e

    logging.info("Replication finished, returning success to client")


if __name__ == "__main__":
    # run HTTP server
    port = int(os.environ.get("PORT"))
    host = "0.0.0.0"
    logging.info("Starting Master on %s:%s with secondaries=%s", host, port, secondaries)
    # threaded=True is default, but the lock ensures order
    app.run(host=host, port=port, threaded=True)

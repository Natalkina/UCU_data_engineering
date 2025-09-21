from flask import Flask, request, jsonify
import logging
import os
import requests
import time
import threading

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [MASTER] %(levelname)s - %(message)s")

# list of secondary endpoints
secondaries_env = os.environ.get("SECONDARIES", "")
secondaries = [s.strip().rstrip("/") for s in secondaries_env.split(",") if s.strip()]

# in-memory replicated log
state = {
    "messages": [],
    "acks": {},
    "last_ack_message_id": {},
}

for secondaryUrl in secondaries:
    state["last_ack_message_id"][secondaryUrl] = -1

REPLICATION_TIMEOUT = float(os.environ.get("REPLICATION_TIMEOUT", "10"))

# lock to enforce message ordering
append_lock = threading.Lock()
replication_lock = threading.Lock()
send_to_secondary_lock = {}
for secondaryUrl in secondaries:
    send_to_secondary_lock[secondaryUrl] = threading.Lock()

@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(state['messages']), 200

@app.route("/messages", methods=["POST"])
def append_message():
    payload = request.get_json(silent=True)
    if payload is None or "message" not in payload:
        return jsonify({"error": "missing 'message' in JSON body"}), 400

    msg = payload["message"]
    write_concern = 1 if "write_concern" not in payload else int(payload["write_concern"])

    # Acquire lock to enforce strict ordering
    with append_lock:
        timestamp = time.time()
        entry = {"message": msg, "timestamp": timestamp}
        state['messages'].append(entry)
        message_id = len(state['messages']) - 1
        state['acks'][message_id] = 0
        logging.info("Appended message locally: %s", entry)

    if write_concern > 1:
        while True:
            if state['acks'][message_id] >= write_concern - 1:
                break
            else:
                time.sleep(0.005)

    return jsonify({"status": "ok", "entry": entry}), 201

def send_to_secondary(secondaryUrl, message, message_id):
    replicate_url = f"{secondaryUrl}/replicate"
    logging.info("Replicating to %s ...", replicate_url)
    try:
        resp = requests.post(replicate_url, json=message, timeout=REPLICATION_TIMEOUT)
        if resp.status_code == 200:
            logging.info("ACK from %s", secondaryUrl)
        else:
            logging.error("Non-OK response from %s: %s - %s", secondaryUrl, resp.status_code, resp.text)
            return jsonify({"error": f"replication failed to {secondaryUrl}", "status": resp.status_code}), 500
    except requests.RequestException as e:
        logging.exception("Failed to replicate to %s: %s", secondaryUrl, e)
        return jsonify({"error": f"replication to {secondaryUrl} failed", "details": str(e)}), 500

def replicate(secondaryUrl):
    logging.info("Starting replication thread...")

    while True:
        if len(state['messages']) - 1 <= state['last_ack_message_id'][secondaryUrl]:
            time.sleep(0.005)
            continue

        with send_to_secondary_lock[secondaryUrl]:
            message_id = state['last_ack_message_id'][secondaryUrl] + 1
            message = state['messages'][message_id]

            logging.info("Replicating message %s...", message['message'])

            send_to_secondary(secondaryUrl, message, message_id)

            state['last_ack_message_id'][secondaryUrl] += 1

            with replication_lock:
                state['acks'][message_id] += 1

            logging.info("Replication finished, returning success to client")



if __name__ == "__main__":
    # run replication thread
    for secondaryUrl in secondaries:
        replicate_thread = threading.Thread(target=replicate, daemon=True, args=(secondaryUrl,))
        replicate_thread.start()

    # run HTTP server
    port = int(os.environ.get("PORT"))
    host = "0.0.0.0"
    logging.info("Starting Master on %s:%s with secondaries=%s", host, port, secondaries)
    # threaded=True is default, but the lock ensures order
    app.run(host=host, port=port, threaded=True)

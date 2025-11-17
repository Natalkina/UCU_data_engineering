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

REPLICATION_TIMEOUT = float(os.environ.get("REPLICATION_TIMEOUT", "30"))

retry_queue = []            # list of (secondaryUrl, message_id)
retry_lock = threading.Lock()

RETRY_DELAY = 1.0           # seconds, can be exponential later
MAX_RETRY_INTERVAL = 60

# heartbeat state
secondary_health = {s: "UNKNOWN" for s in secondaries}   # Healthy, Suspected, Unhealthy
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 1
SUSPECT_THRESHOLD = 2    # number of failed heartbeats
UNHEALTHY_THRESHOLD = 5  # number of failed heartbeats

failure_counts = {s: 0 for s in secondaries}

# lock to enforce message ordering
append_lock = threading.Lock()

def majority_quorum():
    """Return True if majority of (master + secondaries) is Healthy."""
    total_nodes = 1 + len(secondaries)
    healthy_nodes = 1 + sum(1 for s in secondaries if secondary_health[s] == "Healthy")
    return healthy_nodes >= (total_nodes // 2) + 1


@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(state['messages']), 200

@app.route("/messages", methods=["POST"])
def append_message():
    payload = request.get_json(silent=True)

    #read-only mode if quorum is lost
    if not majority_quorum():
        return jsonify({
            "error": "No quorum. Master is in read-only mode.",
            "quorum": False,
            "secondaries": secondary_health
        }), 503
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

        # expected_write_concern -= 1
        logging.info("Appended message locally: %s", entry)

    futures = []
    executor = ThreadPoolExecutor(max_workers=len(secondaries))
    for secondaryUrl in secondaries:
        future = executor.submit(replicate_with_retry, secondaryUrl, message_id)
        futures.append(future)

    if expected_write_concern == 1:
        return jsonify({"status": "ok", "entry": entry}), 201

    # deadline to limit waiting for ACKs
    deadline = time.time() + REPLICATION_TIMEOUT
    remaining_ack = expected_write_concern - 1
    for fut in as_completed(futures):
        try:
            ok = fut.result(timeout=deadline - time.time())
            remaining_ack -= 1

            if remaining_ack == 0:
                return jsonify({"status": "ok", "entry": entry}), 201
        except:
            logging.info("Failed replication")

    return jsonify({"status": "error", "entry": entry}), 500

@app.route("/health", methods=["GET"])
def master_health():
    return jsonify({
        "status": "ok",
        "secondaries": secondary_health,
        "quorum": majority_quorum()
    }), 200


def replicate_with_retry(secondaryUrl, message_id, max_attempts=10):
    attempt = 0
    while True:
        # skip if secondary is unhealthy
        if secondary_health.get(secondaryUrl) == "Unhealthy":
            logging.info("Skipping replication to %s because it is Unhealthy", secondaryUrl)
            return False
        try:
            replicate(secondaryUrl, message_id)
            return True
        except:
            attempt += 1
            delay = min(RETRY_DELAY * 2 ** (attempt - 1), MAX_RETRY_INTERVAL)
            logging.info("Retrying replication to %s after %.1f seconds (attempt %d)", secondaryUrl, delay, attempt)
            time.sleep(delay)
            if attempt >= max_attempts:
                with retry_lock:
                    retry_queue.append((secondaryUrl, message_id))
                return False


def replicate(secondaryUrl, message_id):
    message = state['messages'][message_id]
    logging.info("Replicating message %s...", message['message'])

    replicate_url = f"{secondaryUrl}/replicate"
    logging.info("Replicating to %s ...", replicate_url)

    for mid in range(message_id + 1):
        msg = state['messages'][mid]
        try:
            resp = requests.post(replicate_url, json=msg, timeout=REPLICATION_TIMEOUT)
            if resp.status_code != 200:
                raise Exception(f"Non-OK response {resp.status_code}")
        except requests.RequestException as e:
            logging.exception("Failed to replicate to %s: %s", secondaryUrl, e)
            raise e

    logging.info("Replication finished, returning success to client")

def retry_worker():
    while True:
        time.sleep(RETRY_DELAY)
        with retry_lock:
            if not retry_queue:
                continue
            items = list(retry_queue)
            retry_queue.clear()

        for secondaryUrl, message_id in items:
            try:
                replicate(secondaryUrl, message_id)   # try again
            except:
                # push back for next retry iteration
                with retry_lock:
                    retry_queue.append((secondaryUrl, message_id))

# start retry worker thread
threading.Thread(target=retry_worker, daemon=True).start()


def heartbeat_worker():
    while True:
        for sec in secondaries:
            try:
                r = requests.get(f"{sec}/health", timeout=HEARTBEAT_TIMEOUT)
                if r.status_code == 200:
                    secondary_health[sec] = "Healthy"
                    failure_counts[sec] = 0
                    continue
            except:
                pass

            # failure path
            failure_counts[sec] += 1
            if failure_counts[sec] < SUSPECT_THRESHOLD:
                secondary_health[sec] = "Suspected"
            elif failure_counts[sec] < UNHEALTHY_THRESHOLD:
                secondary_health[sec] = "Unhealthy"
            else:
                secondary_health[sec] = "Unhealthy"

        time.sleep(HEARTBEAT_INTERVAL)

# Start heartbeat thread
threading.Thread(target=heartbeat_worker, daemon=True).start()


if __name__ == "__main__":
    # run HTTP server
    port = int(os.environ.get("PORT"))
    host = "0.0.0.0"
    logging.info("Starting Master on %s:%s with secondaries=%s", host, port, secondaries)
    # threaded=True is default, but the lock ensures order
    app.run(host=host, port=port, threaded=True)

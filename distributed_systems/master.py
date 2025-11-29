# Updated master.py
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, jsonify
import logging
import os
import requests
import time
import threading
from itertools import count
from collections import deque

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

# retry queue per-secondary to avoid global scans
RETRY_DELAY = 1.0 # seconds, can be exponential later
MAX_RETRY_INTERVAL = 60

# heartbeat state
secondary_health = {secondary_url: "UNKNOWN" for secondary_url in secondaries} # Healthy, Suspected, Unhealthy
HEARTBEAT_INTERVAL = 2
HEARTBEAT_TIMEOUT = 1
SUSPECTED_THRESHOLD = 2
UNHEALTHY_THRESHOLD = 5  # number of failed heartbeats

failure_counts = {secondary_url: 0 for secondary_url in secondaries}

# event per-secondary to notify healthy transition
secondary_events = {secondary_url: threading.Event() for secondary_url in secondaries}

# lock to enforce message ordering for local append
append_lock = threading.Lock()

def has_majority_quorum():
    """Return True if majority of (master + secondaries) is Healthy."""
    total_nodes = 1 + len(secondaries)
    healthy_nodes = 1 + sum(1 for s in secondaries if secondary_health.get(s) == "Healthy")
    return healthy_nodes >= (total_nodes // 2) + 1


@app.route("/messages", methods=["GET"])
def get_messages():
    return jsonify(state['messages']), 200


@app.route("/messages", methods=["POST"])
def append_message():
    payload = request.get_json(silent=True)

    # read-only mode if quorum is lost
    if not has_majority_quorum():
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

    # Acquire lock to enforce strict ordering of local append
    with append_lock:
        timestamp = time.time()
        message_id = next(message_id_count)

        entry = {"id": message_id, "message": msg, "timestamp": timestamp}
        state['messages'].append(entry)
        logging.info("Appended message locally: %s", entry)

    # Build per-secondary tasks
    executor = ThreadPoolExecutor(max_workers=max(1, len(secondaries)))
    futures = {executor.submit(replicate_with_retry, s, message_id): s for s in secondaries}

    # if write_concern == 1: local write only, immediate success
    if expected_write_concern == 1:
        return jsonify({"status": "ok", "entry": entry}), 201

    # we need (expected_write_concern - 1) ACKs from secondaries
    required_acks = expected_write_concern - 1

    success_acks = 0
    try:
        for fut in as_completed(futures, 60):
            success_acks += 1
            if success_acks >= required_acks:
                return jsonify({"status": "ok", "entry": entry}), 201
    except TimeoutError:
        return jsonify({"status": "timeout", "entry": entry}), 500
    finally:
        executor.shutdown(wait=False)

    return jsonify({"status": "error", "entry": entry}), 500


@app.route("/health", methods=["GET"])
def master_health():
    return jsonify({
        "status": "ok",
        "secondaries": secondary_health,
        "quorum": has_majority_quorum()
    }), 200


def replicate_with_retry(secondaryUrl, message_id):
    logging.info(
        "Replicating message %s \"%s\" to %s started",
        message_id,
        state['messages'][message_id],
        secondaryUrl
    )

    attempt = 0
    backoff = RETRY_DELAY
    while True:
        # If node marked Unhealthy -> wait for event signalled by heartbeat
        if secondary_health.get(secondaryUrl) == "Unhealthy":
            logging.info("Secondary %s is Unhealthy — waiting for it to become Healthy before trying replication (msg=%d)", secondaryUrl, message_id)
            # block until secondary becomes Healthy (no timeout by requirement)
            secondary_events[secondaryUrl].wait()  # blocks until set
            logging.info("Secondary %s became Healthy — resuming replication attempts (msg=%d)", secondaryUrl, message_id)

        try:
            replicate(secondaryUrl, message_id)
            break
        except Exception:
            attempt += 1
            # exponential backoff but bounded
            delay = min(backoff * (2 ** (attempt - 1)), MAX_RETRY_INTERVAL)
            logging.info("Replication to %s failed for message %d: attempt %d; retrying after %.1f sec", secondaryUrl, message_id, attempt, delay)
            time.sleep(delay)


def replicate(secondaryUrl, message_id):
    """
    Send (0..message_id) messages to the secondary. Uses REPLICATION_TIMEOUT for HTTP calls.
    """
    message = state['messages'][message_id]
    logging.info(
        "Replicating message %s \"%s\" to %s...",
        message_id,
        message['message'],
        secondaryUrl
    )

    replicate_url = f"{secondaryUrl}/replicate"

    # send all messages up to message_id to ensure total order on secondary
    for mid in range(message_id + 1):
        msg = state['messages'][mid]
        try:
            resp = requests.post(replicate_url, json=msg, timeout=REPLICATION_TIMEOUT)
            if resp.status_code != 200:
                raise Exception(f"Non-OK response {resp.status_code}")
        except requests.RequestException as e:
            logging.exception("Failed to replicate to %s: %s", secondaryUrl, e)
            raise e

    logging.info("Replication to %s finished for message %d", secondaryUrl, message_id)


# heartbeat worker: pings secondaries and updates status + events

def heartbeat_worker():
    while True:
        for sec in secondaries:
            prev = secondary_health.get(sec)
            try:
                r = requests.get(f"{sec}/health", timeout=HEARTBEAT_TIMEOUT)
                if r.status_code == 200:
                    secondary_health[sec] = "Healthy"
                    failure_counts[sec] = 0
                    # signal event so any waiting replicate_with_retry resumes
                    secondary_events[sec].set()
                    continue
            except Exception:
                pass

            # failure path
            failure_counts[sec] += 1
            if SUSPECTED_THRESHOLD <= failure_counts[sec] < UNHEALTHY_THRESHOLD:
                secondary_health[sec] = "Suspected"
                secondary_events[sec].clear()
            elif failure_counts[sec] >= UNHEALTHY_THRESHOLD:
                secondary_health[sec] = "Unhealthy"
                secondary_events[sec].clear()

        time.sleep(HEARTBEAT_INTERVAL)

# Start heartbeat thread
threading.Thread(target=heartbeat_worker, daemon=True).start()


if __name__ == "__main__":
    # run HTTP server
    port = int(os.environ.get("PORT"))
    host = "0.0.0.0"
    logging.info("Starting Master on %s:%s with secondaries=%s", host, port, secondaries)
    app.run(host=host, port=port, threaded=True)

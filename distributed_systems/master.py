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
retry_queue = {s: deque() for s in secondaries}            # secondaryUrl -> deque of message_id
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

# event per-secondary to notify healthy transition
secondary_events = {s: threading.Event() for s in secondaries}

# lock to enforce message ordering for local append
append_lock = threading.Lock()


def majority_quorum():
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

    # Acquire lock to enforce strict ordering of local append
    with append_lock:
        timestamp = time.time()
        message_id = next(message_id_count)

        entry = {"id": message_id, "message": msg, "timestamp": timestamp}
        state['messages'].append(entry)
        logging.info("Appended message locally: %s", entry)

    # if write_concern == 1: local write only, immediate success
    if expected_write_concern == 1:
        return jsonify({"status": "ok", "entry": entry}), 201

    # Build per-secondary tasks
    executor = ThreadPoolExecutor(max_workers=max(1, len(secondaries)))
    futures = {executor.submit(replicate_with_retry, s, message_id): s for s in secondaries}

    # we need (expected_write_concern - 1) ACKs from secondaries
    required_acks = expected_write_concern - 1

    # Wait for required number of successful replications. We DO NOT return success on first quick False.
    success_acks = 0
    # No global deadline here for write_concern > 1: we must wait until required secondaries ack (per requirements)
    try:
        for fut in as_completed(futures):
            sec = futures[fut]
            try:
                ok = fut.result()
            except Exception:
                ok = False
            if ok:
                success_acks += 1
                logging.info("Received ACK from %s for message %d (total acks=%d)", sec, message_id, success_acks)
            else:
                logging.info("Replication task for %s finished with failure; it will be retried asynchronously", sec)

            if success_acks >= required_acks:
                return jsonify({"status": "ok", "entry": entry}), 201

        # If we exhausted futures but didn't get enough acks, keep waiting: some futures may still retry in background
        # We'll block here waiting for remaining futures (they were submitted; replicate_with_retry tries until success or enqueue)
        # To avoid busy-loop we poll futures that are still running
        while success_acks < required_acks:
            # check finished futures
            time.sleep(0.5)
            for fut, sec in list(futures.items()):
                if fut.done():
                    try:
                        ok = fut.result()
                    except Exception:
                        ok = False
                    if ok:
                        success_acks += 1
                        logging.info("Late ACK from %s for message %d (total acks=%d)", sec, message_id, success_acks)
                        # remove from dict to reduce checks
                        del futures[fut]
                    else:
                        # still failed; it may have enqueued for retry; remove future
                        del futures[fut]
            # if no futures left but still not enough acks, wait until node(s) become healthy and retry queue triggers
            if not futures and success_acks < required_acks:
                logging.info("Waiting for additional ACKs - required=%d got=%d; waiting for secondaries to become healthy...",
                             required_acks, success_acks)
                # Wait for any secondary to become healthy
                # simple sleep; heartbeat will trigger retries when nodes become healthy
                time.sleep(1)

    finally:
        executor.shutdown(wait=False)

    return jsonify({"status": "error", "entry": entry}), 500


@app.route("/health", methods=["GET"])
def master_health():
    return jsonify({
        "status": "ok",
        "secondaries": secondary_health,
        "quorum": majority_quorum()
    }), 200


def replicate_with_retry(secondaryUrl, message_id, max_attempts=10):
    """
    Tries to replicate message_id to secondaryUrl.
    - If secondary is Unhealthy, waits (blocks) until it becomes Healthy (per requirements: client should wait).
    - Retries with exponential backoff on transient failures.
    - If too many attempts, enqueues message to per-secondary retry_queue and returns False (so caller can continue waiting for other acks).
    """
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
            return True
        except Exception:
            attempt += 1
            # exponential backoff but bounded
            delay = min(backoff * (2 ** (attempt - 1)), MAX_RETRY_INTERVAL)
            logging.info("Replication to %s failed for message %d: attempt %d; retrying after %.1f sec", secondaryUrl, message_id, attempt, delay)
            time.sleep(delay)
            if attempt >= max_attempts:
                # enqueue for background retry when node becomes Healthy
                with retry_lock:
                    if message_id not in retry_queue.get(secondaryUrl, deque()):
                        retry_queue.setdefault(secondaryUrl, deque()).append(message_id)
                logging.info("Queued message %d for background retry to %s", message_id, secondaryUrl)
                return False


def replicate(secondaryUrl, message_id):
    """
    Send (0..message_id) messages to the secondary. Uses REPLICATION_TIMEOUT for HTTP calls.
    """
    message = state['messages'][message_id]
    logging.info("Replicating message %s to %s...", message['message'], secondaryUrl)

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


def process_retry_queue_for(secondaryUrl):
    """Attempt to resend queued messages for a specific secondary sequentially until queue empty or failure.
    Called when a secondary becomes Healthy.
    """
    logging.info("Starting retry processing for %s", secondaryUrl)
    while True:
        with retry_lock:
            q = retry_queue.get(secondaryUrl)
            if not q:
                break
            message_id = q[0]
        try:
            replicate(secondaryUrl, message_id)
            # success -> pop
            with retry_lock:
                popped = retry_queue[secondaryUrl].popleft()
                logging.info("Retry succeeded for %s message %d", secondaryUrl, popped)
        except Exception:
            logging.info("Retry to %s for message %d failed; will retry later", secondaryUrl, message_id)
            # backoff before trying next time
            time.sleep(RETRY_DELAY)
            break

    logging.info("Finished retry processing for %s", secondaryUrl)


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
                    # if transitioned from non-Healthy to Healthy, start retry worker for this node
                    if prev != "Healthy":
                        threading.Thread(target=process_retry_queue_for, args=(sec,), daemon=True).start()
                    continue
            except Exception:
                pass

            # failure path
            failure_counts[sec] += 1
            if failure_counts[sec] < SUSPECT_THRESHOLD:
                secondary_health[sec] = "Suspected"
                secondary_events[sec].clear()
            elif failure_counts[sec] < UNHEALTHY_THRESHOLD:
                secondary_health[sec] = "Unhealthy"
                secondary_events[sec].clear()
            else:
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

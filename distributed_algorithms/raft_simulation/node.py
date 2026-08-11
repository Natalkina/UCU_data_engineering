import logging
import os
import sys

import requests
from flask import Flask, jsonify, request

from raft_node import RaftNode, RPC_TIMEOUT

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("node")

NODE_ID = os.environ.get("NODE_ID", "node1")
PORT = int(os.environ.get("PORT", "5000"))

PEER_ADDRESSES = dict(
    pair.split("=", 1) for pair in os.environ.get("PEERS", "").split(",") if pair.strip()
)
PEER_ADDRESSES = {k.strip(): v.strip().rstrip("/") for k, v in PEER_ADDRESSES.items()}
PEER_IDS = list(PEER_ADDRESSES.keys())


def _rpc(peer_id: str, path: str, payload: dict):
    url = PEER_ADDRESSES.get(peer_id)
    if not url:
        return None
    try:
        resp = requests.post(f"{url}{path}", json=payload, timeout=RPC_TIMEOUT)
        return resp.json() if resp.status_code == 200 else None
    except requests.RequestException:
        return None


def send_request_vote(peer_id: str, args: dict):
    return _rpc(peer_id, "/raft/request_vote", args)


def send_append_entries(peer_id: str, args: dict):
    return _rpc(peer_id, "/raft/append_entries", args)


app = Flask(__name__)
raft = RaftNode(node_id=NODE_ID, peers=PEER_IDS, transport=sys.modules[__name__])


# Client API

_COMMAND_STATUS_CODES = {
    "committed": 201,
    "not_leader": 409,
    "timeout": 503,
}

@app.route("/command", methods=["POST"])
def post_command():
    payload = request.get_json(silent=True)
    if payload is None or "command" not in payload:
        return jsonify({"error": "missing 'command' in JSON body"}), 400

    result = raft.submit_command(payload["command"])
    status = result["status"]
    body = dict(result)
    body["committed"] = status == "committed"
    if status == "timeout":
        body["warning"] = (
            "no commit confirmation; the command may or may not have been "
            "committed - retry against the current leader"
        )
    return jsonify(body), _COMMAND_STATUS_CODES.get(status, 500)


@app.route("/log", methods=["GET"])
def get_log():
    return jsonify(raft.get_log_snapshot()), 200


@app.route("/status", methods=["GET"])
def get_status():
    return jsonify(raft.get_status()), 200


# Internal Raft RPC endpoints

@app.route("/raft/request_vote", methods=["POST"])
def raft_request_vote():
    args = request.get_json(force=True)
    return jsonify(raft.handle_request_vote(args)), 200


@app.route("/raft/append_entries", methods=["POST"])
def raft_append_entries():
    args = request.get_json(force=True)
    return jsonify(raft.handle_append_entries(args)), 200


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "alive", "node_id": NODE_ID}), 200


if __name__ == "__main__":
    raft.start()
    logger.info("Starting Raft node %s on port %d with peers=%s", NODE_ID, PORT, PEER_IDS)
    app.run(host="0.0.0.0", port=PORT, threaded=True)

# Distributed Systems — Raft Consensus

An implementation of the Raft consensus algorithm

## Architecture

- **raft_node.py** — transport-agnostic core of the Raft algorithm:
  - Node states: `Follower`, `Candidate`, `Leader`
  - Terms, randomized election timeouts, `RequestVote` RPC
  - Log replication and heartbeats via `AppendEntries` RPC
  - Commit index advancement (majority replication rule, current-term-only
    commit safety from §5.4.2)
  - All state is kept **in-memory only** — no persistence, per the task spec

- **node.py** — Flask HTTP wrapper around a `RaftNode`:
  - Client API:
    - `POST /command` — append a command to the replicated log. Only
      accepted if this node is currently the Leader.
    - `GET /log` — committed log, full log, and commit index.
    - `GET /status` — state, term, leader, commit index, log length.
  - Internal Raft RPCs (peer-to-peer, not meant for clients):
    - `POST /raft/request_vote`
    - `POST /raft/append_entries`
  - `GET /health` — liveness probe.


## Running the cluster

```bash
cd distributed_algorithms/raft_simulation
docker compose build

# Start the first 2 nodes
docker compose up -d node1 node2

# Start the 3rd node whenever you want it to join
docker compose up -d node3
```

Nodes are reachable on the host at:

| Node  | URL                    |
|-------|------------------------|
| node1 | http://localhost:5001  |
| node2 | http://localhost:5002  |
| node3 | http://localhost:5003  |

## API examples

```bash
# Append a command (must target the current leader)
curl -X POST http://localhost:5001/command -H "Content-Type: application/json" \
     -d '{"command": "msg1"}'

# Read the log
curl http://localhost:5001/log

# Read node status
curl http://localhost:5001/status
```

Partition a node by taking it off the Docker network:

```bash
docker network disconnect raft_simulation_default node1   # partition
docker network connect    raft_simulation_default node1   # heal
```

A disconnected container is unreachable from the host too, so to talk to an
isolated node use its own loopback:

```bash
docker exec node1 python -c "import requests; \
  print(requests.get('http://127.0.0.1:5000/status').text)"
```
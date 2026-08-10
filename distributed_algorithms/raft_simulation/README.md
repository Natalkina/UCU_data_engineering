#  Raft Consensus

An implementation of the Raft consensus algorithm.

## Architecture

- **raft_node.py** - core of the Raft algorithm:
  - Node states: `Follower`, `Candidate`, `Leader`
  - Terms, randomized election timeouts, `RequestVote` RPC
  - Log replication and heartbeats via `AppendEntries` RPC
  - Commit index advancement (majority replication rule, current-term-only
    commit safety from the paper)
  - Network partition simulation (`set_partitioned`) used testing

- **node.py** — Flask HTTP wrapper around a `RaftNode`:
  - Client API:
    - `POST /command` — append a command to the replicated log. Only
      accepted if this node is currently the Leader; otherwise responds
      `409 {"error": "not_leader", "leader_id": "..."}`.
    - `GET /log` — returns the committed log, the full (uncommitted-included)
      log, and the current commit index.
    - `GET /status` — returns node status: state, term, leader, commit
      index, partitioned flag, etc.
  - Internal Raft RPCs (peer-to-peer, not meant for clients):
    - `POST /raft/request_vote`
    - `POST /raft/append_entries`
  - Self-test helper:
    - `POST /partition {"partitioned": true|false}` — simulates this node
      being cut off from (or rejoining) the rest of the cluster. While
      partitioned, the node ignores/rejects all incoming and outgoing RPCs.
  - `GET /health` — liveness probe.


## Running the cluster

```bash
cd distributed_systems/raft_simulation
docker compose build

# Start the first 2 nodes
docker compose up -d node1 node2

# Start the 3rd node whenever you want it to join
docker compose up -d node3
```

## API examples

```bash
# Check status of a node
curl http://localhost:5001/status

# Append a command (must target the current leader)
curl -X POST http://localhost:5001/command -H "Content-Type: application/json" \
     -d '{"command": "msg1"}'

# Read the log
curl http://localhost:5001/log

# Simulate partitioning node1 away from the cluster
curl -X POST http://localhost:5001/partition -H "Content-Type: application/json" \
     -d '{"partitioned": true}'

# Heal the partition
curl -X POST http://localhost:5001/partition -H "Content-Type: application/json" \
     -d '{"partitioned": false}'
```

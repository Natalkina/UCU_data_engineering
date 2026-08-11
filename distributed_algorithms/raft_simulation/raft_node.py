import logging
import random
import threading
import time
from enum import Enum
from typing import Dict, List, Optional

logger = logging.getLogger("raft")


class NodeState(Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
HEARTBEAT_INTERVAL = 0.5
RPC_TIMEOUT = 1.0
COMMIT_WAIT_TIMEOUT = 5.0


class RaftNode:

    def __init__(self, node_id: str, peers: List[str], transport):
        self.node_id = node_id
        self.peers = list(peers)
        self.transport = transport

        self._lock = threading.RLock()

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.log: List[dict] = []

        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        self.leader_id: Optional[str] = None

        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        self._election_deadline = 0.0
        self._reset_election_timer_locked()

        self._stop = False


    def start(self):
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        logger.info("Raft node %s started with peers=%s", self.node_id, self.peers)

    def stop(self):
        self._stop = True

    # Helpers

    def _reset_election_timer_locked(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_deadline = time.monotonic() + timeout

    def last_log_index(self) -> int:
        return len(self.log)

    def last_log_term(self) -> int:
        return self.log[-1]["term"] if self.log else 0

    def get_entry(self, index: int) -> Optional[dict]:
        if index <= 0 or index > len(self.log):
            return None
        return self.log[index - 1]

    # Public status snapshot

    def get_status(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "state": self.state.value,
                "term": self.current_term,
                "leader_id": self.leader_id,
                "voted_for": self.voted_for,
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "log_length": len(self.log),
                "peers": self.peers,
            }

    def get_log_snapshot(self) -> dict:
        with self._lock:
            return {
                "commit_index": self.commit_index,
                "committed_log": list(self.log[: self.commit_index]),
                "full_log": list(self.log),
            }

    # Election timer loop

    def _election_timer_loop(self):
        while not self._stop:
            time.sleep(0.05)
            with self._lock:
                if self.state == NodeState.LEADER:
                    continue
                if time.monotonic() >= self._election_deadline:
                    self._start_election_locked()

    def _start_election_locked(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        term = self.current_term
        last_log_index = self.last_log_index()
        last_log_term = self.last_log_term()
        self._reset_election_timer_locked()
        logger.info("Node %s starting election for term %d", self.node_id, term)

        votes_received = {self.node_id}
        peers = list(self.peers)

        def worker(peer):
            args = {
                "term": term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            }
            reply = self.transport.send_request_vote(peer, args)
            if reply is None:
                return
            with self._lock:
                if reply.get("term", 0) > self.current_term:
                    self._become_follower_locked(reply["term"])
                    return
                if self.state != NodeState.CANDIDATE or self.current_term != term:
                    return
                if reply.get("vote_granted"):
                    votes_received.add(peer)
                    if self._has_majority(len(votes_received)):
                        self._become_leader_locked()

        for peer in peers:
            threading.Thread(target=worker, args=(peer,), daemon=True).start()

    def _has_majority(self, count: int) -> bool:
        total_nodes = len(self.peers) + 1
        return count >= (total_nodes // 2) + 1

    def _become_follower_locked(self, term: int):
        self.state = NodeState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.leader_id = None
        self._reset_election_timer_locked()

    def _become_leader_locked(self):
        if self.state == NodeState.LEADER:
            return
        logger.info("Node %s became LEADER for term %d", self.node_id, self.current_term)
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        term = self.current_term
        for peer in self.peers:
            self.next_index[peer] = self.last_log_index() + 1
            self.match_index[peer] = 0
            threading.Thread(
                target=self._replication_loop, args=(peer, term), daemon=True
            ).start()

    # Leader replication / heartbeat

    def _replication_loop(self, peer: str, term: int):
        while not self._stop:
            with self._lock:
                if self.state != NodeState.LEADER or self.current_term != term:
                    return
                next_idx = self.next_index.get(peer, self.last_log_index() + 1)
                prev_log_index = next_idx - 1
                prev_entry = self.get_entry(prev_log_index)
                args = {
                    "term": term,
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_entry["term"] if prev_entry else 0,
                    "entries": list(self.log[prev_log_index:]),
                    "leader_commit": self.commit_index,
                }

            reply = self.transport.send_append_entries(peer, args)

            with self._lock:
                if self.state != NodeState.LEADER or self.current_term != term:
                    return
                if reply is not None:
                    if reply.get("term", 0) > self.current_term:
                        self._become_follower_locked(reply["term"])
                        return

                    if reply.get("success"):
                        match = prev_log_index + len(args["entries"])
                        self.match_index[peer] = max(self.match_index.get(peer, 0), match)
                        self.next_index[peer] = self.match_index[peer] + 1
                        self._advance_commit_index_locked()
                    else:
                        self.next_index[peer] = max(1, next_idx - 1)

            time.sleep(HEARTBEAT_INTERVAL)

    def _advance_commit_index_locked(self):
        for index in range(self.last_log_index(), self.commit_index, -1):
            entry = self.get_entry(index)
            if entry is None or entry["term"] != self.current_term:
                continue
            replicas = 1
            for peer in self.peers:
                if self.match_index.get(peer, 0) >= index:
                    replicas += 1
            if self._has_majority(replicas):
                self.commit_index = index
                self.last_applied = index
                logger.info("Node %s (leader) advanced commit_index to %d", self.node_id, index)
                break

    # Client-facing operation

    def submit_command(self, command) -> dict:
        with self._lock:
            if self.state != NodeState.LEADER:
                return {"status": "not_leader", "leader_id": self.leader_id}
            entry = {"term": self.current_term, "command": command}
            self.log.append(entry)
            index = self.last_log_index()
            term = entry["term"]
            logger.info("Leader %s appended command at index %d: %r", self.node_id, index, command)

        deadline = time.monotonic() + COMMIT_WAIT_TIMEOUT
        while time.monotonic() < deadline:
            with self._lock:
                if self.commit_index >= index:
                    stored = self.get_entry(index)
                    if stored is not None and stored["term"] == term:
                        return {"status": "committed", "index": index, "term": term}
                    break
                if self.state != NodeState.LEADER:
                    break
            time.sleep(0.05)

        return {"status": "timeout", "index": index, "term": term}


    # RPC handlers

    def handle_request_vote(self, args: dict) -> dict:
        with self._lock:
            term = args["term"]
            candidate_id = args["candidate_id"]
            last_log_index = args["last_log_index"]
            last_log_term = args["last_log_term"]

            if term < self.current_term:
                return {"term": self.current_term, "vote_granted": False}

            if term > self.current_term:
                self._become_follower_locked(term)

            grant = False
            log_ok = (last_log_term > self.last_log_term()) or (
                last_log_term == self.last_log_term() and last_log_index >= self.last_log_index()
            )
            if (self.voted_for in (None, candidate_id)) and log_ok:
                self.voted_for = candidate_id
                grant = True
                self._reset_election_timer_locked()
                logger.info("Node %s voted for %s in term %d", self.node_id, candidate_id, term)

            return {"term": self.current_term, "vote_granted": grant}

    def handle_append_entries(self, args: dict) -> dict:
        with self._lock:
            term = args["term"]
            leader_id = args["leader_id"]
            prev_log_index = args["prev_log_index"]
            prev_log_term = args["prev_log_term"]
            entries = args["entries"]
            leader_commit = args["leader_commit"]

            if term < self.current_term:
                return {"term": self.current_term, "success": False}

            if term > self.current_term:
                self._become_follower_locked(term)
            elif self.state == NodeState.CANDIDATE:
                self.state = NodeState.FOLLOWER

            self.leader_id = leader_id
            self._reset_election_timer_locked()

            if prev_log_index > 0:
                prev_entry = self.get_entry(prev_log_index)
                if prev_entry is None or prev_entry["term"] != prev_log_term:
                    return {"term": self.current_term, "success": False}

            for i, new_entry in enumerate(entries):
                log_index = prev_log_index + i + 1
                existing = self.get_entry(log_index)
                if existing is not None and existing["term"] != new_entry["term"]:
                    del self.log[log_index - 1 :]
                    existing = None
                if existing is None:
                    self.log.append({"term": new_entry["term"], "command": new_entry["command"]})

            last_new_index = prev_log_index + len(entries)
            new_commit = min(leader_commit, last_new_index)
            if new_commit > self.commit_index:
                self.commit_index = new_commit
                self.last_applied = new_commit

            return {"term": self.current_term, "success": True}

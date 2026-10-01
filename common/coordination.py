"""
common/coordination.py
Milestone 4: Distributed Algorithms & Coordination Engine
Implements:
1. Lamport Logical Clocks (Causal Event Ordering)
2. Bully Leader Election Algorithm (Consensus & Coordinator Failover)
"""
import threading
import time

class LamportClock:
    """
    Implements Leslie Lamport's Logical Clock algorithm (1978).
    Maintains a monotonically increasing integer counter to establish
    a strict partial/total causal ordering of distributed events.
    """
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Rule 1: Increment local clock before generating any local or send event."""
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_time: int) -> int:
        """
        Rule 2: On receiving a message with timestamp T_msg:
        Local Clock = max(Local Clock, T_msg) + 1
        """
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value

    @property
    def time(self) -> int:
        with self._lock:
            return self._value


class BullyElectionNode:
    """
    Implements the classic Bully Leader Election Algorithm for distributed nodes.
    Each node has a unique ID and a priority rank (Cloud=4, Core=3, Edge-B=2, Edge-A=1).
    Highest-ranking active node bullies lower-ranked nodes and becomes coordinator.
    """
    def __init__(self, node_id: str, rank: int, all_nodes: dict):
        self.node_id = node_id
        self.rank = rank
        self.all_nodes = all_nodes  # dict of node_id -> rank
        self.leader = None
        self.is_alive = True
        self.clock = LamportClock()
        self.messages_sent = 0
        self.election_in_progress = False

    def start_election(self) -> dict:
        """
        Triggered when leader failure is detected.
        Sends ELECTION messages to all nodes with a higher rank.
        """
        self.election_in_progress = True
        self.clock.tick()
        t_start = time.perf_counter()
        higher_nodes = [nid for nid, r in self.all_nodes.items() if r > self.rank]

        # Count election broadcast messages
        election_msgs = 0
        answered_by_higher = False

        for nid in higher_nodes:
            election_msgs += 1  # ELECTION message sent
            # Check if higher node is alive
            if self._mock_ping_higher_node(nid):
                answered_by_higher = True
                election_msgs += 1  # OK / ANSWER message received

        if not answered_by_higher:
            # No higher node is alive; this node declares itself COORDINATOR
            self.leader = self.node_id
            coordinator_msgs = len(self.all_nodes) - 1  # Broadcast COORDINATOR to all others
            t_end = time.perf_counter()
            self.election_in_progress = False
            return {
                "elected_leader": self.node_id,
                "winner_rank": self.rank,
                "messages_exchanged": election_msgs + coordinator_msgs,
                "sync_delay_ms": round((t_end - t_start) * 1000.0, 3),
                "lamport_time": self.clock.tick(),
                "status": "ELECTED_SELF"
            }
        else:
            # A higher node will take over the election
            t_end = time.perf_counter()
            self.election_in_progress = False
            return {
                "elected_leader": "HIGHER_NODE_PENDING",
                "winner_rank": None,
                "messages_exchanged": election_msgs,
                "sync_delay_ms": round((t_end - t_start) * 1000.0, 3),
                "lamport_time": self.clock.tick(),
                "status": "YIELDED_TO_HIGHER"
            }

    def _mock_ping_higher_node(self, target_id: str) -> bool:
        """Checks if a higher node is active."""
        # Simulated node state checker
        return False  # Overridden in test runner
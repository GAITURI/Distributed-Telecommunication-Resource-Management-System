#!/usr/bin/env python3
"""
TeleRM Global Resource Manager (design doc section 3, "Global Resource
Manager" box: Admission control, Placement policy, Allocation table,
Topology + link reservations, Site registry + failure detector,
Utilisation sampler).

Run standalone:
    python3 manager.py --config config.json --results-dir results/run1
"""
import argparse
import json
import os
import sys
import threading
import time

from common import protocol, util
from common.counters import Counters
from common.topology import Topology


class Manager:
    def __init__(self, config: dict, results_dir: str, policy: str = None):
        self.config = config
        self.results_dir = results_dir
        self.policy = policy or config.get("placement_policy", "edge_first")
        self.heartbeat_interval = config.get("heartbeat_interval_s", 1.0)
        self.failure_timeout_hb = config.get("failure_timeout_heartbeats", 3)
        self.fairness_threshold = config.get("fairness_saturation_threshold", 0.9)

        self.topology = Topology(config["links"])
        self.counters = Counters()

        # --- protected state (single lock: admission intentionally holds
        # it for the whole ALLOCATE round trip -- design doc 8, "Serialised
        # admission. The manager holds a lock during ALLOCATE...") ---
        self.lock = threading.RLock()
        self.sites = {}            # site_id -> record (capacity, allocated, status, ...)
        self.allocation_table = {}  # sid -> instance record
        self._next_sid = 1

        # metrics
        self.offered = {"_all": 0}
        self.blocked = {"_all": 0}
        self.blocked_reasons = {"_all": {}}
        self.admitted = {"_all": 0}
        self.decision_times_ms = []
        self.completed_instances = []   # finished instance summaries
        self.heartbeat_log = []         # raw heartbeat records
        self.utilisation_samples = []

        self._stop = threading.Event()
        self._srv = None

    # ---------------------------------------------------------- lifecycle
    def start(self):
        host, port = self.config["manager"]["host"], self.config["manager"]["port"]
        self._srv = protocol.serve_forever(host, port, self._dispatch, counters=self.counters)
        threading.Thread(target=self._sampler_loop, daemon=True).start()
        threading.Thread(target=self._failure_detector_loop, daemon=True).start()
        print(f"[manager] listening on {host}:{port} (policy={self.policy})", flush=True)

    def stop(self):
        self._stop.set()
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass

    # ---------------------------------------------------------- dispatch
    def _dispatch(self, msg: dict) -> dict:
        t = msg.get("type")
        try:
            if t == "REGISTER":
                return self._on_register(msg)
            if t == "HEARTBEAT":
                return self._on_heartbeat(msg)
            if t == "REQUEST":
                return self._on_request(msg)
            if t == "RESIZE":
                return self._on_resize(msg)
            if t == "INSTANCE_EXIT":
                return self._on_instance_exit(msg)
            if t == "STATUS":
                return self._status()
            if t == "SHUTDOWN":
                threading.Thread(target=self._delayed_shutdown, daemon=True).start()
                return {"type": "SHUTDOWN_OK"}
        except Exception as e:  # pragma: no cover - defensive
            return {"type": "ERROR", "error": str(e)}
        return {"type": "ERROR", "error": f"unknown message type {t!r}"}

    def _delayed_shutdown(self):
        time.sleep(0.2)  # let the SHUTDOWN_OK reply flush first
        self._write_report()
        self.stop()

    # ---------------------------------------------------------- REGISTER
    def _on_register(self, msg):
        with self.lock:
            sid = msg["site_id"]
            self.sites[sid] = {
                "site_id": sid,
                "host": msg["host"],
                "port": msg["port"],
                "tier": msg["tier"],
                "capacity": dict(msg["capacity"]),
                "allocated": {"cpu": 0, "mem": 0, "bw": 0},
                "workers": msg.get("workers", 1),
                "status": "ACTIVE",
                "last_heartbeat": time.time(),
                "last_fairness": None,
                "last_slot_util": None,
            }
        print(f"[manager] REGISTER {sid} ({msg['tier']}) at {msg['host']}:{msg['port']}", flush=True)
        return {"type": "REGISTER_OK", "site_id": sid}

    # --------------------------------------------------------- HEARTBEAT
    def _on_heartbeat(self, msg):
        with self.lock:
            sid = msg["site_id"]
            site = self.sites.get(sid)
            if site is None:
                return {"type": "ERROR", "error": "unregistered site"}
            site["last_heartbeat"] = time.time()
            if site["status"] == "SUSPECT":
                print(f"[manager] {sid} back ACTIVE", flush=True)
            site["status"] = "ACTIVE"
            site["last_fairness"] = msg.get("fairness_j")
            site["last_slot_util"] = msg.get("slot_utilisation")
            self.heartbeat_log.append({
                "ts": msg.get("ts", time.time()),
                "site_id": sid,
                "slot_utilisation": msg.get("slot_utilisation"),
                "jobs_interval": msg.get("jobs_interval"),
                "fairness_j": msg.get("fairness_j"),
                "instances_running": msg.get("instances_running"),
            })
        return {"type": "HEARTBEAT_OK"}

    # ----------------------------------------------------------- REQUEST
    def _on_request(self, msg):
        t0 = time.perf_counter()
        service = msg["service"]
        ingress = msg["ingress"]
        demand = msg["demand"]
        max_latency = msg["max_latency_ms"]
        holding_s = msg["holding_s"]

        with self.lock:  # held for the whole admission round trip, by design
            self.offered["_all"] += 1
            self.offered[service] = self.offered.get(service, 0) + 1

            candidates = []
            fail_reasons = []
            for sid, site in self.sites.items():
                if site["status"] != "ACTIVE":
                    continue
                path, lat = self.topology.shortest_path(ingress, sid)
                if path is None:
                    fail_reasons.append("unreachable")
                    continue
                if lat > max_latency:
                    fail_reasons.append("latency")
                    continue
                if not util.fits(site["allocated"], demand, site["capacity"]):
                    fail_reasons.append("node_capacity")
                    continue
                bw_ok, _blocking_link = self.topology.path_has_bandwidth(path, demand["bw"])
                if not bw_ok:
                    fail_reasons.append("link_bandwidth")
                    continue
                allocated_after = {r: site["allocated"][r] + demand.get(r, 0) for r in site["capacity"]}
                candidates.append({
                    "site_id": sid,
                    "path": path,
                    "path_latency_ms": lat,
                    "dominant_share_after": util.dominant_share(allocated_after, site["capacity"]),
                    "dominant_free_share_after": util.dominant_free_share(allocated_after, site["capacity"]),
                })

            if not candidates:
                reason = self._pick_reason(fail_reasons)
                self.blocked["_all"] += 1
                self.blocked[service] = self.blocked.get(service, 0) + 1
                sr = self.blocked_reasons.setdefault(service, {})
                sr[reason] = sr.get(reason, 0) + 1
                self.blocked_reasons["_all"][reason] = self.blocked_reasons["_all"].get(reason, 0) + 1
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": msg.get("req_id"), "reason": reason, "decision_ms": round(dt, 3)}

            chosen = self._apply_policy(candidates, ingress)

            sid = chosen["site_id"]
            site = self.sites[sid]
            path = chosen["path"]

            # reserve node vector + path bandwidth *before* contacting the site
            for r in site["capacity"]:
                site["allocated"][r] += demand.get(r, 0)
            self.topology.reserve_path(path, demand["bw"])

            new_sid = util.format_sid(self._next_sid)
            self._next_sid += 1

            try:
                reply = protocol.request(
                    site["host"], site["port"],
                    {"type": "ALLOCATE", "sid": new_sid, "service": service,
                     "demand": demand, "holding_s": holding_s},
                    counters=self.counters,
                )
                ok = reply.get("type") == "ALLOCATE_OK"
            except (ConnectionError, OSError, TimeoutError):
                ok = False

            if not ok:
                # roll back the reservation -- design doc 5.1
                for r in site["capacity"]:
                    site["allocated"][r] -= demand.get(r, 0)
                self.topology.release_path(path, demand["bw"])
                self.blocked["_all"] += 1
                self.blocked[service] = self.blocked.get(service, 0) + 1
                sr = self.blocked_reasons.setdefault(service, {})
                sr["site_unreachable"] = sr.get("site_unreachable", 0) + 1
                self.blocked_reasons["_all"]["site_unreachable"] = self.blocked_reasons["_all"].get("site_unreachable", 0) + 1
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": msg.get("req_id"), "reason": "site_unreachable", "decision_ms": round(dt, 3)}

            self.allocation_table[new_sid] = {
                "sid": new_sid, "service": service, "site": sid, "ingress": ingress,
                "demand": dict(demand), "path": path, "path_latency_ms": chosen["path_latency_ms"],
                "holding_s": holding_s, "admitted_at": time.time(), "state": "RUNNING",
            }
            self.admitted["_all"] += 1
            self.admitted[service] = self.admitted.get(service, 0) + 1

            dt = (time.perf_counter() - t0) * 1000
            self.decision_times_ms.append(dt)
            return {
                "type": "ACCEPTED", "req_id": msg.get("req_id"), "sid": new_sid, "site": sid,
                "path": path, "path_latency_ms": chosen["path_latency_ms"], "decision_ms": round(dt, 3),
            }

    @staticmethod
    def _pick_reason(reasons):
        if not reasons:
            return "no_active_sites"
        priority = ["latency", "node_capacity", "link_bandwidth", "unreachable"]
        counts = {}
        for r in reasons:
            counts[r] = counts.get(r, 0) + 1
        best = max(counts.items(), key=lambda kv: (kv[1], -priority.index(kv[0]) if kv[0] in priority else -99))
        return best[0]

    def _apply_policy(self, candidates, ingress):
        if self.policy == "least_loaded":
            key = lambda c: c["dominant_share_after"]
        elif self.policy == "best_fit":
            key = lambda c: c["dominant_free_share_after"]
        else:  # edge_first (default): ingress falls out of "lowest latency"
               # first, since only the ingress site has a zero-latency path;
               # ties broken by least loaded.
            key = lambda c: (c["path_latency_ms"], c["dominant_share_after"])
        return min(candidates, key=key)

    # ------------------------------------------------------------ RESIZE
    def _on_resize(self, msg):
        with self.lock:
            sid = msg["sid"]
            inst = self.allocation_table.get(sid)
            if inst is None or inst["state"] != "RUNNING":
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"), "reason": "unknown_or_terminated"}
            factor = msg["factor"]
            old_cpu = inst["demand"]["cpu"]
            new_cpu = max(1, round(old_cpu * factor))
            extra = new_cpu - old_cpu
            site = self.sites[inst["site"]]
            if extra > 0 and site["allocated"]["cpu"] + extra > site["capacity"]["cpu"] + 1e-9:
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"), "reason": "node_capacity"}
            try:
                reply = protocol.request(
                    site["host"], site["port"],
                    {"type": "RESIZE", "sid": sid, "cpu": new_cpu}, counters=self.counters)
                ok = reply.get("type") == "RESIZE_OK"
            except (ConnectionError, OSError, TimeoutError):
                ok = False
            if not ok:
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"), "reason": "site_unreachable"}
            site["allocated"]["cpu"] += extra
            inst["demand"]["cpu"] = new_cpu
            return {"type": "RESIZE_OK", "req_id": msg.get("req_id"), "sid": sid, "cpu": new_cpu}

    # ------------------------------------------------------- INSTANCE_EXIT
    def _on_instance_exit(self, msg):
        with self.lock:
            sid = msg["sid"]
            inst = self.allocation_table.get(sid)
            if inst is None:
                return {"type": "INSTANCE_EXIT_OK"}
            site = self.sites.get(inst["site"])
            if site is not None:
                for r in site["capacity"]:
                    site["allocated"][r] = max(0, site["allocated"][r] - inst["demand"].get(r, 0))
            self.topology.release_path(inst["path"], inst["demand"]["bw"])
            inst["state"] = "TERMINATED"
            summary = dict(msg.get("summary", {}))
            summary.update({"sid": sid, "service": inst["service"], "site": inst["site"]})
            self.completed_instances.append(summary)
            del self.allocation_table[sid]
        return {"type": "INSTANCE_EXIT_OK"}

    # ------------------------------------------------------------ STATUS
    def _status(self):
        with self.lock:
            return {
                "type": "STATUS_OK",
                "policy": self.policy,
                "sites": {sid: {
                    "tier": s["tier"], "status": s["status"], "capacity": s["capacity"],
                    "allocated": s["allocated"], "last_fairness": s["last_fairness"],
                    "last_slot_util": s["last_slot_util"],
                } for sid, s in self.sites.items()},
                "running_instances": len(self.allocation_table),
                "offered": dict(self.offered),
                "blocked": dict(self.blocked),
                "admitted": dict(self.admitted),
                "link_utilisation": self.topology.link_utilisation(),
                "counters": self.counters.snapshot(),
            }

    # -------------------------------------------------------- background
    def _sampler_loop(self):
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            with self.lock:
                sample = {
                    "ts": time.time(),
                    "sites": {sid: {
                        "cpu_util": s["allocated"]["cpu"] / s["capacity"]["cpu"] if s["capacity"]["cpu"] else 0,
                        "mem_util": s["allocated"]["mem"] / s["capacity"]["mem"] if s["capacity"]["mem"] else 0,
                        "bw_util": s["allocated"]["bw"] / s["capacity"]["bw"] if s["capacity"]["bw"] else 0,
                    } for sid, s in self.sites.items()},
                    "links": self.topology.link_utilisation(),
                }
                self.utilisation_samples.append(sample)

    def _failure_detector_loop(self):
        timeout = self.heartbeat_interval * self.failure_timeout_hb
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            now = time.time()
            with self.lock:
                for sid, s in self.sites.items():
                    if s["status"] == "ACTIVE" and now - s["last_heartbeat"] > timeout:
                        s["status"] = "SUSPECT"
                        print(f"[manager] {sid} -> SUSPECT (no heartbeat for {now - s['last_heartbeat']:.1f}s)", flush=True)

    # ------------------------------------------------------------ report
    def _write_report(self):
        os.makedirs(self.results_dir, exist_ok=True)
        with self.lock:
            report = {
                "policy": self.policy,
                "offered": dict(self.offered),
                "blocked": dict(self.blocked),
                "admitted": dict(self.admitted),
                "blocking_probability": {
                    k: (self.blocked.get(k, 0) / v if v else 0.0) for k, v in self.offered.items()
                },
                "blocked_reasons": self.blocked_reasons,
                "decision_time_ms": {
                    "mean": sum(self.decision_times_ms) / len(self.decision_times_ms) if self.decision_times_ms else None,
                    "max": max(self.decision_times_ms) if self.decision_times_ms else None,
                    "count": len(self.decision_times_ms),
                },
                "link_utilisation_final": self.topology.link_utilisation(),
                "counters": self.counters.snapshot(),
                "sites_final": {sid: {"capacity": s["capacity"], "allocated": s["allocated"]} for sid, s in self.sites.items()},
            }
        with open(os.path.join(self.results_dir, "manager_report.json"), "w") as f:
            json.dump(report, f, indent=2)
        with open(os.path.join(self.results_dir, "instances_log.json"), "w") as f:
            json.dump(self.completed_instances, f, indent=2)
        with open(os.path.join(self.results_dir, "heartbeat_log.json"), "w") as f:
            json.dump(self.heartbeat_log, f, indent=2)
        with open(os.path.join(self.results_dir, "utilisation_timeseries.json"), "w") as f:
            json.dump(self.utilisation_samples, f, indent=2)
        print(f"[manager] report written to {self.results_dir}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="TeleRM Global Resource Manager")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--results-dir", default="results/latest")
    ap.add_argument("--policy", default=None, choices=["edge_first", "least_loaded", "best_fit"])
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    mgr = Manager(config, args.results_dir, policy=args.policy)
    mgr.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mgr._write_report()
        mgr.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()

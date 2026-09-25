#!/usr/bin/env python3
"""
TeleRM site node (design doc 3.1/4.4): registers with the manager, then
runs a fixed worker pool driven by stride scheduling. Every running
instance is backlogged (always has jobs waiting); the scheduler hands
each free worker to the eligible instance with the smallest pass value,
then advances that instance's pass by its stride = 10^6 / tickets.

Workers are implemented as threads within this one OS process (each
site *is* its own OS process -- that's what satisfies "every node is a
separate OS process with its own TCP port" in the design doc). We
simulate a "job" as a short unit of work of configurable duration
rather than real traffic processing, since S1 has no real payload.

Run standalone:
    python3 site.py --site-id edge-A --config config.json
"""
import argparse
import json
import math
import random
import threading
import time

from common import protocol, util
from common.counters import Counters


class Instance:
    def __init__(self, sid, service, demand, holding_s):
        self.sid = sid
        self.service = service
        self.demand = dict(demand)
        self.tickets = demand["cpu"]
        self.stride = 1_000_000 / self.tickets
        self.pass_value = 0.0
        self.max_concurrent = util.ceil_div(self.tickets, 1000)
        self.in_flight = 0
        self.created_at = time.time()
        self.expires_at = self.created_at + holding_s
        self.holding_s = holding_s
        self.jobs_done = 0
        self.busy_s = 0.0
        self.latency_sum_ms = 0.0
        # counters since the last heartbeat, for jobs_interval / fairness
        self.jobs_since_hb = 0


class Site:
    def __init__(self, site_id, config, manager_host, manager_port):
        self.site_id = site_id
        self.config = config
        self.manager_host = manager_host
        self.manager_port = manager_port
        scfg = config["sites"][site_id]
        self.host = scfg["host"]
        self.port = scfg["port"]
        self.tier = scfg["tier"]
        self.capacity = {"cpu": scfg["cpu"], "mem": scfg["mem"], "bw": scfg["bw"]}
        self.n_workers = scfg["workers"]
        self.job_service_ms = config.get("job_service_ms", 6)
        self.heartbeat_interval = config.get("heartbeat_interval_s", 1.0)
        self.fairness_threshold = config.get("fairness_saturation_threshold", 0.9)

        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.instances = {}   # sid -> Instance
        self.rng = random.Random(hash(site_id) & 0xFFFFFFFF)
        self.counters = Counters()

        self._stop = threading.Event()
        self._srv = None
        self.busy_workers = 0
        self.busy_time_since_hb = 0.0
        self._last_hb_ts = time.time()

    # ---------------------------------------------------------- lifecycle
    def start(self):
        self._srv = protocol.serve_forever(self.host, self.port, self._dispatch, counters=self.counters)
        self._register()
        for i in range(self.n_workers):
            threading.Thread(target=self._worker_loop, args=(i,), daemon=True, name=f"{self.site_id}-worker-{i}").start()
        threading.Thread(target=self._reaper_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        print(f"[{self.site_id}] listening on {self.host}:{self.port}, "
              f"{self.n_workers} workers, registered with manager", flush=True)

    def stop(self):
        self._stop.set()
        with self.cv:
            self.cv.notify_all()
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass

    def _register(self):
        reply = protocol.request(self.manager_host, self.manager_port, {
            "type": "REGISTER", "site_id": self.site_id, "host": self.host, "port": self.port,
            "tier": self.tier, "capacity": self.capacity, "workers": self.n_workers,
        }, counters=self.counters)
        if reply.get("type") != "REGISTER_OK":
            raise RuntimeError(f"registration failed: {reply}")

    # ---------------------------------------------------------- dispatch
    def _dispatch(self, msg: dict) -> dict:
        t = msg.get("type")
        if t == "ALLOCATE":
            return self._on_allocate(msg)
        if t == "RELEASE":
            return self._on_release(msg)
        if t == "RESIZE":
            return self._on_resize(msg)
        if t == "STATUS":
            return self._status()
        if t == "SHUTDOWN":
            threading.Thread(target=self._delayed_shutdown, daemon=True).start()
            return {"type": "SHUTDOWN_OK"}
        return {"type": "ERROR", "error": f"unknown message type {t!r}"}

    def _delayed_shutdown(self):
        time.sleep(0.2)
        self.stop()

    # ---------------------------------------------------------- ALLOCATE
    def _on_allocate(self, msg):
        sid = msg["sid"]
        inst = Instance(sid, msg["service"], msg["demand"], msg["holding_s"])
        with self.cv:
            if self.instances:
                min_pass = min(i.pass_value for i in self.instances.values())
                inst.pass_value = min_pass  # a new instance starts at the current minimum
            self.instances[sid] = inst
            self.cv.notify_all()
        return {"type": "ALLOCATE_OK", "sid": sid}

    # ----------------------------------------------------------- RELEASE
    def _on_release(self, msg):
        sid = msg["sid"]
        with self.cv:
            inst = self.instances.pop(sid, None)
            self.cv.notify_all()
        if inst is not None:
            self._report_exit(inst)
        return {"type": "RELEASE_OK", "sid": sid}

    # ------------------------------------------------------------ RESIZE
    def _on_resize(self, msg):
        sid = msg["sid"]
        with self.cv:
            inst = self.instances.get(sid)
            if inst is None:
                return {"type": "RESIZE_FAIL", "sid": sid, "error": "not_found"}
            inst.tickets = msg["cpu"]
            inst.demand["cpu"] = msg["cpu"]
            inst.stride = 1_000_000 / inst.tickets
            inst.max_concurrent = util.ceil_div(inst.tickets, 1000)
            # pass_value is left untouched on purpose: resizing changes the
            # *rate* an instance earns share at from now on, without
            # resetting the fairness bookkeeping it has already accrued.
        return {"type": "RESIZE_OK", "sid": sid, "cpu": msg["cpu"]}

    # ------------------------------------------------------------ STATUS
    def _status(self):
        with self.lock:
            return {
                "type": "STATUS_OK", "site_id": self.site_id,
                "instances": {sid: {
                    "service": i.service, "tickets": i.tickets, "jobs_done": i.jobs_done,
                    "busy_s": round(i.busy_s, 3), "expires_in_s": round(i.expires_at - time.time(), 1),
                } for sid, i in self.instances.items()},
                "counters": self.counters.snapshot(),
            }

    # ------------------------------------------------------ stride pick
    def _pick_next_locked(self):
        """Caller holds self.lock. Returns the eligible instance with the
        smallest pass value, or None."""
        best = None
        for inst in self.instances.values():
            if inst.in_flight >= inst.max_concurrent:
                continue
            if best is None or inst.pass_value < best.pass_value:
                best = inst
        return best

    # ------------------------------------------------------- worker loop
    def _worker_loop(self, worker_idx):
        while not self._stop.is_set():
            with self.cv:
                inst = self._pick_next_locked()
                while inst is None and not self._stop.is_set():
                    self.cv.wait(timeout=0.5)
                    inst = self._pick_next_locked()
                if self._stop.is_set():
                    return
                inst.in_flight += 1
                self.busy_workers += 1

            job_start = time.perf_counter()
            # simulate the job: exponential service time around job_service_ms
            duration = self.rng.expovariate(1.0 / (self.job_service_ms / 1000.0))
            duration = min(duration, self.job_service_ms * 5 / 1000.0)  # cap the long tail
            time.sleep(duration)
            elapsed_ms = (time.perf_counter() - job_start) * 1000

            with self.cv:
                inst.in_flight -= 1
                inst.jobs_done += 1
                inst.jobs_since_hb += 1
                inst.busy_s += duration
                inst.latency_sum_ms += elapsed_ms
                inst.pass_value += inst.stride
                self.busy_workers -= 1
                self.busy_time_since_hb += duration
                self.cv.notify_all()

    # -------------------------------------------------------- reaper
    def _reaper_loop(self):
        """Terminates instances whose holding time has elapsed and reports
        INSTANCE_EXIT to the manager (design doc 4.2 life cycle)."""
        while not self._stop.is_set():
            time.sleep(0.1)
            now = time.time()
            expired = []
            with self.cv:
                for sid, inst in list(self.instances.items()):
                    if now >= inst.expires_at:
                        expired.append(inst)
                        del self.instances[sid]
                if expired:
                    self.cv.notify_all()
            for inst in expired:
                self._report_exit(inst)

    def _report_exit(self, inst: Instance):
        try:
            protocol.request(self.manager_host, self.manager_port, {
                "type": "INSTANCE_EXIT", "sid": inst.sid,
                "summary": {
                    "jobs_done": inst.jobs_done,
                    "busy_s": round(inst.busy_s, 3),
                    "latency_sum_ms": round(inst.latency_sum_ms, 3),
                    "holding_s": inst.holding_s,
                },
            }, counters=self.counters)
        except (ConnectionError, OSError, TimeoutError) as e:
            print(f"[{self.site_id}] failed to report INSTANCE_EXIT for {inst.sid}: {e}", flush=True)

    # ------------------------------------------------------- heartbeat
    def _heartbeat_loop(self):
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            now = time.time()
            with self.cv:
                elapsed = now - self._last_hb_ts
                self._last_hb_ts = now
                slot_capacity_s = self.n_workers * elapsed
                slot_util = (self.busy_time_since_hb / slot_capacity_s) if slot_capacity_s > 0 else 0.0
                jobs_interval = sum(i.jobs_since_hb for i in self.instances.values())

                fairness_j = None
                if slot_util > self.fairness_threshold and self.instances:
                    xs = [i.jobs_since_hb / i.tickets for i in self.instances.values() if i.tickets > 0]
                    fairness_j = util.jains_fairness_index(xs)

                instances_running = len(self.instances)
                for i in self.instances.values():
                    i.jobs_since_hb = 0
                self.busy_time_since_hb = 0.0

            try:
                protocol.request(self.manager_host, self.manager_port, {
                    "type": "HEARTBEAT", "site_id": self.site_id, "ts": now,
                    "slot_utilisation": round(slot_util, 4),
                    "jobs_interval": jobs_interval,
                    "fairness_j": round(fairness_j, 4) if fairness_j is not None else None,
                    "instances_running": instances_running,
                }, counters=self.counters, timeout=2.0)
            except (ConnectionError, OSError, TimeoutError):
                pass  # manager may be briefly unreachable; next heartbeat will retry


def main():
    ap = argparse.ArgumentParser(description="TeleRM site node")
    ap.add_argument("--site-id", required=True)
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--manager-host", default=None)
    ap.add_argument("--manager-port", type=int, default=None)
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    mhost = args.manager_host or config["manager"]["host"]
    mport = args.manager_port or config["manager"]["port"]

    site = Site(args.site_id, config, mhost, mport)
    site.start()
    try:
        while not site._stop.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        site.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
TeleRM discrete-event simulator + big-dataset generator.

Runs the SAME admission control and placement code as the live manager
(common/placement.py, common/topology.py) but on a virtual clock, so
thousands of runs finish in minutes instead of 30 s each. Output is a
Power BI / Tableau-ready star schema of CSV files.

    python3 simulate.py                       # default sweep (~ a few hundred thousand rows)
    python3 simulate.py --seeds 100 --duration 600 --rates 1,2,4,6,8,12
    python3 simulate.py --quick               # tiny sweep for a smoke test

Tables written to --out (default: bigdata/):
    dim_run.csv          one row per run: parameters + result summary
    dim_service.csv      service catalogue
    dim_site.csv         site capacities
    fact_requests.csv    one row per offered request (outcome, site, reason, ...)
    fact_resizes.csv     one row per resize attempt
    fact_utilisation.csv per-second per-site cpu/mem/bw utilisation
    fact_link.csv        per-second per-link utilisation
    fact_run_service.csv per run x service offered/blocked counts
"""
import argparse
import collections
import copy
import csv
import heapq
import json
import multiprocessing as mp
import os
import random
import time

from common import placement, util
from common.topology import Topology

HERE = os.path.dirname(os.path.abspath(__file__))


def simulate_run(job):
    """Run one simulation. `job` is a dict; returns dict of row-lists."""
    cfg, run_id, policy, seed, rate, duration, mean_hold = (
        job["cfg"], job["run_id"], job["policy"], job["seed"],
        job["rate"], job["duration"], job["mean_hold"])

    wl = cfg["workload"]
    services = cfg["services"]
    names = list(services)
    shares = [services[n]["share"] for n in names]

    # common random numbers: the arrival stream depends only on (seed, rate),
    # so every policy sees IDENTICAL traffic -> fair, paired comparison.
    arr = random.Random(f"arr-{seed}-{rate}")
    rsz = random.Random(f"rsz-{seed}-{rate}")

    topo = Topology(cfg["links"])
    sites = {sid: {"tier": s["tier"], "status": "ACTIVE",
                   "capacity": {"cpu": s["cpu"], "mem": s["mem"], "bw": s["bw"]},
                   "allocated": {"cpu": 0, "mem": 0, "bw": 0}}
             for sid, s in cfg["sites"].items()}

    heap, seq = [], 0

    def push(t, kind, data):
        nonlocal seq
        seq += 1
        heapq.heappush(heap, (t, seq, kind, data))

    live = {}                                   # sid -> instance
    recent = collections.deque(maxlen=50)       # rolling resize-target window
    rows = {"req": [], "rsz": [], "util": [], "link": []}
    offered = collections.Counter()
    blocked = collections.Counter()
    reasons = collections.Counter()
    next_sid = 1

    push(arr.expovariate(rate), "arrival", None)
    for k in range(1, int(duration) + 1):
        push(float(k), "sample", None)

    while heap:
        t, _, kind, data = heapq.heappop(heap)

        if kind == "arrival":
            if t > duration:
                continue
            service = arr.choices(names, weights=shares, k=1)[0]
            ingress = arr.choice(wl["ingress_sites"])
            holding = arr.expovariate(1.0 / mean_hold)
            will_resize = rsz.random() < wl["resize_prob"]
            factor = rsz.choice(wl["resize_factors"])
            push(t + arr.expovariate(rate), "arrival", None)

            spec = services[service]
            demand = {"cpu": spec["cpu"], "mem": spec["mem"], "bw": spec["bw"]}
            edge_cpu = [s["allocated"]["cpu"] / s["capacity"]["cpu"]
                        for s in sites.values() if s["tier"] == "edge"]
            core_cpu = [s["allocated"]["cpu"] / s["capacity"]["cpu"]
                        for s in sites.values() if s["tier"] == "core"]
            offered[service] += 1

            cands, fails = placement.evaluate_sites(
                sites, topo, ingress, demand, spec["max_latency_ms"])
            if not cands:
                reason = placement.pick_reason(fails)
                blocked[service] += 1
                reasons[(service, reason)] += 1
                rows["req"].append([run_id, round(t, 3), service, ingress, "BLOCKED", "",
                                    "", reason, round(holding, 3),
                                    round(sum(edge_cpu) / len(edge_cpu), 4),
                                    round(sum(core_cpu) / max(1, len(core_cpu)), 4)])
                continue

            ch = placement.choose(policy, cands)
            site = sites[ch["site_id"]]
            for r in site["capacity"]:
                site["allocated"][r] += demand[r]
            topo.reserve_path(ch["path"], demand["bw"])
            sid = next_sid
            next_sid += 1
            inst = {"sid": sid, "site": ch["site_id"], "demand": dict(demand), "path": ch["path"]}
            live[sid] = inst
            recent.append(sid)
            push(t + holding, "depart", sid)
            if will_resize and recent:
                target = rsz.choice(list(recent))
                push(t + rsz.uniform(0.2, max(0.3, holding * 0.6)), "resize", (target, factor))
            rows["req"].append([run_id, round(t, 3), service, ingress, "ACCEPTED",
                                ch["site_id"], ch["path_latency_ms"], "", round(holding, 3),
                                round(sum(edge_cpu) / len(edge_cpu), 4),
                                round(sum(core_cpu) / max(1, len(core_cpu)), 4)])

        elif kind == "depart":
            inst = live.pop(data, None)
            if inst:
                site = sites[inst["site"]]
                for r in site["capacity"]:
                    site["allocated"][r] = max(0, site["allocated"][r] - inst["demand"][r])
                topo.release_path(inst["path"], inst["demand"]["bw"])

        elif kind == "resize":
            sid, factor = data
            inst = live.get(sid)
            if inst is None:
                if t <= duration:
                    rows["rsz"].append([run_id, round(t, 3), factor, "FAILED", "unknown_or_terminated"])
                continue
            site = sites[inst["site"]]
            old = inst["demand"]["cpu"]
            new = max(1, round(old * factor))
            extra = new - old
            if extra > 0 and site["allocated"]["cpu"] + extra > site["capacity"]["cpu"]:
                rows["rsz"].append([run_id, round(t, 3), factor, "FAILED", "node_capacity"])
            else:
                site["allocated"]["cpu"] += extra
                inst["demand"]["cpu"] = new
                rows["rsz"].append([run_id, round(t, 3), factor, "OK", ""])

        elif kind == "sample":
            for sid_, s in sites.items():
                c = s["capacity"]
                a = s["allocated"]
                rows["util"].append([run_id, int(t), sid_, round(a["cpu"] / c["cpu"], 4),
                                     round(a["mem"] / c["mem"], 4), round(a["bw"] / c["bw"], 4)])
            for name, u in topo.link_utilisation().items():
                rows["link"].append([run_id, int(t), name, u["utilisation"]])

    n_off = sum(offered.values())
    n_blk = sum(blocked.values())
    du_off = offered["vRAN-DU"]
    mean_cpu = collections.defaultdict(list)
    for r in rows["util"]:
        mean_cpu[r[2]].append(r[3])
    run_row = [run_id, policy, seed, rate, duration, mean_hold, n_off, n_blk,
               round(100 * n_blk / n_off, 3) if n_off else 0,
               round(100 * blocked["vRAN-DU"] / du_off, 3) if du_off else 0]
    for sid_ in cfg["sites"]:
        v = mean_cpu[sid_]
        run_row.append(round(100 * sum(v) / len(v), 3) if v else 0)
    peaks = collections.defaultdict(float)
    for r in rows["link"]:
        peaks[r[2]] = max(peaks[r[2]], r[3])
    for l in cfg["links"]:
        peaks_key = f"{l['a']}<->{l['b']}"
        run_row.append(round(100 * peaks[peaks_key], 2))
    rows["run"] = [run_row]
    rows["run_service"] = [[run_id, n, offered[n], blocked[n]] for n in names]
    rows["reasons"] = [[run_id, k[0], k[1], v] for k, v in reasons.items()]
    return rows


def main():
    ap = argparse.ArgumentParser(description="TeleRM big-dataset simulator")
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    ap.add_argument("--out", default=os.path.join(HERE, "bigdata"))
    ap.add_argument("--policies", default=",".join(placement.POLICIES))
    ap.add_argument("--seeds", type=int, default=30)
    ap.add_argument("--rates", default="1,2,4,6,8")
    ap.add_argument("--duration", type=float, default=300)
    ap.add_argument("--mean-holding", type=float, default=None)
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--quick", action="store_true", help="tiny sweep for a smoke test")
    args = ap.parse_args()

    cfg = json.load(open(args.config))
    policies = args.policies.split(",")
    rates = [float(x) for x in args.rates.split(",")]
    seeds, duration = args.seeds, args.duration
    if args.quick:
        seeds, duration, rates = 3, 60, [2.0, 6.0]
    mean_hold = args.mean_holding or cfg["workload"]["mean_holding_s"]

    jobs, run_id = [], 0
    for rate in rates:
        for seed in range(1, seeds + 1):
            for policy in policies:
                run_id += 1
                jobs.append({"cfg": cfg, "run_id": run_id, "policy": policy, "seed": seed,
                             "rate": rate, "duration": duration, "mean_hold": mean_hold})

    os.makedirs(args.out, exist_ok=True)
    site_ids = list(cfg["sites"])
    link_names = [f"{l['a']}<->{l['b']}" for l in cfg["links"]]
    files = {
        "run": ("dim_run.csv", ["run_id", "policy", "seed", "arrival_rate", "duration_s", "mean_holding_s",
                                "offered", "blocked", "blocking_pct", "du_blocking_pct"]
                + [f"cpu_util_pct_{s}" for s in site_ids] + [f"peak_link_pct_{l}" for l in link_names]),
        "req": ("fact_requests.csv", ["run_id", "t", "service", "ingress", "outcome", "site",
                                      "path_latency_ms", "block_reason", "holding_s",
                                      "edge_cpu_util", "core_cpu_util"]),
        "rsz": ("fact_resizes.csv", ["run_id", "t", "factor", "outcome", "reason"]),
        "util": ("fact_utilisation.csv", ["run_id", "t", "site", "cpu_util", "mem_util", "bw_util"]),
        "link": ("fact_link.csv", ["run_id", "t", "link", "utilisation"]),
        "run_service": ("fact_run_service.csv", ["run_id", "service", "offered", "blocked"]),
        "reasons": ("fact_block_reasons.csv", ["run_id", "service", "reason", "count"]),
    }
    handles = {k: open(os.path.join(args.out, v[0]), "w", newline="") for k, v in files.items()}
    writers = {k: csv.writer(handles[k]) for k in files}
    for k, v in files.items():
        writers[k].writerow(v[1])

    print(f"[sim] {len(jobs)} runs = {len(policies)} policies x {seeds} seeds x {len(rates)} rates, "
          f"{duration:.0f}s each, {args.workers} workers", flush=True)
    t0 = time.time()
    counts = collections.Counter()
    with mp.Pool(args.workers) as pool:
        for i, res in enumerate(pool.imap_unordered(simulate_run, jobs, chunksize=4), 1):
            for k in files:
                writers[k].writerows(res[k])
                counts[k] += len(res[k])
            if i % 50 == 0 or i == len(jobs):
                print(f"[sim] {i}/{len(jobs)} runs  ({time.time() - t0:.0f}s)", flush=True)
    for h in handles.values():
        h.close()

    # static dimensions
    with open(os.path.join(args.out, "dim_service.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["service", "cpu_millicores", "mem_mb", "bw_mbps", "max_latency_ms", "arrival_share"])
        for n, s in cfg["services"].items():
            w.writerow([n, s["cpu"], s["mem"], s["bw"], s["max_latency_ms"], s["share"]])
    with open(os.path.join(args.out, "dim_site.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["site", "tier", "cpu_millicores", "mem_mb", "bw_mbps", "workers"])
        for n, s in cfg["sites"].items():
            w.writerow([n, s["tier"], s["cpu"], s["mem"], s["bw"], s["workers"]])

    total = sum(counts[k] for k in ("req", "rsz", "util", "link"))
    print(f"\n[sim] done in {time.time() - t0:.1f}s -> {args.out}")
    for k, (fn, _) in files.items():
        print(f"   {fn:24s} {counts[k]:>10,d} rows")
    print(f"   fact rows total: {total:,d}")


if __name__ == "__main__":
    main()
